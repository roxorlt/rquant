"""Ownerless requests and private AI views; OpenAPI is the browser type source."""

from __future__ import annotations

from datetime import date
from typing import Annotated, Literal
from uuid import UUID

from pydantic import Field, TypeAdapter, field_validator

from rquant.ai_assistance_contracts import Sha256, AISealedFact, AISealedResultBinding
from rquant.ai_interpretation import ValidatedInterpretation
from rquant.ai_usage import AIUsageSummary
from rquant.portfolio_backtest_models import PortfolioBacktestConfig
from rquant.page_control import PageControlReceipt
from rquant.runtime_contracts import AwareUtcDatetime, RuntimeContractModel
from rquant.screen.query_contracts import ScreenQueryDefinition
from rquant.stock_news_digest import StockNewsCoverage, ValidatedStockNewsDigest
from rquant.web.models.pool_editor import EditablePool, PoolRuleChange, EditorRuleCall


class _DraftRequest(RuntimeContractModel):
    request_id: UUID
    instruction: str = Field(min_length=1, max_length=500)

    @field_validator("instruction")
    @classmethod
    def nonblank(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("instruction cannot be blank")
        return value


class AIScreenRequest(_DraftRequest):
    purpose: Literal["screen"] = "screen"
    source_kind: Literal["serving", "replica"]
    source_identity: Sha256
    trade_date: date
    include_ranking: bool = Field(default=True, strict=True)


class AIPoolRequest(_DraftRequest):
    purpose: Literal["pool_edit"] = "pool_edit"
    pool_key: str = Field(min_length=1, max_length=100)
    generation_id: str = Field(min_length=1, max_length=128)
    expected_version: Sha256


class AIInterpretationRequest(RuntimeContractModel):
    purpose: Literal["interpretation"] = "interpretation"
    request_id: UUID
    source_kind: Literal["portfolio", "strategy_template"]
    job_id: UUID
    spec_sha256: Sha256
    manifest_sha256: Sha256
    result_sha256: Sha256


class AIInterpretationContextRequest(RuntimeContractModel):
    source_kind: Literal["portfolio", "strategy_template"]
    job_id: UUID
    result_sha256: Sha256


class AINewsRequest(RuntimeContractModel):
    purpose: Literal["news_digest"] = "news_digest"
    request_id: UUID
    stock_code: str = Field(pattern=r"^\d{6}\.(SH|SZ|BJ)$")
    context_sha256: Sha256


AIGenerateRequest = Annotated[AIScreenRequest | AIPoolRequest | AIInterpretationRequest | AINewsRequest, Field(discriminator="purpose")]
AI_REQUEST_ADAPTER = TypeAdapter(AIGenerateRequest)


class AIScreenDraft(RuntimeContractModel):
    purpose: Literal["screen"] = "screen"
    definition: ScreenQueryDefinition


class AIPoolDraft(RuntimeContractModel):
    purpose: Literal["pool_edit"] = "pool_edit"
    base_generation_id: str
    base: EditablePool
    rule_calls: tuple[EditorRuleCall, ...]
    changes: tuple[PoolRuleChange, ...]
    hit_count: int | None = None
    message: str = "建议已生成。应用后请预览，再确认保存。"


class AIInterpretationContent(RuntimeContractModel):
    purpose: Literal["interpretation"] = "interpretation"
    interpretation: ValidatedInterpretation


class AINewsSource(RuntimeContractModel):
    document_id: str
    title: str
    url: str
    provider: str
    source_kind: str
    published_date: date | None
    published_at: AwareUtcDatetime | None
    body_date: date | None
    first_collected_at: AwareUtcDatetime


class AINewsContent(RuntimeContractModel):
    purpose: Literal["news_digest"] = "news_digest"
    digest: ValidatedStockNewsDigest
    sources: tuple[AINewsSource, ...]
    coverage: tuple[StockNewsCoverage, ...]
    citations: tuple["AINewsCitation", ...]


class AINewsCitation(RuntimeContractModel):
    fact_id: str
    document_id: str
    nature: Literal["actual", "forecast", "context"]
    quote: str
    body_start: int = Field(strict=True, ge=0)
    body_end: int = Field(strict=True, ge=1)
    body_sha256: Sha256
    source_path: str
    period_end: date | None = None


AINewsContent.model_rebuild()


AIContent = Annotated[AIScreenDraft | AIPoolDraft | AIInterpretationContent | AINewsContent, Field(discriminator="purpose")]
AI_CONTENT_ADAPTER = TypeAdapter(AIContent)


class AIRequestView(RuntimeContractModel):
    request_id: UUID
    purpose: Literal["screen", "pool_edit", "interpretation", "news_digest"]
    state: Literal["reserved", "dispatched", "completed", "unknown", "not_dispatched"]
    created_at: AwareUtcDatetime
    completed_at: AwareUtcDatetime | None = None
    result: AIContent | None = None
    message: str | None = None


class AICapabilities(RuntimeContractModel):
    available: bool
    can_generate: bool
    daily_limit: int | None = None
    remaining_calls: int | None = None
    message: str | None = None
    can_prepare_backtest: bool = False


class AIUsageView(RuntimeContractModel):
    available: bool
    summary: AIUsageSummary | None = None
    remaining_calls: int | None = None
    daily_limit: int | None = None
    message: str | None = None


class AINewsProgress(RuntimeContractModel):
    request_id: UUID
    scope_sha256: Sha256
    start_date: date
    end_date: date
    total: int = Field(strict=True, ge=1)
    completed: int = Field(strict=True, ge=0)
    pending: int = Field(strict=True, ge=0)
    unknown: int = Field(strict=True, ge=0)
    complete: bool = Field(strict=True)


class AIStockNewsView(RuntimeContractModel):
    stock_code: str = Field(pattern=r'^\d{6}\.(SH|SZ|BJ)$')
    state: Literal['missing','collected','ready']
    context_sha256: Sha256 | None = None
    content: AINewsContent | None = None
    coverage: tuple[StockNewsCoverage,...] = ()
    message: str | None = None
    nightly_enabled: bool = False
    progress: AINewsProgress | None = None


class AIInterpretationView(RuntimeContractModel):
    binding: AISealedResultBinding
    content: ValidatedInterpretation | None = None
    facts: tuple['AISealedFact',...] = ()
    cache_key: Sha256
    message: str | None = None


class AIGenerateAction(RuntimeContractModel):
    operation: Literal["generate"] = "generate"
    request: AIGenerateRequest


class AILookupAction(RuntimeContractModel):
    operation: Literal["lookup"] = "lookup"
    original: AIGenerateRequest


class AICapabilitiesAction(RuntimeContractModel):
    operation: Literal["capabilities"] = "capabilities"


class AIUsageAction(RuntimeContractModel):
    operation: Literal["usage"] = "usage"
    start_date: date
    end_date: date


class AIBacktestPrepareRequest(RuntimeContractModel):
    request_id: UUID
    execution_id: str = Field(min_length=1, max_length=128)
    start_date: date
    end_date: date


class AIBacktestPreparation(RuntimeContractModel):
    request_id: UUID
    execution_id: str
    created_at: AwareUtcDatetime
    config: PortfolioBacktestConfig
    config_sha256: Sha256
    material_sha256: Sha256
    proof_sha256: Sha256
    complete: bool
    trading_days: int = Field(ge=1)
    candidate_count: int = Field(ge=0)
    message: str = "完整区间已准备。请核对默认策略，再确认回测。"


class AIBacktestConfirmRequest(RuntimeContractModel):
    command_id: UUID
    requested_at: AwareUtcDatetime
    prepared_request_id: UUID
    config_sha256: Sha256
    proof_sha256: Sha256


class AIBacktestConfirmation(RuntimeContractModel):
    job_id: UUID
    receipt: PageControlReceipt


class AIBacktestPrepareAction(RuntimeContractModel):
    operation: Literal["prepare_backtest"] = "prepare_backtest"
    request: AIBacktestPrepareRequest


class AIBacktestLookupAction(RuntimeContractModel):
    operation: Literal["lookup_backtest"] = "lookup_backtest"
    original: AIBacktestPrepareRequest


class AIBacktestConfirmAction(RuntimeContractModel):
    operation: Literal["confirm_backtest"] = "confirm_backtest"
    request: AIBacktestConfirmRequest


class AIStockNewsAction(RuntimeContractModel):
    operation: Literal['stock_news'] = 'stock_news'
    stock_code: str = Field(pattern=r'^\d{6}\.(SH|SZ|BJ)$')


class AIInterpretationReadAction(RuntimeContractModel):
    operation: Literal['read_interpretation'] = 'read_interpretation'
    original: AIInterpretationRequest | AIInterpretationContextRequest


AIAction = Annotated[AIGenerateAction | AILookupAction | AICapabilitiesAction | AIUsageAction | AIBacktestPrepareAction | AIBacktestLookupAction | AIBacktestConfirmAction | AIStockNewsAction | AIInterpretationReadAction, Field(discriminator="operation")]


class AIReadData(RuntimeContractModel):
    request: AIRequestView | None = None
    usage: AIUsageView | None = None
    capabilities: AICapabilities | None = None
    preparation: AIBacktestPreparation | None = None
    confirmation: AIBacktestConfirmation | None = None
    stock_news: AIStockNewsView | None = None
    interpretation: AIInterpretationView | None = None
