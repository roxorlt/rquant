"""Typed command outbox for page-owned control actions.

Streamlit pages submit immutable commands. Only the control consumer owns the
mutable Canvas, preset, query-log, and Lab export paths.
"""

from __future__ import annotations

from contextvars import ContextVar
from functools import wraps

from rquant.collaboration_commands import (
    CommandAuthorization, PageControlRoleAuthority, SetUserRoleCommand,
    require_original_role_command,
)

_TRUSTED_COLLABORATION_ACTOR: ContextVar[str | None] = ContextVar("page_control_private_actor", default=None)

import errno
import fcntl
import json
import os
import re
import sqlite3
import stat
import urllib.request
from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager, suppress
from contextvars import ContextVar
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from enum import StrEnum
from pathlib import Path
from typing import TYPE_CHECKING, Annotated, Literal, Protocol, TypeVar

if TYPE_CHECKING:
    from rquant.factor.job_ledger import FactorJobRecord
    from rquant.web.models.collaboration import ResultOwnerProof
    from rquant.ai_assistance import AIAssistanceOwner
    from rquant.ai_assistance_contracts import AIMeasuredUsage, AIRequestBinding
    from rquant.ai_usage import AIUsageDispatch, AIUsageRecord, AIUsageRepository, AIUsageSummary
    from rquant.task_control import TaskControlPageControlBackend
    from rquant.paper_portfolio_commands import PaperPortfolioPageControlBackend
    from rquant.web.condition_alert_commands import ConditionRuleScopeResolver
    from rquant.research_query.saved import SavedResearchQuery
    from rquant.strategy_authoring import StrategyAuthoringPageControlBackend
    from rquant.strategy_authoring_source import StrategySourceCatalog
    from rquant.strategy_promotion import StrategyPromotionPageControlBackend
    from rquant.minute_backtest_parameter_study_journal import MinuteParameterStudyCommandWriter
    from rquant.minute_backtest_native_report_runtime import MinuteNativeReportCommandWriter
from urllib.parse import urlsplit, urlunsplit
from uuid import UUID, uuid4

from pydantic import (
    Field,
    JsonValue,
    StrictBool,
    StrictInt,
    TypeAdapter,
    field_validator,
    model_validator,
)

_AIUsageT = TypeVar("_AIUsageT")

from rquant.alert_price_rule import PriceAlertRule
from rquant.alert_rule_contracts import ConditionAlertRuleDefinition
from rquant.pool_result_receipt import DailyWriterCapability, PublishedDailyScreenEvidence
from rquant.canvas_publication_receipt import (
    CanvasPublicationCatalogRecord,
    CanvasPublicationCommand,
    CanvasPublicationKeyring,
    CanvasPublicationReceipt,
    CanvasPublicationReceiptStore,
    CanvasPublicationSigner,
    build_canvas_publication_claims,
)
from rquant.data_audit_contracts import MAX_AUDIT_DAYS
from rquant.factor.capability import HISTORICAL_DAILY_V1, DailyFactorCapabilities
from rquant.factor.definition import FactorDefinition
from rquant.factor.draft import FactorSaveDraft, build_draft_definition, draft_sha256
from rquant.factor.job_ledger import FactorLedgerIdentity
from rquant.factor.registry import FactorHeadRef, FactorRegistryIdentity
from rquant.factor.run_request import FactorRunRequest
from rquant.factor.stream_job_spec import FactorStreamJobSpec
from rquant.factor.tracking import FactorTrackingIdentity, FactorTrackingRequest
from rquant.lab_job_protocol import LabCommand
from rquant.llm.schemas import RuleCall
from rquant.manual_watchlist import (
    ManualWatchlistDelete,
    ManualWatchlistKey,
    ManualWatchlistRepository,
    ManualWatchlistUpsert,
    OwnerId,
    TsCode,
    WatchlistCapacityError,
    WatchlistVersionConflictError,
)
from rquant.paper_operator_commands import (
    OwnedPaperPortfolioCommand,
    OwnedSavePaperPortfolioConfiguration,
    OwnedSetPaperAccountPaused,
    PaperPortfolioCommand,
    SavePaperPortfolioConfiguration,
    SetPaperAccountPaused,
)
from rquant.paper_portfolio_models import PaperPortfolioStateIdentity
from rquant.paper_research_commands import RunPaperPortfolioResearch, OwnedRunPaperPortfolioResearch
from rquant.portfolio_backtest_commands import (
    ExportPortfolioBacktestZip,
    PortfolioPageControlBackend,
    SubmitPortfolioBacktest,
)
from rquant.minute_backtest_commands import ExportMinuteReplayZip, MinuteCommand, MinutePageControlBackend, SubmitMinuteReplay
from rquant.minute_backtest_parameter_study_commands import SubmitMinuteParameterStudy
from rquant.strict_json import canonical_json_bytes
from rquant.experiment_platform_commands import (
    ExperimentCommand,
    EXPERIMENT_COMMAND_TYPES,
    ExperimentPageControlBackend,
    ExperimentPreparationUncertainError,
)
from rquant.price_alert_rule_store import (
    PriceAlertRuleCapacityError,
    PriceAlertRuleDelete,
    PriceAlertRuleKey,
    PriceAlertRuleRepository,
    PriceAlertRuleScopeError,
    PriceAlertRuleUpsert,
    PriceAlertRuleVersionConflictError,
    RuleId,
)
from rquant.runtime_contracts import (
    AwareUtcDatetime,
    RuntimeContractModel,
    canonical_sha256,
)
from rquant.screen.pool_ranking import PoolRankingPlan
from rquant.screen.query_contracts import (
    ExecuteScreenQuery,
    _OwnedExecuteScreenQuery,
    ScreenPresetDefinition,
    ScreenQueryDefinition,
)
from rquant.screen.query_history import (
    ScreenQueryHistory,
    install_screen_query_tables,
    register_screen_execution,
)
from rquant.web.models.screen import ScreenRunData
from rquant.strategy_authoring_commands import (
    ArchiveStrategyTemplate,
    OwnedArchiveStrategyTemplate,
    OwnedSaveStrategyTemplate,
    SaveStrategyTemplate,
    StrategyAuthoringIdentity,
)

from rquant.strategy_template_run_commands import (
    OwnedRunStrategyTemplate,
    RunStrategyTemplate,
    OwnedStrategyTemplateCommandValue as OwnedStrategyTemplateCommand,
    StrategyTemplateCommandValue as StrategyTemplateCommand,
)

from rquant.task_control_commands import (
    TASK_CONTROL_KINDS, TASK_CONTROL_OWNED_TYPES, TASK_CONTROL_PUBLIC_TYPES,
    OwnedPrepareUnitRun, OwnedRequestUnitRun, OwnedSetLabSchedulingPaused,
    OwnedPrepareNotifierDeliveryMode, OwnedSetNotifierDeliveryMode, OwnedSetMonitorBuiltinEnabled,
    OwnedTaskControl, TaskControlRequest, TaskControlIdentity,
)
from rquant.strategy_promotion_commands import (
    PROMOTION_PUBLIC_TYPES, PROMOTION_OWNED_TYPES, OwnedApprovePromotion,
    OwnedRequestPromotionReview, OwnedPreparePromotionApproval, OwnedRunStrategyWalkForward,
    OwnedStrategyPromotionCommand, StrategyPromotionCommand,
    StrategyPromotionRateLimitError,
)

_SAFE_NAME = re.compile(r"^[\w\u4e00-\u9fff-]+$")
_CANVAS_CATALOG_SCHEMA_VERSION = 1
_PRIVATE_DIRECTORY_MODE = 0o700
_PRIVATE_FILE_MODE = 0o600

_MAX_MANAGED_JSON_BYTES = 1024 * 1024
_MAX_MANAGED_LOG_BYTES = 8 * 1024 * 1024
_DEFAULT_LEASE_SECONDS = 30
_MAX_REQUEST_FUTURE_SKEW = timedelta(minutes=5)
DEFAULT_PAGE_CONTROL_SERVICE_ID = "rquant-page-control"
_CONSUMER_MUTEX_SUFFIX = ".consumer.lock"
_SAFE_EFFECT_JOURNAL_MARKER = "safe-effect-journal-v2"
_SAFE_EFFECT_JOURNAL_VERSION = 2
_MANUAL_WATCHLIST_MARKER = "manual-watchlist/v1"
_MANUAL_WATCHLIST_VERSION = 1
_MANUAL_WATCHLIST_KINDS = frozenset({"add_watchlist_item", "remove_watchlist_item"})
_PRICE_RULE_MARKER = "price-alert-rule/v1"
_PRICE_RULE_VERSION = 1
_PRICE_RULE_KINDS = frozenset(
    {"save_price_alert_rule", "set_price_alert_rule_enabled", "delete_price_alert_rule"}
)
_CONDITION_RULE_KINDS = frozenset(
    {"save_alert_rule", "set_alert_rule_enabled", "delete_alert_rule"}
)
_FACTOR_DEFINITION_KINDS = frozenset({"save_factor_definition", "archive_factor"})
_FACTOR_RUN_KINDS = frozenset({"submit_factor_run"})
_FACTOR_TRACKING_KINDS = frozenset({"set_factor_tracked"})
_STRATEGY_AUTHORING_KINDS = frozenset(
    {"save_strategy_template", "archive_strategy_template", "run_strategy_template"}
)
_STRATEGY_PROMOTION_KINDS = frozenset(model.model_fields["kind"].default for model in PROMOTION_PUBLIC_TYPES)
_PAPER_PORTFOLIO_KINDS = frozenset({"set_paper_account_paused", "save_paper_portfolio_configuration", "run_paper_portfolio_research"})
_FACTOR_REGISTRY_EFFECT_IDENTITY = "factor-registry-identity/v1"
_LOCAL_FILESYSTEM_FENCE_SCHEMA_VERSION = 1
_CANVAS_HEAD_CONTRACT = "canvas-current-head/v1"
_CANVAS_HEAD_SOURCE = "canvas_current_head"
_CANVAS_WATERMARK_DIRECTORY = "canvas-publication-watermarks"
_HELD_CONSUMER_MUTEXES: set[Path] = set()
_EXTERNAL_LAB_COMMAND_KINDS = frozenset(
    {
        "submit_lab_command",
        "export_lab_artifact_zip",
        "discard_lab_artifact_zip",
    }
)


class PageControlCommand(RuntimeContractModel):
    kind: str
    command_id: str = Field(min_length=1, max_length=128)
    requested_at: AwareUtcDatetime


class SaveResearchQuery(PageControlCommand):
    kind: Literal["save_research_query"] = "save_research_query"
    query_id: str = Field(pattern=r"^[A-Za-z0-9._-]{1,64}$")
    name: str = Field(min_length=1, max_length=60)
    sql: str
    expected_version: StrictInt | None = Field(default=None, ge=1)

    @field_validator("sql")
    @classmethod
    def validate_research_sql(cls, value: str) -> str:
        from rquant.research_query.contracts import validate_sql

        return validate_sql(value)


class _OwnedSaveResearchQuery(SaveResearchQuery):
    owner_id: OwnerId


class AckAlert(PageControlCommand):
    kind: Literal["ack_alert"] = "ack_alert"
    generation_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    alert_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    actor_id: str = Field(min_length=1, max_length=256)


class AddWatchlistItem(PageControlCommand):
    kind: Literal["add_watchlist_item"] = "add_watchlist_item"
    item: ManualWatchlistUpsert


class RemoveWatchlistItem(PageControlCommand):
    kind: Literal["remove_watchlist_item"] = "remove_watchlist_item"
    item: ManualWatchlistDelete


class SavePriceAlertRule(PageControlCommand):
    kind: Literal["save_price_alert_rule"] = "save_price_alert_rule"
    ts_code: TsCode
    membership_version: StrictInt = Field(ge=1)
    expected_version: StrictInt | None = Field(default=None, ge=1)
    rule: PriceAlertRule


class SetPriceAlertRuleEnabled(PageControlCommand):
    kind: Literal["set_price_alert_rule_enabled"] = "set_price_alert_rule_enabled"
    rule_id: RuleId
    expected_version: StrictInt = Field(ge=1)
    enabled: StrictBool


class DeletePriceAlertRule(PageControlCommand):
    kind: Literal["delete_price_alert_rule"] = "delete_price_alert_rule"
    rule_id: RuleId
    expected_version: StrictInt = Field(ge=1)


class SaveAlertRule(PageControlCommand):
    kind: Literal["save_alert_rule"] = "save_alert_rule"
    expected_version: StrictInt | None = Field(default=None, ge=1)
    rule: ConditionAlertRuleDefinition


class SetAlertRuleEnabled(PageControlCommand):
    kind: Literal["set_alert_rule_enabled"] = "set_alert_rule_enabled"
    rule_id: RuleId
    expected_version: StrictInt = Field(ge=1)
    enabled: StrictBool


class DeleteAlertRule(PageControlCommand):
    kind: Literal["delete_alert_rule"] = "delete_alert_rule"
    rule_id: RuleId
    expected_version: StrictInt = Field(ge=1)


class _OwnedSaveAlertRule(SaveAlertRule):
    owner_id: OwnerId


class _OwnedSetAlertRuleEnabled(SetAlertRuleEnabled):
    owner_id: OwnerId


class _OwnedDeleteAlertRule(DeleteAlertRule):
    owner_id: OwnerId


ConditionAlertRuleRequestValue = SaveAlertRule | SetAlertRuleEnabled | DeleteAlertRule
_OwnedConditionAlertRuleValue = (
    _OwnedSaveAlertRule | _OwnedSetAlertRuleEnabled | _OwnedDeleteAlertRule
)
_CONDITION_PUBLIC_TYPES = (SaveAlertRule, SetAlertRuleEnabled, DeleteAlertRule)
_CONDITION_OWNED_TYPES = (_OwnedSaveAlertRule, _OwnedSetAlertRuleEnabled, _OwnedDeleteAlertRule)


def _owned_condition_rule_command(
    command: ConditionAlertRuleRequestValue, *, authenticated_owner_id: str
) -> _OwnedConditionAlertRuleValue:
    if type(command) not in _CONDITION_PUBLIC_TYPES:
        raise TypeError("condition rule requires an exact ownerless request")
    model = dict(zip(_CONDITION_PUBLIC_TYPES, _CONDITION_OWNED_TYPES, strict=True))[type(command)]
    return model.model_validate(
        {**command.model_dump(mode="python"), "owner_id": authenticated_owner_id}
    )


class _OwnedSavePriceAlertRule(SavePriceAlertRule):
    owner_id: OwnerId


class _OwnedSetPriceAlertRuleEnabled(SetPriceAlertRuleEnabled):
    owner_id: OwnerId


class _OwnedDeletePriceAlertRule(DeletePriceAlertRule):
    owner_id: OwnerId


PriceAlertRuleRequestValue = SavePriceAlertRule | SetPriceAlertRuleEnabled | DeletePriceAlertRule
_OwnedPriceAlertRuleValue = (
    _OwnedSavePriceAlertRule | _OwnedSetPriceAlertRuleEnabled | _OwnedDeletePriceAlertRule
)


def _owned_price_rule_command(
    command: PriceAlertRuleRequestValue, *, authenticated_owner_id: str
) -> _OwnedPriceAlertRuleValue:
    if type(command) is SavePriceAlertRule:
        model = _OwnedSavePriceAlertRule
    elif type(command) is SetPriceAlertRuleEnabled:
        model = _OwnedSetPriceAlertRuleEnabled
    elif type(command) is DeletePriceAlertRule:
        model = _OwnedDeletePriceAlertRule
    else:
        raise TypeError("trusted price rule submission requires an ownerless request")
    return model.model_validate(
        {**command.model_dump(mode="python"), "owner_id": authenticated_owner_id}
    )


class SaveFactorDefinition(PageControlCommand):
    kind: Literal["save_factor_definition"] = "save_factor_definition"
    command_id: str = Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")
    definition: FactorDefinition
    expected_head: FactorHeadRef | None


class ArchiveFactor(PageControlCommand):
    kind: Literal["archive_factor"] = "archive_factor"
    command_id: str = Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")
    factor_id: str = Field(pattern=r"^[a-z][a-z0-9_]{0,63}$")
    expected_head: FactorHeadRef


class _OwnedSaveFactorDefinition(SaveFactorDefinition):
    actor_id: OwnerId
    # Optional only so pre-existing trusted local save payloads keep their original meaning.
    registry_identity: FactorRegistryIdentity | None = None
    original_request_sha256: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    original_generation_id: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")


class _OwnedArchiveFactor(ArchiveFactor):
    actor_id: OwnerId
    registry_identity: FactorRegistryIdentity


class SubmitFactorRun(PageControlCommand):
    kind: Literal["submit_factor_run"] = "submit_factor_run"
    request: FactorRunRequest

    @model_validator(mode="after")
    def _original(self) -> SubmitFactorRun:
        if (
            self.command_id != self.request.command_id
            or self.requested_at != self.request.requested_at
        ):
            raise ValueError("factor run differs from its original command")
        return self


class _OwnedSubmitFactorRun(SubmitFactorRun):
    actor_id: OwnerId
    registry_identity: FactorRegistryIdentity
    ledger_identity: FactorLedgerIdentity
    spec: FactorStreamJobSpec
    original_request_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")

    @model_validator(mode="after")
    def _frozen(self) -> _OwnedSubmitFactorRun:
        params, adapter = self.request.parameters, self.spec.adapter_request
        definition = adapter.formula.definition
        if (
            self.original_request_sha256 != self.request.request_sha256
            or params.factor_id != definition.factor_id
            or params.expected_head.version != definition.version
            or params.expected_head.content_sha256 != self.spec.definition_content_sha256
            or params.selection != adapter.formula.selection
            or params.holding_sessions != adapter.holding_sessions
            or params.neutralization != adapter.formula.neutralization
            or not all(
                params.start_date <= day <= params.end_date for day in adapter.evaluation_days
            )
        ):
            raise ValueError("factor run frozen inputs differ from the original request")
        return self


class SetFactorTracked(PageControlCommand):
    kind: Literal["set_factor_tracked"] = "set_factor_tracked"
    request: FactorTrackingRequest

    @model_validator(mode="after")
    def _original(self) -> SetFactorTracked:
        if (
            self.command_id != self.request.command_id
            or self.requested_at != self.request.requested_at
        ):
            raise ValueError("tracking toggle differs from its original command")
        return self


class _OwnedSetFactorTracked(SetFactorTracked):
    actor_id: OwnerId
    registry_identity: FactorRegistryIdentity
    tracking_identity: FactorTrackingIdentity


class FactorTrackingPageControlBackend(Protocol):
    def authorize(self, actor_id: str) -> None: ...
    def compile(
        self, request: FactorTrackingRequest, *, verified_registry_instance_id: str
    ) -> object: ...
    def validate(self, command: _OwnedSetFactorTracked) -> None: ...
    def submit(self, command: _OwnedSetFactorTracked) -> JsonValue: ...
    def recover(self, command: _OwnedSetFactorTracked) -> JsonValue | None: ...


class FactorRunPageControlBackend(Protocol):
    def capabilities(self) -> DailyFactorCapabilities: ...
    def authorize(self, actor_id: str) -> None: ...
    def compile(
        self, request: FactorRunRequest, *, verified_registry_instance_id: str
    ) -> object: ...
    def confirm_unsubmitted(
        self, request: FactorRunRequest, *, verified_registry_instance_id: str
    ) -> None: ...
    def validate(self, command: _OwnedSubmitFactorRun) -> None: ...
    def submit(self, command: _OwnedSubmitFactorRun) -> JsonValue: ...
    def recover(self, command: _OwnedSubmitFactorRun) -> JsonValue | None: ...


FactorDefinitionRequestValue = SaveFactorDefinition | ArchiveFactor
_OwnedFactorDefinitionValue = _OwnedSaveFactorDefinition | _OwnedArchiveFactor


def _owned_factor_definition_command(
    command: FactorDefinitionRequestValue,
    *,
    authenticated_actor_id: str,
    registry_identity: FactorRegistryIdentity | None = None,
) -> _OwnedFactorDefinitionValue:
    if type(command) is SaveFactorDefinition:
        model = _OwnedSaveFactorDefinition
    elif type(command) is ArchiveFactor:
        if registry_identity is None:
            raise ValueError("archive requires a captured factor registry identity")
        model = _OwnedArchiveFactor
    else:
        raise TypeError("trusted factor submission requires an ownerless request")
    fields = {**command.model_dump(mode="python"), "actor_id": authenticated_actor_id}
    if registry_identity is not None:
        fields["registry_identity"] = registry_identity
    return model.model_validate(fields)


class SaveCanvas(PageControlCommand):
    kind: Literal["save_canvas"] = "save_canvas"
    name: str
    description: str = ""
    pool_refs: tuple[str, ...] = ()
    source: str = "page_control"

    @field_validator("name")
    @classmethod
    def validate_name(cls, value: str) -> str:
        return _validated_name(value, label="canvas name")


class CreateCanvas(PageControlCommand):
    kind: Literal["create_canvas"] = "create_canvas"
    name: str = Field(min_length=1, max_length=80)
    description: str = Field(default="", max_length=1_024)

    @field_validator("name")
    @classmethod
    def validate_name(cls, value: str) -> str:
        return _validated_name(value, label="canvas name")


class DeleteCanvas(PageControlCommand):
    kind: Literal["delete_canvas"] = "delete_canvas"
    name: str

    @field_validator("name")
    @classmethod
    def validate_name(cls, value: str) -> str:
        return _validated_name(value, label="canvas name")


class SetCanvasPoolRefs(PageControlCommand):
    kind: Literal["set_canvas_pool_refs"] = "set_canvas_pool_refs"
    name: str
    pool_refs: tuple[str, ...]

    @field_validator("name")
    @classmethod
    def validate_name(cls, value: str) -> str:
        return _validated_name(value, label="canvas name")


class AddPoolToCanvas(PageControlCommand):
    kind: Literal["add_pool_to_canvas"] = "add_pool_to_canvas"
    canvas_name: str
    pool_name: str
    expected_pool_version: str = Field(pattern=r"^[0-9a-f]{64}$")

    @field_validator("canvas_name")
    @classmethod
    def validate_canvas_name(cls, value: str) -> str:
        return _validated_name(value, label="canvas name")

    @field_validator("pool_name")
    @classmethod
    def validate_pool_name(cls, value: str) -> str:
        if not value.startswith("user/"):
            raise ValueError("only user pools may be added by this command")
        _validated_name(value.removeprefix("user/"), label="pool name")
        return value


class SaveUserPool(PageControlCommand):
    kind: Literal["save_user_pool"] = "save_user_pool"
    base_name: str
    description: str = ""
    rule_calls: tuple[RuleCall, ...] = ()
    include_columns: tuple[str, ...] = ()
    source: str = "page_control"
    canvas_name: str | None = None

    @field_validator("base_name")
    @classmethod
    def validate_base_name(cls, value: str) -> str:
        return _validated_name(value, label="user pool name")

    @field_validator("canvas_name")
    @classmethod
    def validate_canvas_name(cls, value: str | None) -> str | None:
        return None if value is None else _validated_name(value, label="canvas name")


class SaveUserPoolV2(PageControlCommand):
    kind: Literal["save_user_pool_v2"] = "save_user_pool_v2"
    base_name: str
    display_name: str = Field(min_length=1, max_length=80)
    description: str = ""
    rule_calls: tuple[RuleCall, ...] = ()
    include_columns: tuple[str, ...] = ()
    depends_on: str | None = None
    delay_days: int = 0
    expected_version: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")

    @field_validator("base_name")
    @classmethod
    def validate_base_name(cls, value: str) -> str:
        return _validated_name(value, label="user pool name")


class SaveUserPoolV3(PageControlCommand):
    kind: Literal["save_user_pool_v3"] = "save_user_pool_v3"
    base_name: str
    display_name: str = Field(min_length=1, max_length=80)
    description: str = ""
    rule_calls: tuple[RuleCall, ...] = Field(default=(), max_length=26)
    include_columns: tuple[str, ...] = Field(default=(), max_length=26)
    depends_on: str | None = None
    delay_days: int = 0
    expected_version: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    ranking: PoolRankingPlan | None

    @field_validator("base_name")
    @classmethod
    def validate_base_name(cls, value: str) -> str:
        return _validated_name(value, label="user pool name")


class SaveFormulaPoolV1(PageControlCommand):
    kind: Literal["save_formula_pool_v1"] = "save_formula_pool_v1"
    base_name: str = Field(min_length=1, max_length=80)
    display_name: str = Field(min_length=1, max_length=80)
    task_id: str = Field(pattern=r"^[0-9a-f]{32}$")
    actor_id: str = Field(min_length=1, max_length=256)
    expected_version: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")

    @field_validator("base_name")
    @classmethod
    def validate_base_name(cls, value: str) -> str:
        return _validated_name(value, label="formula pool name")


class DeleteUserPool(PageControlCommand):
    kind: Literal["delete_user_pool"] = "delete_user_pool"
    base_name: str

    @field_validator("base_name")
    @classmethod
    def validate_base_name(cls, value: str) -> str:
        return _validated_name(value, label="user pool name")


class ForkBuiltinPool(PageControlCommand):
    kind: Literal["fork_builtin_pool"] = "fork_builtin_pool"
    builtin_name: str
    target_base_name: str
    canvas_name: str | None = None

    @field_validator("builtin_name", "target_base_name")
    @classmethod
    def validate_pool_name(cls, value: str) -> str:
        return _validated_name(value, label="pool name")

    @field_validator("canvas_name")
    @classmethod
    def validate_canvas_name(cls, value: str | None) -> str | None:
        return None if value is None else _validated_name(value, label="canvas name")


class SaveNlPreset(PageControlCommand):
    kind: Literal["save_nl_preset"] = "save_nl_preset"
    name: str
    description: str = ""
    rule_calls: tuple[RuleCall, ...] = ()
    include_columns: tuple[str, ...] = ()
    overwrite: bool = False

    @field_validator("name")
    @classmethod
    def validate_name(cls, value: str) -> str:
        return _validated_name(value, label="preset name")


class AppendNlQueryLog(PageControlCommand):
    kind: Literal["append_nl_query_log"] = "append_nl_query_log"
    query: str = Field(min_length=1)
    plan: JsonValue | None = None
    outcome: Literal["success", "clarification", "error"]
    error: str | None = None


class _OwnedSaveNlPreset(SaveNlPreset):
    kind: Literal["save_screen_query_preset"] = "save_screen_query_preset"
    owner_id: OwnerId
    definition: ScreenPresetDefinition
    expected_version: int | None = Field(default=None, ge=1)

    @model_validator(mode="after")
    def verify_private_context(self) -> _OwnedSaveNlPreset:
        if (
            self.name != self.definition.preset_id
            or self.description != self.definition.definition.description
            or self.rule_calls != self.definition.definition.conditions
        ):
            raise ValueError("private preset differs from original NL command")
        if self.overwrite != (self.expected_version is not None):
            raise ValueError("private preset overwrite requires an explicit current version")
        return self


class _OwnedAppendNlQueryLog(AppendNlQueryLog):
    owner_id: OwnerId


_SCREEN_QUERY_PRIVATE_KINDS = frozenset({"execute_screen_query", "save_screen_query_preset"})


class InitializeLabExports(PageControlCommand):
    kind: Literal["initialize_lab_exports"] = "initialize_lab_exports"
    export_root: Path
    runtime_root: Path


class SubmitLabCommand(PageControlCommand):
    kind: Literal["submit_lab_command"] = "submit_lab_command"
    command: LabCommand
    interaction_key: str | None = Field(default=None, min_length=1, max_length=256)


class SubmitBackfillPlan(PageControlCommand):
    kind: Literal["submit_backfill_plan"] = "submit_backfill_plan"
    actor_id: str = Field(min_length=1, max_length=256)
    audit_start: date
    completed_through: date

    @model_validator(mode="after")
    def validate_range(self) -> SubmitBackfillPlan:
        days = (self.completed_through - self.audit_start).days + 1
        if days < 1 or days > MAX_AUDIT_DAYS:
            raise ValueError(f"backfill plan audit range must contain 1 to {MAX_AUDIT_DAYS} days")
        return self


class SubmitDataAuditReport(PageControlCommand):
    kind: Literal["submit_data_audit_report"] = "submit_data_audit_report"
    actor_id: str = Field(min_length=1, max_length=256)
    audit_start: date
    observed_through: date

    @model_validator(mode="after")
    def validate_range(self) -> SubmitDataAuditReport:
        days = (self.observed_through - self.audit_start).days + 1
        if days < 1 or days > MAX_AUDIT_DAYS:
            raise ValueError(f"data audit report range must contain 1 to {MAX_AUDIT_DAYS} days")
        return self


class PrepareBackfillExecution(PageControlCommand):
    kind: Literal['prepare_backfill_execution'] = 'prepare_backfill_execution'
    actor_id: str = Field(min_length=1,max_length=256)
    plan_task_id: str = Field(pattern=r'^[0-9a-f]{32}$')
    plan_hash: str = Field(pattern=r'^[0-9a-f]{64}$')


class ExecuteBackfillPlan(PageControlCommand):
    kind: Literal['execute_backfill_plan'] = 'execute_backfill_plan'
    actor_id: str = Field(min_length=1,max_length=256)
    execution_id: str = Field(pattern=r'^[0-9a-f]{64}$')
    intent_id: str = Field(pattern=r'^[0-9a-f]{64}$')
    prepare_command_id: str = Field(min_length=1,max_length=128)
    plan_task_id: str = Field(pattern=r'^[0-9a-f]{32}$')
    plan_hash: str = Field(pattern=r'^[0-9a-f]{64}$')
    exact_dates_sha256: str = Field(pattern=r'^[0-9a-f]{64}$')
    confirmed: Literal[True]


class PauseDataCenterExecution(PageControlCommand):
    kind: Literal['pause_data_center_execution'] = 'pause_data_center_execution'
    actor_id: str = Field(min_length=1,max_length=256)
    execution_id: str = Field(pattern=r'^[0-9a-f]{64}$')
    expected_sequence: StrictInt = Field(ge=1)


class ResumeDataCenterExecution(PageControlCommand):
    kind: Literal['resume_data_center_execution'] = 'resume_data_center_execution'
    actor_id: str = Field(min_length=1,max_length=256)
    execution_id: str = Field(pattern=r'^[0-9a-f]{64}$')
    expected_sequence: StrictInt = Field(ge=1)


class PrepareFinancialCollection(PageControlCommand):
    kind: Literal['prepare_financial_collection'] = 'prepare_financial_collection'
    actor_id: str = Field(min_length=1,max_length=256)
    audit_report_hash: str = Field(pattern=r'^[0-9a-f]{64}$')
    security_scope: Literal['available_securities','selected_securities']
    selected_securities: tuple[str,...] = Field(default=(),max_length=250)
    start_date: date
    end_date: date
    report_periods: tuple[date,...] = Field(min_length=1,max_length=41)

    @model_validator(mode='after')
    def bind_scope(self) -> PrepareFinancialCollection:
        if not 1<=(self.end_date-self.start_date).days+1<=3660:
            raise ValueError('financial request dates exceed fixed range')
        if self.selected_securities!=tuple(sorted(set(self.selected_securities))):
            raise ValueError('selected securities must be unique and ordered')
        if (self.security_scope=='selected_securities')!=bool(self.selected_securities):
            raise ValueError('financial security scope differs from selection')
        if self.report_periods!=tuple(sorted(set(self.report_periods))) or any(
                period>self.end_date or (period.month,period.day) not in {(3,31),(6,30),(9,30),(12,31)} for period in self.report_periods):
            raise ValueError('financial report periods must be ordered quarter ends')
        return self


class ExecuteFinancialCollection(PageControlCommand):
    kind: Literal['execute_financial_collection'] = 'execute_financial_collection'
    actor_id: str = Field(min_length=1,max_length=256)
    execution_id: str = Field(pattern=r'^[0-9a-f]{64}$')
    intent_id: str = Field(pattern=r'^[0-9a-f]{64}$')
    prepare_command_id: str = Field(min_length=1,max_length=128)
    plan_hash: str = Field(pattern=r'^[0-9a-f]{64}$')
    confirmed: Literal[True]


DataCenterExecutionCommand = PrepareBackfillExecution | ExecuteBackfillPlan | PauseDataCenterExecution | ResumeDataCenterExecution | PrepareFinancialCollection | ExecuteFinancialCollection
DATA_CENTER_EXECUTION_COMMAND_TYPES = (PrepareBackfillExecution,ExecuteBackfillPlan,PauseDataCenterExecution,ResumeDataCenterExecution,PrepareFinancialCollection,ExecuteFinancialCollection)


class SubmitFormulaMarketRun(PageControlCommand):
    kind: Literal["submit_formula_market_run"] = "submit_formula_market_run"
    actor_id: str = Field(min_length=1, max_length=256)
    formula: str = Field(min_length=1, max_length=4096)
    trade_date: date


class ExportLabArtifactZip(PageControlCommand):
    kind: Literal["export_lab_artifact_zip"] = "export_lab_artifact_zip"
    job_id: UUID


class DiscardLabArtifactZip(PageControlCommand):
    kind: Literal["discard_lab_artifact_zip"] = "discard_lab_artifact_zip"
    request_id: UUID
    job_id: UUID
    path: Path
    byte_size: int = Field(ge=0)
    sha256: str = Field(pattern=r"^[0-9a-f]{64}$")


class LabArtifactZipResult(RuntimeContractModel):
    request_id: UUID
    job_id: UUID
    path: Path
    byte_size: int = Field(ge=0)
    sha256: str = Field(pattern=r"^[0-9a-f]{64}$")


class LabPageControlBackend(Protocol):
    def submit_command(
        self,
        command: LabCommand,
        *,
        interaction_key: str | None,
    ) -> JsonValue: ...

    def export_zip(self, job_id: UUID) -> JsonValue: ...

    def discard_zip(self, command: DiscardLabArtifactZip) -> JsonValue: ...


class BackfillPlanPageControlBackend(Protocol):
    def submit(self, command: SubmitBackfillPlan) -> JsonValue: ...

    def recover(self, command: SubmitBackfillPlan) -> JsonValue | None: ...


class DataCenterExecutionPageControlBackend(Protocol):
    def submit(self,command: DataCenterExecutionCommand) -> JsonValue: ...

    def recover(self,command: DataCenterExecutionCommand) -> JsonValue | None: ...


class DataAuditReportPageControlBackend(Protocol):
    def submit(self, command: SubmitDataAuditReport) -> JsonValue: ...

    def recover(self, command: SubmitDataAuditReport) -> JsonValue | None: ...


class FormulaMarketPageControlBackend(Protocol):
    def submit(self, command: SubmitFormulaMarketRun) -> JsonValue: ...

    def recover(self, command: SubmitFormulaMarketRun) -> JsonValue | None: ...


class FormulaPoolPageControlBackend(Protocol):
    def submit(self, command: SaveFormulaPoolV1) -> JsonValue: ...

    def recover(self, command: SaveFormulaPoolV1) -> JsonValue | None: ...


class FactorDefinitionPageControlBackend(Protocol):
    def identity(self) -> FactorRegistryIdentity: ...

    def submit(
        self,
        command: FactorDefinitionRequestValue,
        *,
        expected_identity: FactorRegistryIdentity,
    ) -> JsonValue: ...

    def recover(
        self,
        command: FactorDefinitionRequestValue,
        *,
        expected_identity: FactorRegistryIdentity,
    ) -> JsonValue | None: ...


PageControlCommandValue = Annotated[
    AckAlert
    | SetUserRoleCommand
    | _OwnedExecuteScreenQuery
    | _OwnedSaveNlPreset
    | _OwnedSaveResearchQuery
    | AddWatchlistItem
    | RemoveWatchlistItem
    | _OwnedSavePriceAlertRule
    | _OwnedSetPriceAlertRuleEnabled
    | _OwnedDeletePriceAlertRule
    | _OwnedSaveAlertRule
    | _OwnedSetAlertRuleEnabled
    | _OwnedDeleteAlertRule
    | _OwnedSaveFactorDefinition
    | _OwnedArchiveFactor
    | _OwnedSubmitFactorRun
    | _OwnedSetFactorTracked
    | OwnedSaveStrategyTemplate
    | OwnedArchiveStrategyTemplate
    | OwnedRunStrategyTemplate
    | OwnedRequestPromotionReview
    | OwnedPreparePromotionApproval
    | OwnedApprovePromotion
    | OwnedRunStrategyWalkForward
    | OwnedSetPaperAccountPaused
    | OwnedSavePaperPortfolioConfiguration
    | OwnedRunPaperPortfolioResearch
    | OwnedPrepareUnitRun
    | OwnedRequestUnitRun
    | OwnedSetLabSchedulingPaused
    | OwnedPrepareNotifierDeliveryMode
    | OwnedSetNotifierDeliveryMode
    | OwnedSetMonitorBuiltinEnabled
    | SaveCanvas
    | CreateCanvas
    | DeleteCanvas
    | SetCanvasPoolRefs
    | AddPoolToCanvas
    | SaveUserPool
    | SaveUserPoolV2
    | SaveUserPoolV3
    | SaveFormulaPoolV1
    | DeleteUserPool
    | ForkBuiltinPool
    | SaveNlPreset
    | AppendNlQueryLog
    | InitializeLabExports
    | SubmitLabCommand
    | SubmitBackfillPlan
    | SubmitDataAuditReport
    | DataCenterExecutionCommand
    | SubmitFormulaMarketRun
    | ExportLabArtifactZip
    | SubmitPortfolioBacktest
    | SubmitMinuteReplay
    | SubmitMinuteParameterStudy
    | ExportMinuteReplayZip
    | ExportPortfolioBacktestZip
    | ExperimentCommand
    | DiscardLabArtifactZip,
    Field(discriminator="kind"),
]
_COMMAND_ADAPTER = TypeAdapter(PageControlCommandValue)


class PageControlStatus(StrEnum):
    PENDING = "pending"
    PROCESSING = "processing"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    AMBIGUOUS = "ambiguous"


class PageControlCommandConflictError(ValueError):
    """A command ID already binds a different durable command payload."""


class PageControlEffectStatus(StrEnum):
    STARTED = "started"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    AMBIGUOUS = "ambiguous"


_PAGE_CONTROL_TERMINAL_STATUSES = frozenset(
    {
        PageControlStatus.SUCCEEDED,
        PageControlStatus.FAILED,
        PageControlStatus.AMBIGUOUS,
    }
)
_PAGE_CONTROL_EFFECT_TERMINAL_STATUSES = frozenset(
    {
        PageControlEffectStatus.SUCCEEDED,
        PageControlEffectStatus.FAILED,
        PageControlEffectStatus.AMBIGUOUS,
    }
)


class PageControlReceipt(RuntimeContractModel):
    command_id: str
    status: PageControlStatus
    enqueued_at: AwareUtcDatetime
    completed_at: AwareUtcDatetime | None = None
    result: JsonValue | None = None
    error: str | None = None


class AlertAcknowledgment(RuntimeContractModel):
    alert_id: str
    confirmation_id: str
    actor_id: str
    confirmed_at: AwareUtcDatetime
    generation_id: str


class PageControlCommandAudit(RuntimeContractModel):
    command_id: str
    command_kind: str
    command_hash: str
    status: PageControlStatus
    result: JsonValue | None = None


class PageControlClaim(RuntimeContractModel):
    command: PageControlCommandValue
    owner_id: str
    claim_token: str


class PageControlEffectRecord(RuntimeContractModel):
    command_id: str
    command_hash: str
    effect_kind: str
    status: PageControlEffectStatus
    owner_id: str
    claim_token: str
    result: JsonValue | None = None
    error: str | None = None


@dataclass(frozen=True)
class _ExecutionOutcome:
    status: PageControlStatus
    result: JsonValue | None
    error: str | None = None


class _RetryableUncertainEffectError(RuntimeError):
    """A durable effect may have committed; retry its recovery before finalizing."""


@dataclass(frozen=True)
class _LocalEffectFenceTarget:
    role: str
    path: Path
    create: bool = True


@dataclass(frozen=True)
class _BoundManagedDirectory:
    path: Path
    descriptors: tuple[int, ...]
    component_names: tuple[str, ...]

    @property
    def descriptor(self) -> int:
        return self.descriptors[-1]

    def verify(self) -> None:
        for parent, child, component in zip(
            self.descriptors[:-1],
            self.descriptors[1:],
            self.component_names,
            strict=True,
        ):
            try:
                entry = os.stat(component, dir_fd=parent, follow_symlinks=False)
            except FileNotFoundError as exc:
                raise ValueError(
                    f"managed directory ancestor changed while bound: {self.path}"
                ) from exc
            if stat.S_ISLNK(entry.st_mode):
                raise ValueError(f"managed directory ancestor cannot be a symlink: {self.path}")
            if not stat.S_ISDIR(entry.st_mode):
                raise ValueError(f"managed directory ancestor is not a directory: {self.path}")
            if _file_node_tuple(entry) != _file_node_tuple(os.fstat(child)):
                raise ValueError(f"managed directory ancestor changed while bound: {self.path}")

    def duplicate(self) -> int:
        self.verify()
        return os.dup(self.descriptor)

    def close(self) -> None:
        for descriptor in reversed(self.descriptors):
            with suppress(OSError):
                os.close(descriptor)


_ACTIVE_EFFECT_DIRECTORY_BINDINGS: ContextVar[Mapping[Path, _BoundManagedDirectory] | None] = (
    ContextVar("page_control_effect_directory_bindings", default=None)
)


@dataclass(frozen=True)
class CanvasCurrentHead:
    receipt: CanvasPublicationReceipt
    state: Literal["active", "deleted"]
    sequence: int
    previous_head_receipt_id: str | None
    publication_receipt_id: str | None
    authority_command_kind: str
    authority_command_hash: str


def read_canvas_current_head(
    root: Path,
    canvas_name: str,
    keyring: CanvasPublicationKeyring,
    *,
    observed_at: datetime | None = None,
    directory_descriptor: int | None = None,
) -> CanvasCurrentHead | None:
    canvas_root = Path(os.path.abspath(root)) / _validated_name(
        canvas_name,
        label="canvas name",
    )
    if directory_descriptor is None:
        try:
            directory = _open_existing_managed_directory(canvas_root)
        except FileNotFoundError:
            return None
    else:
        directory = os.dup(directory_descriptor)
    try:
        _verify_open_directory_matches_path(canvas_root, directory)
        names = sorted(os.listdir(directory))
    finally:
        os.close(directory)
    if not names:
        return None
    nodes: list[CanvasCurrentHead] = []
    store = CanvasPublicationReceiptStore(
        canvas_root,
        directory_descriptor=directory_descriptor,
    )
    for name in names:
        if not name.endswith(".json") or Path(name).name != name:
            raise ValueError("canvas current head directory contains an invalid entry")
        receipt_id = name.removesuffix(".json")
        publication = store.read(receipt_id)
        if not keyring.verify_publication_receipt(publication):
            raise ValueError("canvas current head signature verification failed")
        if observed_at is not None:
            _assert_canvas_receipt_not_future(publication, observed_at=observed_at)
        command = publication.claims.command
        if (
            command.name != canvas_name
            or command.source != _CANVAS_HEAD_SOURCE
            or command.pool_refs
        ):
            raise ValueError("canvas current head signed semantics do not match")
        try:
            payload = json.loads(command.description)
        except json.JSONDecodeError as exc:
            raise ValueError("canvas current head payload is not valid JSON") from exc
        if not isinstance(payload, dict):
            raise ValueError("canvas current head payload is not an object")
        expected_keys = {
            "authority_command_hash",
            "authority_command_kind",
            "canvas_name",
            "contract",
            "previous_head_receipt_id",
            "publication_receipt_id",
            "sequence",
            "state",
        }
        if set(payload) != expected_keys or payload.get("contract") != _CANVAS_HEAD_CONTRACT:
            raise ValueError("canvas current head payload schema mismatch")
        if payload.get("canvas_name") != canvas_name:
            raise ValueError("canvas current head canvas identity mismatch")
        state = payload.get("state")
        sequence = payload.get("sequence")
        previous = payload.get("previous_head_receipt_id")
        active_receipt_id = payload.get("publication_receipt_id")
        command_kind = payload.get("authority_command_kind")
        command_hash = payload.get("authority_command_hash")
        if state not in {"active", "deleted"} or not isinstance(sequence, int):
            raise ValueError("canvas current head state is invalid")
        if sequence < 1:
            raise ValueError("canvas current head sequence is invalid")
        if previous is not None and (
            not isinstance(previous, str) or re.fullmatch(r"[0-9a-f]{64}", previous) is None
        ):
            raise ValueError("canvas current head predecessor is invalid")
        if state == "active":
            if (
                not isinstance(active_receipt_id, str)
                or re.fullmatch(r"[0-9a-f]{64}", active_receipt_id) is None
            ):
                raise ValueError("canvas current head publication identity is invalid")
        elif active_receipt_id is not None:
            raise ValueError("canvas tombstone cannot reference an active publication")
        if not isinstance(command_kind, str) or not command_kind:
            raise ValueError("canvas current head command kind is invalid")
        if not isinstance(command_hash, str) or re.fullmatch(r"[0-9a-f]{64}", command_hash) is None:
            raise ValueError("canvas current head command hash is invalid")
        canonical_payload = json.dumps(
            payload,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        )
        if canonical_payload != command.description:
            raise ValueError("canvas current head payload is not canonical JSON")
        nodes.append(
            CanvasCurrentHead(
                receipt=publication,
                state=state,
                sequence=sequence,
                previous_head_receipt_id=previous,
                publication_receipt_id=active_receipt_id,
                authority_command_kind=command_kind,
                authority_command_hash=command_hash,
            )
        )
    nodes.sort(key=lambda item: item.sequence)
    for index, node in enumerate(nodes):
        expected_sequence = index + 1
        expected_previous = None if index == 0 else nodes[index - 1].receipt.receipt_id
        if node.sequence != expected_sequence or node.previous_head_receipt_id != expected_previous:
            raise ValueError("canvas current head chain is not linear and complete")
    current = nodes[-1]
    if not keyring.verify_publication_receipt(current.receipt, require_active=True):
        raise ValueError("canvas current head active signature verification failed")
    return current


class PageControlOutbox:
    """Durable, idempotent command authority separate from page state."""

    def __init__(self, path: Path) -> None:
        self.path = Path(path)
        self.collaboration: PageControlRoleAuthority | None = None
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS page_control_command (
                    command_id TEXT PRIMARY KEY,
                    command_kind TEXT NOT NULL,
                    command_hash TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    status TEXT NOT NULL,
                    enqueued_at TEXT NOT NULL,
                    completed_at TEXT,
                    result_json TEXT,
                    error TEXT
                );
                CREATE TABLE IF NOT EXISTS page_control_effect (
                    command_id TEXT PRIMARY KEY,
                    command_hash TEXT NOT NULL,
                    effect_kind TEXT NOT NULL,
                    status TEXT NOT NULL,
                    owner_id TEXT NOT NULL,
                    claim_token TEXT NOT NULL,
                    started_at TEXT NOT NULL,
                    completed_at TEXT,
                    result_json TEXT,
                    error TEXT,
                    FOREIGN KEY(command_id) REFERENCES page_control_command(command_id)
                );
                CREATE INDEX IF NOT EXISTS page_control_pending_idx
                    ON page_control_command(status, enqueued_at, command_id);
                CREATE TABLE IF NOT EXISTS page_control_protocol_activation (
                    marker_name TEXT PRIMARY KEY,
                    protocol_version INTEGER NOT NULL,
                    activated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS page_control_alert_activation (
                    marker_name TEXT PRIMARY KEY CHECK(marker_name = 'alert_ack'),
                    activated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS page_control_alert_ack (
                    alert_id TEXT PRIMARY KEY,
                    confirmation_id TEXT NOT NULL UNIQUE,
                    actor_id TEXT NOT NULL,
                    confirmed_at TEXT NOT NULL,
                    generation_id TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS research_query_saved (
                    owner_id TEXT NOT NULL,
                    query_id TEXT NOT NULL,
                    version INTEGER NOT NULL,
                    name TEXT NOT NULL,
                    sql TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY(owner_id, query_id)
                );
                """
            )
            install_screen_query_tables(connection)
            from rquant.ai_usage import install_ai_usage_tables

            install_ai_usage_tables(connection)
            self._ensure_column(connection, "processing_owner", "TEXT")
            self._ensure_column(connection, "lease_expires_at", "TEXT")
            self._ensure_column(connection, "attempt_count", "INTEGER NOT NULL DEFAULT 0")
            self._ensure_column(connection, "claim_token", "TEXT")
            self._ensure_column(connection, "authorization_json", "TEXT")
            try:
                connection.execute("ALTER TABLE page_control_effect ADD COLUMN original_admission_json TEXT")
            except sqlite3.OperationalError as exc:
                if "duplicate column name" not in str(exc):
                    raise
            connection.commit()
            self._activate_safe_effect_journal_protocol(connection)

    @staticmethod
    def _ensure_column(connection: sqlite3.Connection, name: str, definition: str) -> None:
        try:
            connection.execute(f"ALTER TABLE page_control_command ADD COLUMN {name} {definition}")
        except sqlite3.OperationalError as exc:
            if "duplicate column name" not in str(exc):
                raise

    @staticmethod
    def _activate_safe_effect_journal_protocol(connection: sqlite3.Connection) -> None:
        activated_at = datetime.now(UTC).isoformat(timespec="microseconds")
        connection.execute("BEGIN IMMEDIATE")
        try:
            marker = connection.execute(
                """
                SELECT 1 FROM page_control_protocol_activation
                WHERE marker_name = ?
                """,
                (_SAFE_EFFECT_JOURNAL_MARKER,),
            ).fetchone()
            if marker is None:
                PageControlOutbox._terminalize_unjournaled_external_processing(
                    connection,
                    observed_at=activated_at,
                )
                connection.execute(
                    """
                    INSERT INTO page_control_protocol_activation(
                        marker_name, protocol_version, activated_at
                    ) VALUES (?, ?, ?)
                    """,
                    (
                        _SAFE_EFFECT_JOURNAL_MARKER,
                        _SAFE_EFFECT_JOURNAL_VERSION,
                        activated_at,
                    ),
                )
            connection.commit()
        except Exception:
            connection.rollback()
            raise

    @staticmethod
    def _terminalize_unjournaled_external_processing(
        connection: sqlite3.Connection,
        *,
        observed_at: str,
    ) -> None:
        rows = connection.execute(
            """
            SELECT
                c.command_id,
                c.command_kind,
                c.command_hash,
                c.payload_json
            FROM page_control_command AS c
            LEFT JOIN page_control_effect AS e
                ON e.command_id = c.command_id
            WHERE c.status = ?
              AND c.command_kind IN (?, ?, ?)
              AND (c.lease_expires_at IS NULL OR c.lease_expires_at <= ?)
              AND e.command_id IS NULL
            """,
            (
                PageControlStatus.PROCESSING.value,
                *sorted(_EXTERNAL_LAB_COMMAND_KINDS),
                observed_at,
            ),
        ).fetchall()
        for row in rows:
            result = _ambiguous_external_effect_result_from_row(row)
            connection.execute(
                """
                UPDATE page_control_command
                SET status = ?, completed_at = ?, result_json = ?, error = ?,
                    processing_owner = NULL, lease_expires_at = NULL,
                    claim_token = NULL
                WHERE command_id = ? AND status = ?
                """,
                (
                    PageControlStatus.AMBIGUOUS.value,
                    observed_at,
                    json.dumps(result, ensure_ascii=True),
                    "external Lab effect lacks a PageControl effect journal",
                    row["command_id"],
                    PageControlStatus.PROCESSING.value,
                ),
            )

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path, timeout=5.0)
        connection.row_factory = sqlite3.Row
        return connection

    def _ai_usage_transaction(self, operation: Callable[[AIUsageRepository], _AIUsageT]) -> _AIUsageT:
        from contextlib import closing
        from rquant.ai_usage import AIUsageRepository

        with closing(self._connect()) as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                result = operation(AIUsageRepository(connection))
                connection.commit()
                return result
            except BaseException:
                connection.rollback()
                raise

    def ai_usage_reserve(self, binding: AIRequestBinding, *, daily_limit: int) -> AIUsageRecord:
        return self._ai_usage_transaction(lambda repo: repo.reserve(binding, daily_limit=daily_limit))

    def ai_usage_lookup(self, owner_uid: str, request_id: UUID, body_sha256: str | None = None) -> AIUsageRecord:
        return self._ai_usage_transaction(lambda repo: repo.lookup(owner_uid, request_id, body_sha256))

    def ai_usage_dispatch(self, owner_uid: str, request_id: UUID, body_sha256: str, *, now: datetime) -> AIUsageDispatch:
        return self._ai_usage_transaction(lambda repo: repo.dispatch(owner_uid, request_id, body_sha256, now=now))

    def ai_usage_finish(self, owner_uid: str, request_id: UUID, body_sha256: str, *, dispatch_token: str,
                        now: datetime, usage: AIMeasuredUsage, result: JsonValue | None,
                        error_code: str | None = None) -> AIUsageRecord:
        return self._ai_usage_transaction(lambda repo: repo.finish(owner_uid, request_id, body_sha256,
            dispatch_token=dispatch_token, now=now, usage=usage, result=result, error_code=error_code))

    def ai_usage_release(self, owner_uid: str, request_id: UUID, body_sha256: str, *, now: datetime,
                         reason: str) -> AIUsageRecord:
        return self._ai_usage_transaction(lambda repo: repo.release(owner_uid, request_id, body_sha256, now=now, reason=reason))

    def ai_usage_unknown(self, owner_uid: str, request_id: UUID, body_sha256: str, *, dispatch_token: str,
                         now: datetime) -> AIUsageRecord:
        return self._ai_usage_transaction(lambda repo: repo.unknown(owner_uid, request_id, body_sha256, dispatch_token=dispatch_token, now=now))

    def ai_usage_recover_dispatches(self, *, now: datetime) -> int:
        return self._ai_usage_transaction(lambda repo: repo.recover_dispatches(now=now))

    def ai_usage_summary(self, owner_uid: str, account_id: str, *, start_date: date, end_date: date) -> AIUsageSummary:
        return self._ai_usage_transaction(lambda repo: repo.summary(owner_uid, account_id, start_date=start_date, end_date=end_date))

    def ai_usage_request_exists(self, request_id: UUID) -> bool:
        return self._ai_usage_transaction(lambda repo: repo.connection.execute("SELECT 1 FROM ai_request WHERE request_id=?", (str(request_id),)).fetchone() is not None)

    def ai_usage_account_calls(self, account_id: str, budget_date: date) -> int:
        return self._ai_usage_transaction(lambda repo: int(repo.connection.execute("SELECT COUNT(*) FROM ai_request WHERE account_id=? AND budget_date=? AND state!='not_dispatched'", (account_id, budget_date.isoformat())).fetchone()[0]))

    def authorize_ai_portfolio_result(self, owner_uid: str, job_id: UUID) -> SubmitPortfolioBacktest:
        from contextlib import closing
        from rquant.portfolio_backtest_commands import SubmitPortfolioBacktest, portfolio_job_id
        from rquant.lab_job_center import CommandSubmissionReceipt
        authority = self.collaboration
        if authority is not None and authority.mode == "enforced":
            with authority.locked():
                authority.require_outbox_path(self.path)
                authority.current_role(owner_uid)
                with closing(self._connect()) as connection:
                    connection.execute("PRAGMA query_only = ON")
                    connection.execute("BEGIN")
                    for row in self._result_submission_rows(connection, "submit_portfolio_backtest"):
                        proof = self._result_submission_binding(row, authority)
                        if proof is not None and proof.owner_id == owner_uid and UUID(proof.job_id) == job_id:
                            command = SubmitPortfolioBacktest.model_validate_json(row["payload_json"])
                            if portfolio_job_id(owner_uid, command.command_id) != job_id:
                                raise PermissionError("original AI result owner submission differs")
                            return command
            raise PermissionError("original proven portfolio result owner is unavailable")
        with closing(self._connect()) as connection:
            rows = connection.execute("SELECT * FROM page_control_command WHERE command_kind='submit_portfolio_backtest' AND json_extract(payload_json,'$.actor_id')=?", (owner_uid,))
            for row in rows:
                command = SubmitPortfolioBacktest.model_validate_json(row["payload_json"])
                if command.actor_id == owner_uid and portfolio_job_id(owner_uid, command.command_id) == job_id:
                    effect=connection.execute('SELECT * FROM page_control_effect WHERE command_id=?',(command.command_id,)).fetchone()
                    if row["status"] != "succeeded" or _command_hash(command) != row["command_hash"] or effect is None or effect['status']!='succeeded' or effect['command_hash']!=row['command_hash'] or effect['effect_kind']!=command.kind:
                        raise PermissionError("original portfolio owner receipt is not complete")
                    receipt=CommandSubmissionReceipt.model_validate_json(row['result_json'])
                    effect_receipt=CommandSubmissionReceipt.model_validate_json(effect['result_json'])
                    if receipt!=effect_receipt or (receipt.job_id,receipt.command_type)!=(job_id,'submit'):
                        raise PermissionError('original portfolio command/effect/job binding differs')
                    return command
        raise PermissionError("original portfolio result is not owned by this viewer")

    def enqueue(self, command: PageControlCommandValue) -> PageControlReceipt:
        if isinstance(command, PROMOTION_PUBLIC_TYPES):
            raise ValueError("manual promotion requires trusted private submission")
        if isinstance(command, TASK_CONTROL_PUBLIC_TYPES):
            raise ValueError("task controls require trusted private submission")
        if isinstance(command, (SetPaperAccountPaused, SavePaperPortfolioConfiguration, RunPaperPortfolioResearch)):
            raise ValueError("paper portfolio requires trusted submission")
        if isinstance(command, _CONDITION_PUBLIC_TYPES):
            raise ValueError("condition rule requires trusted submission")
        if isinstance(command, (ExecuteScreenQuery, _OwnedSaveNlPreset, _OwnedAppendNlQueryLog)):
            raise ValueError("screen history requires trusted submission")
        if isinstance(
            command, (SaveStrategyTemplate, ArchiveStrategyTemplate, RunStrategyTemplate)
        ):
            raise ValueError("strategy authoring requires trusted submission")
        if isinstance(command, SaveResearchQuery):
            raise ValueError("research queries require trusted submission")
        if isinstance(command, AckAlert):
            raise ValueError("ack_alert requires verified Serving eligibility")
        if isinstance(command, (AddWatchlistItem, RemoveWatchlistItem)):
            raise ValueError("watchlist commands require trusted submission")
        if isinstance(
            command, (SavePriceAlertRule, SetPriceAlertRuleEnabled, DeletePriceAlertRule)
        ):
            raise ValueError("price rule commands require trusted submission")
        if isinstance(
            command, (SaveFactorDefinition, ArchiveFactor, SubmitFactorRun, SetFactorTracked)
        ):
            raise ValueError("factor commands require trusted submission")
        return self._enqueue(command)

    def enqueue_verified_ack(self, command: AckAlert) -> PageControlReceipt:
        """Internal admission point for a future verified Serving event lookup."""
        if self.alert_ack_activated_at() is None:
            raise ValueError("alert acknowledgment is not activated")
        return self._enqueue(command)

    def enqueue_trusted_watchlist(
        self, command: AddWatchlistItem | RemoveWatchlistItem
    ) -> PageControlReceipt:
        if not isinstance(command, (AddWatchlistItem, RemoveWatchlistItem)):
            raise TypeError("trusted watchlist submission requires a watchlist command")
        return self._enqueue(command, require_watchlist_activation=True)

    def enqueue_trusted_price_rule(self, command: _OwnedPriceAlertRuleValue) -> PageControlReceipt:
        if not isinstance(
            command,
            (_OwnedSavePriceAlertRule, _OwnedSetPriceAlertRuleEnabled, _OwnedDeletePriceAlertRule),
        ):
            raise TypeError("trusted price rule submission requires an owned command")
        return self._enqueue(command, require_price_rule_activation=True)

    def enqueue_trusted_condition_rule(
        self, command: _OwnedConditionAlertRuleValue
    ) -> PageControlReceipt:
        if type(command) not in _CONDITION_OWNED_TYPES:
            raise TypeError("condition rule requires an exact owned command")
        return self._enqueue(command, require_condition_rule_activation=True)

    def enqueue_trusted_factor_definition(
        self, command: _OwnedFactorDefinitionValue
    ) -> PageControlReceipt:
        if not isinstance(command, (_OwnedSaveFactorDefinition, _OwnedArchiveFactor)):
            raise TypeError("trusted factor submission requires an owned command")
        return self._enqueue(command, require_factor_definition_trust=True)

    def enqueue_trusted_factor_run(self, command: _OwnedSubmitFactorRun) -> PageControlReceipt:
        if type(command) is not _OwnedSubmitFactorRun:
            raise TypeError("factor run requires an owned command")
        return self._enqueue(command, require_factor_run_trust=True)

    def enqueue_trusted_factor_tracking(
        self, command: _OwnedSetFactorTracked
    ) -> PageControlReceipt:
        if type(command) is not _OwnedSetFactorTracked:
            raise TypeError("tracking requires an owned command")
        return self._enqueue(command, require_factor_tracking_trust=True)

    def enqueue_trusted_strategy_authoring(
        self, command: OwnedStrategyTemplateCommand
    ) -> PageControlReceipt:
        if type(command) not in (
            OwnedSaveStrategyTemplate,
            OwnedArchiveStrategyTemplate,
            OwnedRunStrategyTemplate,
        ):
            raise TypeError("strategy authoring requires an owned command")
        return self._enqueue(command, require_strategy_authoring_trust=True)

    def enqueue_trusted_strategy_promotion(self, command: OwnedStrategyPromotionCommand) -> PageControlReceipt:
        if type(command) not in PROMOTION_OWNED_TYPES:
            raise TypeError("private promotion command required")
        return self._enqueue(command, require_strategy_promotion_trust=True)

    def enqueue_trusted_paper_portfolio(
        self, command: OwnedPaperPortfolioCommand
    ) -> PageControlReceipt:
        if type(command) not in (OwnedSetPaperAccountPaused, OwnedSavePaperPortfolioConfiguration, OwnedRunPaperPortfolioResearch):
            raise TypeError("paper portfolio requires an owned command")
        return self._enqueue(command, require_paper_portfolio_trust=True)


    def _enqueue(
        self,
        command: PageControlCommandValue,
        *,
        require_watchlist_activation: bool = False,
        require_price_rule_activation: bool = False,
        require_condition_rule_activation: bool = False,
        require_factor_definition_trust: bool = False,
        require_factor_run_trust: bool = False,
        require_factor_tracking_trust: bool = False,
        require_research_query_trust: bool = False,
        require_strategy_authoring_trust: bool = False,
        require_screen_query_trust: bool = False,
        require_paper_portfolio_trust: bool = False,
        require_task_control_trust: bool = False,
        require_strategy_promotion_trust: bool = False,
    ) -> PageControlReceipt:
        if isinstance(command, PROMOTION_PUBLIC_TYPES) != require_strategy_promotion_trust or (
            require_strategy_promotion_trust and type(command) not in PROMOTION_OWNED_TYPES
        ):
            raise ValueError("manual promotion requires trusted private submission")
        if require_strategy_promotion_trust and (
            self.collaboration is None or self.collaboration.mode != "enforced"
        ):
            raise PermissionError("manual promotion requires installed current roles")
        if isinstance(command, TASK_CONTROL_PUBLIC_TYPES) != require_task_control_trust or (
            require_task_control_trust and type(command) not in TASK_CONTROL_OWNED_TYPES
        ):
            raise ValueError("task controls require trusted private submission")
        if isinstance(command, (SetPaperAccountPaused, SavePaperPortfolioConfiguration, RunPaperPortfolioResearch)) != require_paper_portfolio_trust or (
            require_paper_portfolio_trust and type(command) not in (OwnedSetPaperAccountPaused, OwnedSavePaperPortfolioConfiguration, OwnedRunPaperPortfolioResearch)
        ):
            raise ValueError("paper portfolio requires trusted submission")
        if isinstance(command, _CONDITION_PUBLIC_TYPES) != require_condition_rule_activation or (
            require_condition_rule_activation and type(command) not in _CONDITION_OWNED_TYPES
        ):
            raise ValueError("condition rule requires trusted submission")
        if isinstance(
            command, (ExecuteScreenQuery, _OwnedSaveNlPreset)
        ) != require_screen_query_trust or (
            require_screen_query_trust
            and type(command) not in (_OwnedExecuteScreenQuery, _OwnedSaveNlPreset)
        ):
            raise ValueError("screen history requires trusted submission")
        if isinstance(
            command, (SaveStrategyTemplate, ArchiveStrategyTemplate, RunStrategyTemplate)
        ) != require_strategy_authoring_trust or (
            require_strategy_authoring_trust
            and type(command)
            not in (
                OwnedSaveStrategyTemplate,
                OwnedArchiveStrategyTemplate,
                OwnedRunStrategyTemplate,
            )
        ):
            raise ValueError("strategy authoring requires trusted submission")
        if isinstance(command, SaveResearchQuery) != require_research_query_trust or (
            require_research_query_trust and type(command) is not _OwnedSaveResearchQuery
        ):
            raise ValueError("research queries require trusted submission")
        if (
            isinstance(command, (AddWatchlistItem, RemoveWatchlistItem))
            != require_watchlist_activation
        ):
            raise ValueError("watchlist commands require trusted submission")
        is_price_rule = isinstance(
            command, (SavePriceAlertRule, SetPriceAlertRuleEnabled, DeletePriceAlertRule)
        )
        is_owned_price_rule = isinstance(
            command,
            (_OwnedSavePriceAlertRule, _OwnedSetPriceAlertRuleEnabled, _OwnedDeletePriceAlertRule),
        )
        if is_price_rule != require_price_rule_activation or (
            require_price_rule_activation and not is_owned_price_rule
        ):
            raise ValueError("price rule commands require trusted submission")
        is_factor = isinstance(command, (SaveFactorDefinition, ArchiveFactor))
        is_owned_factor = isinstance(command, (_OwnedSaveFactorDefinition, _OwnedArchiveFactor))
        if is_factor != require_factor_definition_trust or (
            require_factor_definition_trust and not is_owned_factor
        ):
            raise ValueError("factor commands require trusted submission")
        if isinstance(command, SubmitFactorRun) != require_factor_run_trust or (
            require_factor_run_trust and type(command) is not _OwnedSubmitFactorRun
        ):
            raise ValueError("factor runs require trusted submission")
        if isinstance(command, SetFactorTracked) != require_factor_tracking_trust or (
            require_factor_tracking_trust and type(command) is not _OwnedSetFactorTracked
        ):
            raise ValueError("tracking requires trusted submission")
        payload = command.model_dump_json()
        command_hash = _command_hash(command)
        authorization = None
        if self.collaboration is not None and self.collaboration.mode == "enforced":
            actor = _TRUSTED_COLLABORATION_ACTOR.get()
            if actor is None:
                raise PermissionError("command requires the original trusted private ingress")
            authorization = self.collaboration.issue_authorization(actor, command.model_dump(mode="json"))
        enqueued_at = (command.accepted_at if type(command) in TASK_CONTROL_OWNED_TYPES else command.requested_at).isoformat(timespec="microseconds")
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            if require_condition_rule_activation:
                self._require_condition_rule_activation(connection, command.owner_id)
            if require_watchlist_activation and not self._manual_watchlist_activated(connection):
                raise ValueError("manual watchlist is not activated")
            if require_price_rule_activation:
                assert isinstance(
                    command,
                    (
                        _OwnedSavePriceAlertRule,
                        _OwnedSetPriceAlertRuleEnabled,
                        _OwnedDeletePriceAlertRule,
                    ),
                )
                self._require_price_rule_activation(connection, command.owner_id)
            existing = connection.execute(
                "SELECT * FROM page_control_command WHERE command_id = ?",
                (command.command_id,),
            ).fetchone()
            if existing is not None:
                if existing["command_hash"] != command_hash:
                    raise PageControlCommandConflictError(
                        "command_id already exists with different payload"
                    )
                if authorization is not None:
                    original = self._row_authorization(existing)
                    if original is None or original.actor_id != authorization.actor_id:
                        raise PermissionError("original trusted command actor is unavailable or differs")
                if (
                    require_task_control_trust
                    or require_condition_rule_activation
                    or require_price_rule_activation
                    or require_factor_definition_trust
                    or require_strategy_authoring_trust
                    or require_strategy_promotion_trust
                    or require_paper_portfolio_trust
                ):
                    stored = _COMMAND_ADAPTER.validate_json(existing["payload_json"])
                    if (
                        stored != command
                        or existing["command_kind"] != command.kind
                        or _command_hash(stored) != command_hash
                    ):
                        raise PageControlCommandConflictError(
                            "command_id already exists with different payload"
                        )
                return self._receipt(existing)
            if require_strategy_promotion_trust:
                # Client requested_at never changes these persistent rate limits.
                count = connection.execute(
                    "SELECT COUNT(*) FROM page_control_command WHERE command_kind IN (?,?,?,?) "
                    "AND json_extract(payload_json,'$.owner_id')=? "
                    "AND julianday(json_extract(payload_json,'$.accepted_at'))>julianday(?)",
                    (*sorted(_STRATEGY_PROMOTION_KINDS), command.owner_id,
                        (command.accepted_at - timedelta(minutes=1)).isoformat()),
                ).fetchone()[0]
                if count >= 30:
                    raise StrategyPromotionRateLimitError("每分钟最多 30 次操作，请稍后重试。")
                if type(command) is OwnedApprovePromotion and connection.execute(
                    "SELECT 1 FROM page_control_command WHERE command_kind='approve_promotion' "
                    "AND json_extract(payload_json,'$.owner_id')=? "
                    "AND json_extract(payload_json,'$.target.strategy_id')=? "
                    "AND julianday(json_extract(payload_json,'$.accepted_at'))>julianday(?) LIMIT 1",
                    (command.owner_id, command.target.strategy_id,
                        (command.accepted_at - timedelta(minutes=10)).isoformat()),
                ).fetchone() is not None:
                    raise StrategyPromotionRateLimitError("同一策略每 10 分钟只能批准一次。")
            if require_task_control_trust:
                if len(payload.encode()) > 32 * 1024:
                    raise ValueError("task control effect exceeds 32 KiB")
                count = connection.execute(
                    "SELECT COUNT(*) FROM page_control_command WHERE command_kind IN ("
                    + ",".join("?" for _ in TASK_CONTROL_KINDS) + ")",
                    tuple(sorted(TASK_CONTROL_KINDS)),
                ).fetchone()[0]
                if count >= 4096:
                    raise ValueError("task control history exceeds 4096 commands")
            connection.execute(
                """
                INSERT INTO page_control_command(
                    command_id, command_kind, command_hash, payload_json,
                    status, enqueued_at, authorization_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    command.command_id,
                    command.kind,
                    command_hash,
                    payload,
                    PageControlStatus.PENDING.value,
                    enqueued_at,
                    None if authorization is None else authorization.model_dump_json(),
                ),
            )
            if type(command) is _OwnedExecuteScreenQuery:
                register_screen_execution(connection, command)
        receipt = self.receipt(command.command_id)
        assert receipt is not None
        return receipt

    def _row_authorization(self, row: sqlite3.Row) -> CommandAuthorization | None:
        return self._journal_authorization(row, self.collaboration)

    @staticmethod
    def _journal_authorization(
        row: sqlite3.Row, collaboration: PageControlRoleAuthority | None,
    ) -> CommandAuthorization | None:
        if "authorization_json" not in row.keys() or row["authorization_json"] is None:
            return None
        try:
            raw = row["authorization_json"]
            if not isinstance(raw, str) or len(raw.encode()) > 4096:
                raise ValueError("authorization metadata exceeds capacity")
            from rquant.strict_json import strict_json_loads
            strict_json_loads(raw)
            proof = CommandAuthorization.model_validate_json(raw)
            if collaboration is not None and collaboration.mode == "enforced":
                if proof.journal_identity != collaboration.require_outbox_identity():
                    raise ValueError("authorization belongs to another original journal")
            stored = _COMMAND_ADAPTER.validate_json(row["payload_json"])
            if (proof.command_id != row["command_id"] or proof.command_kind != row["command_kind"]
                    or proof.command_sha256 != row["command_hash"]
                    or _command_hash(stored) != proof.command_sha256
                    or stored.command_id != proof.command_id or stored.kind != proof.command_kind):
                raise ValueError("original trusted command binding differs")
            return proof
        except (ValueError, TypeError, KeyError) as exc:
            raise PermissionError("original trusted command provenance is unavailable") from exc

    def trusted_command_actor(self, command_id: str, command_hash: str) -> str | None:
        with self._connect() as connection:
            connection.execute("PRAGMA query_only = ON")
            connection.execute("BEGIN")
            row = self._bounded_command_row(connection, command_id)
        if row is None or row["command_hash"] != command_hash:
            return None
        proof = self._row_authorization(row)
        return None if proof is None else proof.actor_id

    @staticmethod
    def _bounded_command_row(connection: sqlite3.Connection, command_id: str) -> sqlite3.Row | None:
        if not connection.in_transaction:
            raise ValueError("original command read snapshot is required")
        columns = {item[1] for item in connection.execute("PRAGMA table_info(page_control_command)")}
        authorization = "authorization_json" if "authorization_json" in columns else "NULL"
        lengths = connection.execute(f"""
            SELECT length(CAST(command_id AS BLOB)),length(CAST(command_kind AS BLOB)),
                length(CAST(command_hash AS BLOB)),length(CAST(payload_json AS BLOB)),
                length(CAST({authorization} AS BLOB))
            FROM page_control_command WHERE command_id=?
        """, (command_id,)).fetchone()
        if lengths is None:
            return None
        sizes = tuple(int(item or 0) for item in lengths)
        if any(size > 128 for size in sizes[:3]) or sizes[3] > 1024 * 1024 or sizes[4] > 4096:
            raise ValueError("original command or authorization exceeds read capacity")
        return connection.execute(f"""
            SELECT command_id,command_kind,command_hash,payload_json,
                {authorization} AS authorization_json
            FROM page_control_command WHERE command_id=?
        """, (command_id,)).fetchone()

    @staticmethod
    def _factor_job_origin(command: _OwnedSubmitFactorRun) -> tuple[str, FactorJobRecord] | None:
        from rquant.factor.job_ledger import FactorEvaluationJobLedger, _checked_command

        ledger = FactorEvaluationJobLedger.open_existing(command.ledger_identity)
        with ledger._reader() as connection:
            identity = connection.execute("SELECT job_id FROM factor_jobs WHERE spec_sha256 = ?",
                (command.spec.spec_sha256,)).fetchone()
            if identity is None:
                return None
            state = ledger._load_job(connection, identity["job_id"])
            anchor = connection.execute("SELECT * FROM factor_commands WHERE job_id = ? ORDER BY rowid LIMIT 1",
                (identity["job_id"],)).fetchone()
            if state is None or anchor is None:
                raise PermissionError("original factor job origin is unavailable")
            original_id, original_spec, original_job = _checked_command(anchor)
            if state.spec != command.spec or (original_spec, original_job) != (state.spec_sha256, state.job_id):
                raise PermissionError("original factor job origin differs")
            return original_id, state.public()

    @staticmethod
    def _result_submission_binding(
        row: sqlite3.Row, collaboration: PageControlRoleAuthority, *,
        restored_private_owner: str | None = None,
    ) -> ResultOwnerProof | None:
        from rquant.portfolio_backtest_commands import PortfolioRunEffect, portfolio_interaction, portfolio_job_id
        from rquant.strict_json import strict_json_loads
        from rquant.web.models.collaboration import ResultOwnerProof

        command = _COMMAND_ADAPTER.validate_json(row["payload_json"])
        kind = command.kind
        if (row["status"] != "succeeded" or row["verified_effect_status"] != "succeeded"
                or _command_hash(command) != row["command_hash"]
                or row["verified_effect_hash"] != row["command_hash"] or row["verified_effect_kind"] != kind):
            raise PermissionError("original result command/effect binding differs")
        result = strict_json_loads(row["verified_effect_result"])
        completed = strict_json_loads(row["result_json"])
        if not isinstance(result, dict) or result != completed:
            raise PermissionError("original accepted command and effect receipts differ")
        proof = PageControlOutbox._journal_authorization(row, collaboration)
        owner = restored_private_owner if proof is None else proof.actor_id
        if owner is None:
            return None
        if type(command) is _OwnedSubmitFactorRun:
            domain, spec_hash = "factor", command.spec.spec_sha256
            if command.actor_id != owner or result.get("spec_sha256") != spec_hash:
                raise PermissionError("original factor owner/spec acceptance differs")
            job_id = UUID(str(result["job_id"])).hex
            origin = PageControlOutbox._factor_job_origin(command)
            if origin is None or origin[1].job_id != job_id:
                raise PermissionError("original factor ledger binding differs")
            if origin[0] != command.command_id:
                # A new UUID can reuse a job, but cannot replace its first owner.
                return None
        elif type(command) is OwnedRunStrategyTemplate:
            domain, spec_hash = "strategy", command.accepted.spec.spec_hash
            job_id = str(UUID(str(result["job_id"])))
            if (command.owner_id != owner or result.get("owner_id") != owner
                    or result.get("spec_hash") != spec_hash or UUID(command.command_id) != UUID(job_id)
                    or result.get("command_id") != command.command_id):
                raise PermissionError("original strategy owner/spec acceptance differs")
        elif type(command) is SubmitPortfolioBacktest:
            if proof is None:
                return None
            raw = row["original_admission_json"]
            if not isinstance(raw, str) or len(raw.encode()) > 1024 * 1024:
                raise PermissionError("original portfolio admission is unavailable")
            strict_json_loads(raw)
            marker = PortfolioRunEffect.model_validate_json(raw)
            arguments = {item.name: item.value for item in marker.command.spec.parameters.arguments}
            if (marker.command_hash != canonical_sha256(command) or marker.config_hash != command.config.config_hash
                    or arguments.get("config_hash") != marker.config_hash
                    or marker.command.job_id != portfolio_job_id(owner, command.command_id)
                    or marker.interaction_key != portfolio_interaction(command)
                    or marker.command.spec.schema_version != 3
                    or marker.command.spec.parameters.strategy_name != "portfolio_backtest"
                    or command.actor_id != owner or result.get("result") != "submitted"
                    or UUID(str(result["job_id"])) != marker.command.job_id):
                raise PermissionError("original portfolio admission differs")
            domain, spec_hash, job_id = "portfolio", marker.command.spec.spec_hash, str(marker.command.job_id)
        elif type(command) is SubmitMinuteReplay:
            from rquant.minute_backtest_commands import (
                MinuteParameterRunConfig, MinuteRunEffect, _minute_command_hash, _minute_config_hash,
                minute_interaction, minute_job_id,
            )
            from rquant.minute_backtest_formal_adapter import MinuteFormalParameters

            if proof is None:
                return None
            raw = row["original_admission_json"]
            if not isinstance(raw, str) or len(raw.encode()) > 1024 * 1024:
                raise PermissionError("original minute admission is unavailable")
            strict_json_loads(raw)
            marker = MinuteRunEffect.model_validate_json(raw)
            spec, config = marker.command.spec, command.config
            experiment = spec.experiment
            if type(config) is MinuteParameterRunConfig:
                from rquant.minute_backtest_parameter_adapter import MinuteParameterFormalParameters
                from rquant.minute_backtest_parameter_producer import MinuteParameterPreparedPublication

                parameters = MinuteParameterFormalParameters.model_validate({item.name: item.value for item in spec.parameters.arguments})
                if parameters.prepared_publication_json is None:
                    raise PermissionError("original parameter admission lacks its full baseline publication")
                prepared = MinuteParameterPreparedPublication.model_validate_json(parameters.prepared_publication_json)
                from rquant.minute_backtest_parameter_study import verify_minute_parameter_study_request

                verify_minute_parameter_study_request(prepared.study_binding, config)
                if (marker.command_hash != _minute_command_hash(command) or marker.config_hash != _minute_config_hash(config)
                        or marker.command.job_id != minute_job_id(owner, command.command_id)
                        or marker.interaction_key != minute_interaction(command)
                        or spec.schema_version != 3 or spec.parameters.strategy_name != "minute_parameter_replay"
                        or command.actor_id != owner or result.get("result") != "submitted"
                        or UUID(str(result["job_id"])) != marker.command.job_id or experiment is None
                        or (parameters.owner_id, prepared.baseline.owner_id, prepared.baseline.source_key,
                            prepared.baseline.source_version, prepared.baseline.full_input_hash,
                            parameters.parameter_set_json, parameters.parameter_hash,
                            parameters.native_strategy_id, parameters.native_strategy_version, spec.deadline, spec.random_seed,
                            experiment.spec.train_range, experiment.spec.validation_range, experiment.spec.frozen_outer_test_range) != (
                            owner, owner, config.source_key, config.source_version, config.full_input_hash,
                            config.parameters.model_dump_json(), config.parameters.fingerprint,
                            config.parameters.definition_id, config.parameters.definition_version, config.deadline, config.random_seed,
                            config.protocol.train_range, config.protocol.validation_range, config.protocol.frozen_outer_test_range)):
                    raise PermissionError("original complete parameter admission differs")
            else:
                parameters = MinuteFormalParameters.model_validate({item.name: item.value for item in spec.parameters.arguments})
                if (marker.command_hash != canonical_sha256(command) or marker.config_hash != canonical_sha256(config)
                        or marker.command.job_id != minute_job_id(owner, command.command_id)
                        or marker.interaction_key != minute_interaction(command)
                        or spec.schema_version != 3 or spec.parameters.strategy_name != "minute_runtime_replay"
                        or command.actor_id != owner or result.get("result") != "submitted"
                        or UUID(str(result["job_id"])) != marker.command.job_id or experiment is None
                        or (parameters.owner_id, parameters.source_key, parameters.source_version, parameters.full_input_hash,
                            parameters.native_strategy_id, parameters.native_strategy_version, spec.deadline, spec.random_seed,
                            experiment.spec.train_range, experiment.spec.validation_range, experiment.spec.frozen_outer_test_range) != (
                            owner, config.source_key, config.source_version, config.full_input_hash, config.native_id,
                            config.native_version, config.deadline, config.random_seed, config.protocol.train_range,
                            config.protocol.validation_range, config.protocol.frozen_outer_test_range)):
                    raise PermissionError("original minute admission differs")
            domain, spec_hash, job_id = "minute", spec.spec_hash, str(marker.command.job_id)
        else:
            raise PermissionError("unknown original result submission kind")
        return ResultOwnerProof(domain=domain, job_id=job_id, spec_hash=spec_hash, owner_id=owner,
            command_id=row["command_id"], command_sha256=row["command_hash"],
            effect_sha256=canonical_sha256(result), worker_owner_id=row["effect_worker_owner_id"])

    @staticmethod
    def _result_submission_rows(connection: sqlite3.Connection, kind: str) -> Iterator[sqlite3.Row]:
        # Bound transfers before loading full original bodies or saved admission material.
        metadata = connection.execute("""
            SELECT c.rowid,
                length(CAST(c.payload_json AS BLOB)),length(CAST(c.result_json AS BLOB)),
                length(CAST(c.authorization_json AS BLOB)),length(CAST(e.result_json AS BLOB)),
                length(CAST(e.original_admission_json AS BLOB))
            FROM page_control_command c JOIN page_control_effect e ON c.command_id=e.command_id
            WHERE c.command_kind=? AND c.status='succeeded' AND e.status='succeeded'
            ORDER BY c.rowid DESC LIMIT 4097
        """, (kind,))
        total = 0
        for count, item in enumerate(metadata, start=1):
            sizes = tuple(int(value or 0) for value in item[1:])
            total += sum(sizes)
            if count > 4096 or any(size > 1024 * 1024 for size in sizes) or sizes[2] > 4096 or total > 4 * 1024 * 1024:
                raise PermissionError("original result submission history exceeds read capacity")
            row = connection.execute("""
                SELECT c.*,e.status AS verified_effect_status,e.command_hash AS verified_effect_hash,
                    e.effect_kind AS verified_effect_kind,e.result_json AS verified_effect_result,
                    e.original_admission_json,e.owner_id AS effect_worker_owner_id
                FROM page_control_command c JOIN page_control_effect e ON c.command_id=e.command_id
                WHERE c.rowid=?
            """, (item[0],)).fetchone()
            if row is None:
                raise PermissionError("original result submission disappeared")
            yield row

    def original_command_bytes(self, command_id: str) -> bytes:
        with self._connect() as connection:
            connection.execute("PRAGMA query_only = ON")
            connection.execute("BEGIN")
            row = self._bounded_command_row(connection, command_id)
        if row is None:
            raise KeyError("original command is absent")
        self._row_authorization(row)
        return row["payload_json"].encode("utf-8")

    def require_current_command_role(self, command: PageControlCommandValue) -> str | None:
        if self.collaboration is None or self.collaboration.mode != "enforced":
            return
        actor = self.trusted_command_actor(command.command_id, _command_hash(command))
        if actor is None:
            raise PermissionError("original trusted command actor is unavailable")
        self.collaboration.require_command(actor, command.kind)
        return actor

    def require_active_claim(
        self, command: PageControlCommandValue, *, owner_id: str, claim_token: str,
    ) -> None:
        from contextlib import closing

        if not owner_id or not claim_token or len(owner_id) > 128 or len(claim_token) > 128:
            raise PermissionError("original current claim identity is invalid")
        with closing(self._connect()) as connection:
            connection.execute("PRAGMA query_only = ON")
            connection.execute("BEGIN")
            row = self._bounded_command_row(connection, command.command_id)
            if row is None:
                raise PermissionError("original current claim is absent")
            stored = _COMMAND_ADAPTER.validate_json(row["payload_json"])
            if (stored != command or row["command_kind"] != command.kind
                    or row["command_hash"] != _command_hash(command)):
                raise PermissionError("original current claim body binding differs")
            proof = self._row_authorization(row)
            if self.collaboration is not None and self.collaboration.mode == "enforced":
                if proof is None or getattr(command, "actor_id", proof.actor_id) != proof.actor_id:
                    raise PermissionError("original current claim actor is unavailable")
                self.collaboration.require_command(proof.actor_id, command.kind)
            sizes = connection.execute("""
                SELECT length(CAST(status AS BLOB)),length(CAST(processing_owner AS BLOB)),
                    length(CAST(claim_token AS BLOB))
                FROM page_control_command WHERE command_id=?
            """, (command.command_id,)).fetchone()
            if sizes is None or any(int(size or 0) > 128 for size in sizes):
                raise PermissionError("original current claim exceeds read capacity")
            current = connection.execute("""
                SELECT status,processing_owner,claim_token
                FROM page_control_command WHERE command_id=?
            """, (command.command_id,)).fetchone()
            if (current is None or current["status"] != PageControlStatus.PROCESSING.value
                    or current["processing_owner"] != owner_id or current["claim_token"] != claim_token):
                raise PermissionError("original current command claim changed")

    @contextmanager
    def command_fence(self, command: PageControlCommandValue) -> Iterator[None]:
        if self.collaboration is None or self.collaboration.mode != "enforced" or type(command) is SetUserRoleCommand:
            yield
            return
        with self.collaboration.locked(read_only=type(command) in (
                SubmitMinuteReplay, ExportMinuteReplayZip, SubmitMinuteParameterStudy)):
            actor = self.require_current_command_role(command)
            token = _TRUSTED_COLLABORATION_ACTOR.set(actor)
            try:
                yield
            finally:
                _TRUSTED_COLLABORATION_ACTOR.reset(token)

    def enqueue_trusted_screen_query(
        self, command: _OwnedExecuteScreenQuery | _OwnedSaveNlPreset
    ) -> PageControlReceipt:
        return self._enqueue(command, require_screen_query_trust=True)

    def lookup_screen_query_command(
        self, command: _OwnedExecuteScreenQuery | _OwnedSaveNlPreset
    ) -> PageControlReceipt | None:
        from contextlib import closing

        with closing(self._connect()) as connection:
            row = connection.execute(
                "SELECT * FROM page_control_command WHERE command_id=?", (command.command_id,)
            ).fetchone()
            if row is None:
                return None
            stored = _COMMAND_ADAPTER.validate_json(row["payload_json"])
            if (
                type(stored) not in (_OwnedExecuteScreenQuery, _OwnedSaveNlPreset)
                or stored.owner_id != command.owner_id
            ):
                return None
            if (
                stored != command
                or row["command_hash"] != _command_hash(command)
                or row["command_kind"] != command.kind
            ):
                raise PageControlCommandConflictError(
                    "screen command conflicts with original payload"
                )
            return self._receipt(row)

    def enqueue_trusted_research_query(
        self, command: _OwnedSaveResearchQuery
    ) -> PageControlReceipt:
        return self._enqueue(command, require_research_query_trust=True)

    def lookup_research_query_command(
        self, command: _OwnedSaveResearchQuery
    ) -> PageControlReceipt | None:
        from contextlib import closing

        with closing(self._connect()) as connection:
            row = connection.execute(
                "SELECT * FROM page_control_command WHERE command_id=?", (command.command_id,)
            ).fetchone()
            if row is None:
                return None
            if (
                row["command_hash"] != _command_hash(command)
                or _COMMAND_ADAPTER.validate_json(row["payload_json"]) != command
            ):
                raise PageControlCommandConflictError(
                    "query command conflicts with original actor or payload"
                )
            return self._receipt(row)

    def list_research_queries(self, owner_id: str) -> tuple[SavedResearchQuery, ...]:
        from contextlib import closing

        from rquant.research_query.saved import SavedResearchQuery

        checked = TypeAdapter(OwnerId).validate_python(owner_id)
        with closing(self._connect()) as connection:
            rows = connection.execute(
                "SELECT query_id,name,sql,version,updated_at FROM research_query_saved "
                "WHERE owner_id=? ORDER BY updated_at DESC,query_id LIMIT 100",
                (checked,),
            ).fetchall()
            return tuple(SavedResearchQuery.model_validate(dict(row)) for row in rows)

    def complete_research_query(
        self, claim: PageControlClaim, *, now: datetime
    ) -> PageControlReceipt:
        from rquant.research_query.saved import complete_saved_query

        return complete_saved_query(self, claim, now=now)

    @staticmethod
    def _manual_watchlist_activated(connection: sqlite3.Connection) -> bool:
        marker = connection.execute(
            "SELECT protocol_version FROM page_control_protocol_activation WHERE marker_name = ?",
            (_MANUAL_WATCHLIST_MARKER,),
        ).fetchone()
        return marker is not None and marker["protocol_version"] == _MANUAL_WATCHLIST_VERSION

    @staticmethod
    def _require_price_rule_activation(connection: sqlite3.Connection, owner_id: str) -> None:
        marker = connection.execute(
            "SELECT protocol_version, activated_at FROM page_control_protocol_activation "
            "WHERE marker_name = ?",
            (_PRICE_RULE_MARKER,),
        ).fetchone()
        if marker is None or marker["protocol_version"] != _PRICE_RULE_VERSION:
            raise ValueError("price rule protocol is not activated")
        raw_time = marker["activated_at"]
        if not isinstance(raw_time, str):
            raise ValueError("price rule activation marker is malformed")
        try:
            canonical_time = _normalize_utc(datetime.fromisoformat(raw_time)).isoformat(
                timespec="microseconds"
            )
        except ValueError as exc:
            raise ValueError("price rule activation marker is malformed") from exc
        if canonical_time != raw_time:
            raise ValueError("price rule activation marker is malformed")
        PriceAlertRuleRepository(connection).list_current(owner_id)

    def lookup_paper_portfolio_command(
        self, request: PaperPortfolioCommand, *, authenticated_actor_id: str
    ) -> tuple[OwnedPaperPortfolioCommand, PageControlReceipt] | None:
        if type(request) not in (SetPaperAccountPaused, SavePaperPortfolioConfiguration, RunPaperPortfolioResearch):
            raise TypeError("paper lookup requires an ownerless original request")
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM page_control_command WHERE command_id = ?", (request.command_id,)
            ).fetchone()
        if row is None:
            return None
        try:
            stored = _COMMAND_ADAPTER.validate_json(row["payload_json"])
        except ValueError as exc:
            raise PageControlCommandConflictError("stored paper command is invalid") from exc
        if (
            type(stored) not in (OwnedSetPaperAccountPaused, OwnedSavePaperPortfolioConfiguration, OwnedRunPaperPortfolioResearch)
            or stored.owner_id != authenticated_actor_id
            or row["command_kind"] != request.kind
            or stored.original() != request
            or _command_hash(stored) != row["command_hash"]
        ):
            raise PageControlCommandConflictError("command_id already exists with different payload or actor")
        return stored, self._receipt(row)


    def activate_price_alert_rules(self, activated_at: datetime) -> datetime:
        frozen = _normalize_utc(activated_at).isoformat(timespec="microseconds")
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            existing = connection.execute(
                "SELECT protocol_version, activated_at FROM page_control_protocol_activation "
                "WHERE marker_name = ?",
                (_PRICE_RULE_MARKER,),
            ).fetchone()
            if existing is None:
                PriceAlertRuleRepository(connection).install_schema()
                connection.execute(
                    "INSERT INTO page_control_protocol_activation "
                    "(marker_name, protocol_version, activated_at) VALUES (?, ?, ?)",
                    (_PRICE_RULE_MARKER, _PRICE_RULE_VERSION, frozen),
                )
            elif (
                existing["protocol_version"] != _PRICE_RULE_VERSION
                or existing["activated_at"] != frozen
            ):
                raise ValueError("price rule protocol was already activated differently")
            else:
                self._require_price_rule_activation(connection, "__price_rule_schema_probe__")
        return datetime.fromisoformat(frozen)

    def price_alert_rules_activated_at(self) -> datetime | None:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT protocol_version, activated_at FROM page_control_protocol_activation "
                "WHERE marker_name = ?",
                (_PRICE_RULE_MARKER,),
            ).fetchone()
        if row is None:
            return None
        if row["protocol_version"] != _PRICE_RULE_VERSION:
            raise RuntimeError("price rule protocol version is unsupported")
        return datetime.fromisoformat(row["activated_at"])

    @staticmethod
    def _require_condition_rule_activation(connection: sqlite3.Connection, owner_id: str) -> None:
        from rquant.condition_alert_rule_store import ConditionAlertRuleRepository

        marker = connection.execute(
            "SELECT protocol_version,activated_at FROM page_control_protocol_activation WHERE marker_name='condition-alert-rule/v1'"
        ).fetchone()
        if (
            marker is None
            or marker[0] != 1
            or not isinstance(marker[1], str)
            or _normalize_utc(datetime.fromisoformat(marker[1])).isoformat(timespec="microseconds")
            != marker[1]
        ):
            raise ValueError("condition rule protocol is not activated")
        ConditionAlertRuleRepository(connection).list_current(owner_id)

    def activate_condition_alert_rules(self, activated_at: datetime) -> datetime:
        from rquant.condition_alert_rule_store import ConditionAlertRuleRepository

        frozen = _normalize_utc(activated_at).isoformat(timespec="microseconds")
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            marker = connection.execute(
                "SELECT protocol_version,activated_at FROM page_control_protocol_activation WHERE marker_name='condition-alert-rule/v1'"
            ).fetchone()
            if marker is None:
                ConditionAlertRuleRepository(connection).install_schema()
                connection.execute(
                    "INSERT INTO page_control_protocol_activation(marker_name,protocol_version,activated_at) VALUES('condition-alert-rule/v1',1,?)",
                    (frozen,),
                )
            elif marker[0] != 1 or marker[1] != frozen:
                raise ValueError("condition rule protocol was activated differently")
            self._require_condition_rule_activation(connection, "__condition_probe__")
        return datetime.fromisoformat(frozen)

    def lookup_condition_rule_command(
        self, command: _OwnedConditionAlertRuleValue
    ) -> PageControlReceipt | None:
        if type(command) not in _CONDITION_OWNED_TYPES:
            raise TypeError("condition lookup requires exact owned request")
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM page_control_command WHERE command_id=?", (command.command_id,)
            ).fetchone()
        if row is None:
            return None
        stored = _COMMAND_ADAPTER.validate_json(row["payload_json"])
        if type(stored) not in _CONDITION_OWNED_TYPES or stored.owner_id != command.owner_id:
            return None
        if (
            stored != command
            or row["command_kind"] != command.kind
            or row["command_hash"] != _command_hash(command)
        ):
            raise PageControlCommandConflictError("condition original request conflicts")
        return self._receipt(row)

    def _condition_rule_failpoint(self, point: str) -> None:
        return None

    def complete_condition_rule(
        self,
        claim: PageControlClaim,
        *,
        now: datetime,
        resolve_scope: ConditionRuleScopeResolver | None,
    ) -> PageControlReceipt:
        from rquant.web.condition_alert_commands import complete_condition_rule

        return complete_condition_rule(self, claim, now=now, resolve_scope=resolve_scope)

    def activate_manual_watchlist(self, activated_at: datetime) -> datetime:
        frozen = _normalize_utc(activated_at).isoformat(timespec="microseconds")
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            existing = connection.execute(
                "SELECT protocol_version, activated_at FROM page_control_protocol_activation "
                "WHERE marker_name = ?",
                (_MANUAL_WATCHLIST_MARKER,),
            ).fetchone()
            if existing is None:
                ManualWatchlistRepository(connection).install_schema()
                connection.execute(
                    "INSERT INTO page_control_protocol_activation "
                    "(marker_name, protocol_version, activated_at) VALUES (?, ?, ?)",
                    (_MANUAL_WATCHLIST_MARKER, _MANUAL_WATCHLIST_VERSION, frozen),
                )
            elif (
                existing["protocol_version"] != _MANUAL_WATCHLIST_VERSION
                or existing["activated_at"] != frozen
            ):
                raise ValueError("manual watchlist was already activated differently")
            elif (
                connection.execute(
                    "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'manual_watchlist'"
                ).fetchone()
                is None
            ):
                raise RuntimeError("manual watchlist activation exists without its state table")
        return datetime.fromisoformat(frozen)

    def manual_watchlist_activated_at(self) -> datetime | None:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT protocol_version, activated_at FROM page_control_protocol_activation "
                "WHERE marker_name = ?",
                (_MANUAL_WATCHLIST_MARKER,),
            ).fetchone()
        if row is None:
            return None
        if row["protocol_version"] != _MANUAL_WATCHLIST_VERSION:
            raise RuntimeError("manual watchlist activation version is unsupported")
        return datetime.fromisoformat(row["activated_at"])

    def activate_alert_ack(self, activated_at: datetime) -> datetime:
        frozen = _normalize_utc(activated_at).isoformat(timespec="microseconds")
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            existing = connection.execute(
                "SELECT activated_at FROM page_control_alert_activation "
                "WHERE marker_name = 'alert_ack'"
            ).fetchone()
            if existing is None:
                connection.execute(
                    "INSERT INTO page_control_alert_activation(marker_name, activated_at) "
                    "VALUES ('alert_ack', ?)",
                    (frozen,),
                )
            elif existing["activated_at"] != frozen:
                raise ValueError("alert acknowledgment was already activated at another time")
        return datetime.fromisoformat(frozen)

    def alert_ack_activated_at(self) -> datetime | None:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT activated_at FROM page_control_alert_activation "
                "WHERE marker_name = 'alert_ack'"
            ).fetchone()
        return None if row is None else datetime.fromisoformat(row["activated_at"])

    def lookup_ack_command(self, command: AckAlert) -> PageControlReceipt | None:
        """Read-only exact retry lookup, safe across Serving generation changes."""
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM page_control_command WHERE command_id = ?",
                (command.command_id,),
            ).fetchone()
        if row is None:
            return None
        if row["command_kind"] != command.kind or row["command_hash"] != _command_hash(command):
            raise ValueError("command_id already exists with different payload")
        stored = _COMMAND_ADAPTER.validate_json(row["payload_json"])
        if not isinstance(stored, AckAlert) or _command_hash(stored) != row["command_hash"]:
            raise ValueError("stored acknowledgment command conflicts with its hash")
        return self._receipt(row)

    def lookup_price_rule_command(
        self, command: _OwnedPriceAlertRuleValue
    ) -> PageControlReceipt | None:
        """Read-only lookup of one exact persisted owner-bound rule request."""
        if not isinstance(
            command,
            (_OwnedSavePriceAlertRule, _OwnedSetPriceAlertRuleEnabled, _OwnedDeletePriceAlertRule),
        ):
            raise TypeError("price rule lookup requires an owned command")
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM page_control_command WHERE command_id = ?",
                (command.command_id,),
            ).fetchone()
        if row is None:
            return None
        if row["command_kind"] != command.kind or row["command_hash"] != _command_hash(command):
            raise PageControlCommandConflictError(
                "command_id already exists with different payload"
            )
        stored = _COMMAND_ADAPTER.validate_json(row["payload_json"])
        if stored != command or _command_hash(stored) != row["command_hash"]:
            raise PageControlCommandConflictError(
                "stored price rule command conflicts with its hash"
            )
        return self._receipt(row)

    def lookup_factor_archive_command(
        self, command: ArchiveFactor, *, authenticated_actor_id: str
    ) -> tuple[_OwnedArchiveFactor, PageControlReceipt] | None:
        """Match the original ownerless request and actor without consulting a new registry."""
        if type(command) is not ArchiveFactor:
            raise TypeError("factor archive lookup requires an ownerless archive")
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM page_control_command WHERE command_id = ?",
                (command.command_id,),
            ).fetchone()
        if row is None:
            return None
        try:
            stored = _COMMAND_ADAPTER.validate_json(row["payload_json"])
        except ValueError as exc:
            raise PageControlCommandConflictError("stored archive command is invalid") from exc
        if (
            not isinstance(stored, _OwnedArchiveFactor)
            or row["command_kind"] != command.kind
            or stored.actor_id != authenticated_actor_id
            or stored.model_dump(exclude={"actor_id", "registry_identity"}) != command.model_dump()
            or _command_hash(stored) != row["command_hash"]
        ):
            raise PageControlCommandConflictError(
                "command_id already exists with different payload or actor"
            )
        return stored, self._receipt(row)

    def lookup_strategy_authoring_command(
        self, request: StrategyTemplateCommand, *, authenticated_actor_id: str
    ) -> tuple[OwnedStrategyTemplateCommand, PageControlReceipt] | None:
        if type(request) not in (
            SaveStrategyTemplate,
            ArchiveStrategyTemplate,
            RunStrategyTemplate,
        ):
            raise TypeError("strategy lookup requires an ownerless original request")
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM page_control_command WHERE command_id = ?", (request.command_id,)
            ).fetchone()
        if row is None:
            return None
        try:
            stored = _COMMAND_ADAPTER.validate_json(row["payload_json"])
        except ValueError as exc:
            raise PageControlCommandConflictError("stored strategy command is invalid") from exc
        if (
            not isinstance(
                stored,
                (OwnedSaveStrategyTemplate, OwnedArchiveStrategyTemplate, OwnedRunStrategyTemplate),
            )
            or stored.owner_id != authenticated_actor_id
            or row["command_kind"] != request.kind
            or stored.original() != request
            or _command_hash(stored) != row["command_hash"]
        ):
            raise PageControlCommandConflictError(
                "command_id already exists with different payload or actor"
            )
        return stored, self._receipt(row)

    def lookup_strategy_promotion_command(self, request: StrategyPromotionCommand, *,
            authenticated_actor_id: str) -> tuple[OwnedStrategyPromotionCommand, PageControlReceipt] | None:
        if type(request) not in PROMOTION_PUBLIC_TYPES:
            raise TypeError("original ownerless promotion required")
        with self._connect() as connection:
            row = connection.execute("SELECT * FROM page_control_command WHERE command_id=?", (request.command_id,)).fetchone()
        if row is None:
            return None
        stored = _COMMAND_ADAPTER.validate_json(row["payload_json"])
        if (type(stored) not in PROMOTION_OWNED_TYPES or stored.owner_id != authenticated_actor_id
                or stored.original() != request or row["command_kind"] != request.kind
                or _command_hash(stored) != row["command_hash"]):
            raise PageControlCommandConflictError("original promotion UUID body or actor differs")
        proof = self._row_authorization(row)
        if proof is None or proof.actor_id != authenticated_actor_id:
            raise PermissionError("original promotion private actor is unproved")
        return stored, self._receipt(row)

    def lookup_factor_save_command(
        self, draft: FactorSaveDraft, *, authenticated_actor_id: str
    ) -> tuple[_OwnedSaveFactorDefinition, PageControlReceipt] | None:
        """Find only a save with this actor and complete original browser request."""
        checked = FactorSaveDraft.model_validate(draft)
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM page_control_command WHERE command_id = ?",
                (checked.command_id,),
            ).fetchone()
        if row is None:
            return None
        try:
            stored = _COMMAND_ADAPTER.validate_json(row["payload_json"])
        except ValueError as exc:
            raise PageControlCommandConflictError("stored factor save command is invalid") from exc
        if (
            not isinstance(stored, _OwnedSaveFactorDefinition)
            or row["command_kind"] != stored.kind
            or stored.registry_identity is None
            or stored.original_request_sha256 != draft_sha256(checked)
            or stored.original_generation_id != checked.generation_id
            or stored.actor_id != authenticated_actor_id
            or stored.command_id != checked.command_id
            or stored.requested_at != checked.requested_at
            or _command_hash(stored) != row["command_hash"]
        ):
            raise PageControlCommandConflictError(
                "command_id already exists with different payload or actor"
            )
        return stored, self._receipt(row)

    def lookup_factor_run_command(
        self,
        request: FactorRunRequest,
        *,
        authenticated_actor_id: str,
    ) -> tuple[_OwnedSubmitFactorRun, PageControlReceipt] | None:
        checked = FactorRunRequest.model_validate(request)
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM page_control_command WHERE command_id = ?", (checked.command_id,)
            ).fetchone()
        if row is None:
            return None
        try:
            stored = _COMMAND_ADAPTER.validate_json(row["payload_json"])
        except ValueError as exc:
            raise PageControlCommandConflictError("stored factor run is invalid") from exc
        if (
            type(stored) is not _OwnedSubmitFactorRun
            or stored.actor_id != authenticated_actor_id
            or stored.request != checked
            or stored.original_request_sha256 != checked.request_sha256
            or row["command_kind"] != stored.kind
            or row["command_hash"] != _command_hash(stored)
        ):
            raise PageControlCommandConflictError("command ID has different parameters or actor")
        return stored, self._receipt(row)

    def lookup_factor_tracking_command(
        self, request: FactorTrackingRequest, *, authenticated_actor_id: str
    ) -> tuple[_OwnedSetFactorTracked, PageControlReceipt] | None:
        checked = FactorTrackingRequest.model_validate(request)
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM page_control_command WHERE command_id=?", (checked.command_id,)
            ).fetchone()
        if row is None:
            return None
        try:
            stored = _COMMAND_ADAPTER.validate_json(row["payload_json"])
        except ValueError as exc:
            raise PageControlCommandConflictError("stored tracking command is invalid") from exc
        if (
            type(stored) is not _OwnedSetFactorTracked
            or stored.actor_id != authenticated_actor_id
            or stored.request != checked
            or row["command_kind"] != stored.kind
            or row["command_hash"] != _command_hash(stored)
        ):
            raise PageControlCommandConflictError(
                "tracking command has different parameters or actor"
            )
        return stored, self._receipt(row)

    def acknowledgment(self, alert_id: str) -> AlertAcknowledgment | None:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM page_control_alert_ack WHERE alert_id = ?",
                (alert_id,),
            ).fetchone()
        return None if row is None else self._acknowledgment(row)

    def complete_ack(self, claim: PageControlClaim) -> PageControlReceipt:
        """Commit first confirmation, effect, and terminal receipt in one SQLite transaction."""
        command = claim.command
        if not isinstance(command, AckAlert):
            raise TypeError("complete_ack requires an ack_alert claim")
        command_hash = _command_hash(command)
        confirmed_at = datetime.now(UTC).isoformat(timespec="microseconds")
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT * FROM page_control_command WHERE command_id = ?",
                (command.command_id,),
            ).fetchone()
            if (
                row is None
                or row["command_kind"] != command.kind
                or row["command_hash"] != command_hash
            ):
                raise ValueError("acknowledgment command content changed")
            if (
                row["status"] != PageControlStatus.PROCESSING.value
                or row["processing_owner"] != claim.owner_id
                or row["claim_token"] != claim.claim_token
            ):
                raise RuntimeError("stale acknowledgment claim cannot complete")
            if (
                connection.execute(
                    "SELECT 1 FROM page_control_alert_activation WHERE marker_name = 'alert_ack'"
                ).fetchone()
                is None
            ):
                raise ValueError("alert acknowledgment is not activated")
            connection.execute(
                """
                INSERT OR IGNORE INTO page_control_alert_ack(
                    alert_id, confirmation_id, actor_id, confirmed_at, generation_id
                ) VALUES (?, ?, ?, ?, ?)
                """,
                (
                    command.alert_id,
                    command.command_id,
                    command.actor_id,
                    confirmed_at,
                    command.generation_id,
                ),
            )
            acknowledgment = connection.execute(
                "SELECT * FROM page_control_alert_ack WHERE alert_id = ?",
                (command.alert_id,),
            ).fetchone()
            assert acknowledgment is not None
            result: JsonValue = {
                "alert_id": command.alert_id,
                "confirmation_id": acknowledgment["confirmation_id"],
                "confirmed_by": acknowledgment["actor_id"],
                "confirmed_at": acknowledgment["confirmed_at"],
            }
            result_json = json.dumps(result, ensure_ascii=True)
            connection.execute(
                """
                INSERT INTO page_control_effect(
                    command_id, command_hash, effect_kind, status,
                    owner_id, claim_token, started_at, completed_at, result_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    command.command_id,
                    command_hash,
                    command.kind,
                    PageControlEffectStatus.SUCCEEDED.value,
                    claim.owner_id,
                    claim.claim_token,
                    confirmed_at,
                    confirmed_at,
                    result_json,
                ),
            )
            changed = connection.execute(
                """
                UPDATE page_control_command
                SET status = ?, completed_at = ?, result_json = ?, error = NULL,
                    processing_owner = NULL, lease_expires_at = NULL, claim_token = NULL
                WHERE command_id = ? AND status = ? AND processing_owner = ? AND claim_token = ?
                """,
                (
                    PageControlStatus.SUCCEEDED.value,
                    confirmed_at,
                    result_json,
                    command.command_id,
                    PageControlStatus.PROCESSING.value,
                    claim.owner_id,
                    claim.claim_token,
                ),
            ).rowcount
            if changed != 1:
                raise RuntimeError("acknowledgment claim changed during completion")
            completed = connection.execute(
                "SELECT * FROM page_control_command WHERE command_id = ?",
                (command.command_id,),
            ).fetchone()
            assert completed is not None
        return self._receipt(completed)

    def complete_watchlist(self, claim: PageControlClaim, *, now: datetime) -> PageControlReceipt:
        """Commit a watchlist CAS, effect, and terminal receipt in one transaction."""
        command = claim.command
        if not isinstance(command, (AddWatchlistItem, RemoveWatchlistItem)):
            raise TypeError("complete_watchlist requires a watchlist claim")
        observed = _normalize_utc(now)
        completed_at = observed.isoformat(timespec="microseconds")
        command_hash = _command_hash(command)
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT * FROM page_control_command WHERE command_id = ?",
                (command.command_id,),
            ).fetchone()
            if (
                row is None
                or row["command_kind"] != command.kind
                or row["command_hash"] != command_hash
            ):
                raise ValueError("watchlist command content changed")
            stored = _COMMAND_ADAPTER.validate_json(row["payload_json"])
            if stored != command or _command_hash(stored) != command_hash:
                raise ValueError("stored watchlist command conflicts with its hash")
            if (
                row["status"] != PageControlStatus.PROCESSING.value
                or row["processing_owner"] != claim.owner_id
                or row["claim_token"] != claim.claim_token
                or row["lease_expires_at"] is None
                or row["lease_expires_at"] <= completed_at
            ):
                raise RuntimeError("stale or expired watchlist claim cannot complete")
            if not self._manual_watchlist_activated(connection):
                raise ValueError("manual watchlist is not activated")

            action: Literal["add", "remove"] = (
                "add" if isinstance(command, AddWatchlistItem) else "remove"
            )
            error: str | None = None
            status = PageControlStatus.SUCCEEDED
            if command.requested_at > observed + _MAX_REQUEST_FUTURE_SKEW:
                status = PageControlStatus.FAILED
                error = "watchlist command requested_at exceeds allowed future clock skew"
                result: JsonValue = {
                    "ts_code": command.item.ts_code,
                    "action": action,
                    "code": "future_request",
                }
            else:
                repository = ManualWatchlistRepository(connection)
                try:
                    entry = (
                        repository.upsert(command.item, now=observed)
                        if isinstance(command, AddWatchlistItem)
                        else repository.delete(command.item, now=observed)
                    )
                except WatchlistVersionConflictError:
                    status = PageControlStatus.FAILED
                    error = "watchlist version conflict"
                    result = {
                        "ts_code": command.item.ts_code,
                        "action": action,
                        "code": "version_conflict",
                    }
                except WatchlistCapacityError:
                    status = PageControlStatus.FAILED
                    error = "watchlist capacity exceeded"
                    result = {
                        "ts_code": command.item.ts_code,
                        "action": action,
                        "code": "capacity_exceeded",
                    }
                else:
                    result = {
                        "ts_code": entry.ts_code,
                        "action": action,
                        "version": entry.version,
                        "state": entry.status,
                    }
            result_json = json.dumps(result, ensure_ascii=True)
            connection.execute(
                """
                INSERT INTO page_control_effect(
                    command_id, command_hash, effect_kind, status,
                    owner_id, claim_token, started_at, completed_at, result_json, error
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    command.command_id,
                    command_hash,
                    command.kind,
                    (
                        PageControlEffectStatus.SUCCEEDED.value
                        if status is PageControlStatus.SUCCEEDED
                        else PageControlEffectStatus.FAILED.value
                    ),
                    claim.owner_id,
                    claim.claim_token,
                    completed_at,
                    completed_at,
                    result_json,
                    error,
                ),
            )
            changed = connection.execute(
                """
                UPDATE page_control_command
                SET status = ?, completed_at = ?, result_json = ?, error = ?,
                    processing_owner = NULL, lease_expires_at = NULL, claim_token = NULL
                WHERE command_id = ? AND status = ? AND processing_owner = ?
                  AND claim_token = ? AND lease_expires_at > ?
                """,
                (
                    status.value,
                    completed_at,
                    result_json,
                    error,
                    command.command_id,
                    PageControlStatus.PROCESSING.value,
                    claim.owner_id,
                    claim.claim_token,
                    completed_at,
                ),
            ).rowcount
            if changed != 1:
                raise RuntimeError("watchlist claim changed during completion")
            completed = connection.execute(
                "SELECT * FROM page_control_command WHERE command_id = ?",
                (command.command_id,),
            ).fetchone()
            assert completed is not None
        return self._receipt(completed)

    def complete_price_rule(self, claim: PageControlClaim, *, now: datetime) -> PageControlReceipt:
        """Commit rule CAS, effect, and terminal receipt in one SQLite transaction."""
        command = claim.command
        if not isinstance(
            command,
            (_OwnedSavePriceAlertRule, _OwnedSetPriceAlertRuleEnabled, _OwnedDeletePriceAlertRule),
        ):
            raise TypeError("complete_price_rule requires an owned price rule claim")
        observed = _normalize_utc(now)
        completed_at = observed.isoformat(timespec="microseconds")
        command_hash = _command_hash(command)
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT * FROM page_control_command WHERE command_id = ?",
                (command.command_id,),
            ).fetchone()
            if (
                row is None
                or row["command_kind"] != command.kind
                or row["command_hash"] != command_hash
            ):
                raise ValueError("price rule command content changed")
            stored = _COMMAND_ADAPTER.validate_json(row["payload_json"])
            if stored != command or _command_hash(stored) != command_hash:
                raise ValueError("stored price rule command conflicts with its hash")
            if (
                row["status"] != PageControlStatus.PROCESSING.value
                or row["processing_owner"] != claim.owner_id
                or row["claim_token"] != claim.claim_token
                or row["lease_expires_at"] is None
                or row["lease_expires_at"] <= completed_at
            ):
                raise RuntimeError("stale or expired price rule claim cannot complete")
            self._require_price_rule_activation(connection, command.owner_id)

            action = (
                "save"
                if isinstance(command, _OwnedSavePriceAlertRule)
                else "set_enabled"
                if isinstance(command, _OwnedSetPriceAlertRuleEnabled)
                else "delete"
            )
            rule_id = (
                command.rule.rule_id
                if isinstance(command, _OwnedSavePriceAlertRule)
                else command.rule_id
            )
            error: str | None = None
            status = PageControlStatus.SUCCEEDED
            if command.requested_at > observed + _MAX_REQUEST_FUTURE_SKEW:
                status = PageControlStatus.FAILED
                error = "price rule command requested_at exceeds allowed future clock skew"
                result: JsonValue = {"rule_id": rule_id, "action": action, "code": "future_request"}
            else:
                repository = PriceAlertRuleRepository(connection)
                try:
                    if isinstance(command, _OwnedSavePriceAlertRule):
                        entry = repository.upsert(
                            PriceAlertRuleUpsert(
                                owner_id=command.owner_id,
                                ts_code=command.ts_code,
                                membership_version=command.membership_version,
                                expected_version=command.expected_version,
                                rule=command.rule,
                            ),
                            now=observed,
                        )
                    elif isinstance(command, _OwnedSetPriceAlertRuleEnabled):
                        current = repository.get(
                            PriceAlertRuleKey(owner_id=command.owner_id, rule_id=command.rule_id)
                        )
                        if (
                            current is None
                            or current.deleted
                            or current.version != command.expected_version
                        ):
                            raise PriceAlertRuleVersionConflictError(
                                "price rule version does not match"
                            )
                        assert current.rule is not None
                        assert current.ts_code is not None
                        assert current.membership_version is not None
                        entry = repository.upsert(
                            PriceAlertRuleUpsert(
                                owner_id=command.owner_id,
                                ts_code=current.ts_code,
                                membership_version=current.membership_version,
                                expected_version=command.expected_version,
                                rule=current.rule.model_copy(update={"enabled": command.enabled}),
                            ),
                            now=observed,
                        )
                    else:
                        entry = repository.delete(
                            PriceAlertRuleDelete(
                                owner_id=command.owner_id,
                                rule_id=command.rule_id,
                                expected_version=command.expected_version,
                            ),
                            now=observed,
                        )
                except PriceAlertRuleVersionConflictError:
                    status = PageControlStatus.FAILED
                    error = "price rule version conflict"
                    result = {"rule_id": rule_id, "action": action, "code": "version_conflict"}
                except PriceAlertRuleScopeError:
                    status = PageControlStatus.FAILED
                    error = "price rule scope is invalid"
                    result = {"rule_id": rule_id, "action": action, "code": "scope_invalid"}
                except PriceAlertRuleCapacityError:
                    status = PageControlStatus.FAILED
                    error = "price rule capacity exceeded"
                    result = {"rule_id": rule_id, "action": action, "code": "capacity_exceeded"}
                else:
                    result = {
                        "rule_id": entry.rule_id,
                        "action": action,
                        "version": entry.version,
                        "deleted": entry.deleted,
                        "enabled": None if entry.rule is None else entry.rule.enabled,
                    }
            result_json = json.dumps(result, ensure_ascii=True)
            connection.execute(
                """
                INSERT INTO page_control_effect(
                    command_id, command_hash, effect_kind, status,
                    owner_id, claim_token, started_at, completed_at, result_json, error
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    command.command_id,
                    command_hash,
                    command.kind,
                    (
                        PageControlEffectStatus.SUCCEEDED.value
                        if status is PageControlStatus.SUCCEEDED
                        else PageControlEffectStatus.FAILED.value
                    ),
                    claim.owner_id,
                    claim.claim_token,
                    completed_at,
                    completed_at,
                    result_json,
                    error,
                ),
            )
            changed = connection.execute(
                """
                UPDATE page_control_command
                SET status = ?, completed_at = ?, result_json = ?, error = ?,
                    processing_owner = NULL, lease_expires_at = NULL, claim_token = NULL
                WHERE command_id = ? AND status = ? AND processing_owner = ?
                  AND claim_token = ? AND lease_expires_at > ?
                """,
                (
                    status.value,
                    completed_at,
                    result_json,
                    error,
                    command.command_id,
                    PageControlStatus.PROCESSING.value,
                    claim.owner_id,
                    claim.claim_token,
                    completed_at,
                ),
            ).rowcount
            if changed != 1:
                raise RuntimeError("price rule claim changed during completion")
            completed = connection.execute(
                "SELECT * FROM page_control_command WHERE command_id = ?",
                (command.command_id,),
            ).fetchone()
            assert completed is not None
        return self._receipt(completed)

    @staticmethod
    def _acknowledgment(row: sqlite3.Row) -> AlertAcknowledgment:
        return AlertAcknowledgment(
            alert_id=row["alert_id"],
            confirmation_id=row["confirmation_id"],
            actor_id=row["actor_id"],
            confirmed_at=datetime.fromisoformat(row["confirmed_at"]),
            generation_id=row["generation_id"],
        )

    def claim(
        self,
        *,
        limit: int,
        owner_id: str = "page-control-consumer",
        lease_seconds: int = _DEFAULT_LEASE_SECONDS,
        now: datetime | None = None,
    ) -> tuple[PageControlCommandValue, ...]:
        return tuple(
            record.command
            for record in self.claim_records(
                limit=limit,
                owner_id=owner_id,
                lease_seconds=lease_seconds,
                now=now,
            )
        )

    def claim_records(
        self,
        *,
        limit: int,
        owner_id: str = "page-control-consumer",
        lease_seconds: int = _DEFAULT_LEASE_SECONDS,
        now: datetime | None = None,
        target_command_id: str | None = None,
        target_command_kinds: tuple[str, ...] | None = None,
    ) -> tuple[PageControlClaim, ...]:
        if not 1 <= limit <= 100:
            raise ValueError("limit must be between 1 and 100")
        if not owner_id:
            raise ValueError("owner_id is required")
        if lease_seconds < 1:
            raise ValueError("lease_seconds must be positive")
        if target_command_kinds is not None and (not target_command_kinds
                or len(set(target_command_kinds)) != len(target_command_kinds)
                or not set(target_command_kinds) <= {
                    "submit_minute_replay", "export_minute_replay_zip", "submit_minute_parameter_study"}):
            raise ValueError("minute consumer kinds differ from the original closed commands")
        observed = _normalize_utc(now or datetime.now(UTC))
        observed_at = observed.isoformat(timespec="microseconds")
        lease_expires_at = (observed + timedelta(seconds=lease_seconds)).isoformat(
            timespec="microseconds"
        )
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            kind_filter = "" if target_command_kinds is None else (
                " AND command_kind IN (" + ",".join("?" for _ in target_command_kinds) + ") "
            )
            rows = connection.execute(
                """
                SELECT command_id, command_kind, command_hash, payload_json
                FROM page_control_command
                WHERE (status = ?
                   OR (status = ? AND lease_expires_at IS NOT NULL AND lease_expires_at <= ?))
                  AND (? IS NULL OR command_id = ?)
                """ + kind_filter + " ORDER BY CASE status WHEN ? THEN 0 ELSE 1 END, enqueued_at, rowid",
                (
                    PageControlStatus.PENDING.value,
                    PageControlStatus.PROCESSING.value,
                    observed_at,
                    target_command_id,
                    target_command_id,
                    *(target_command_kinds or ()),
                    PageControlStatus.PENDING.value,
                ),
            )
            eligible: list[sqlite3.Row] = []
            try:
                for row in rows:
                    try:
                        parsed_command = _COMMAND_ADAPTER.validate_json(row["payload_json"])
                    except ValueError:
                        if (
                            row["command_kind"]
                            in _CONDITION_RULE_KINDS
                            | _PRICE_RULE_KINDS
                            | _FACTOR_DEFINITION_KINDS
                            | _FACTOR_RUN_KINDS
                        ):
                            continue
                        raise
                    if isinstance(
                        parsed_command,
                        (
                            _OwnedSavePriceAlertRule,
                            _OwnedSetPriceAlertRuleEnabled,
                            _OwnedDeletePriceAlertRule,
                        ),
                    ):
                        try:
                            if (
                                parsed_command.kind != row["command_kind"]
                                or parsed_command.command_id != row["command_id"]
                                or _command_hash(parsed_command) != row["command_hash"]
                            ):
                                continue
                            self._require_price_rule_activation(connection, parsed_command.owner_id)
                        except (ValueError, RuntimeError, sqlite3.Error):
                            continue
                    elif row["command_kind"] in _PRICE_RULE_KINDS:
                        continue
                    elif type(parsed_command) in _CONDITION_OWNED_TYPES:
                        try:
                            if (
                                parsed_command.kind != row["command_kind"]
                                or _command_hash(parsed_command) != row["command_hash"]
                            ):
                                continue
                            self._require_condition_rule_activation(
                                connection, parsed_command.owner_id
                            )
                        except (ValueError, RuntimeError, sqlite3.Error):
                            continue
                    elif row["command_kind"] in _CONDITION_RULE_KINDS:
                        continue
                    elif isinstance(
                        parsed_command,
                        (
                            _OwnedSaveFactorDefinition,
                            _OwnedArchiveFactor,
                            _OwnedSubmitFactorRun,
                            _OwnedSetFactorTracked,
                        ),
                    ):
                        if (
                            parsed_command.kind != row["command_kind"]
                            or parsed_command.command_id != row["command_id"]
                            or _command_hash(parsed_command) != row["command_hash"]
                        ):
                            continue
                    elif (
                        row["command_kind"]
                        in _FACTOR_DEFINITION_KINDS | _FACTOR_RUN_KINDS | _FACTOR_TRACKING_KINDS
                    ):
                        continue
                    eligible.append(row)
                    if len(eligible) == limit:
                        break
            finally:
                rows.close()
            claims: list[PageControlClaim] = []
            for row in eligible:
                claim_token = uuid4().hex
                changed = connection.execute(
                    """
                    UPDATE page_control_command
                    SET status = ?, processing_owner = ?, lease_expires_at = ?,
                        attempt_count = COALESCE(attempt_count, 0) + 1,
                        claim_token = ?
                    WHERE command_id = ?
                      AND (
                        status = ?
                        OR (
                            status = ? AND lease_expires_at IS NOT NULL
                            AND lease_expires_at <= ?
                        )
                      )
                    """,
                    (
                        PageControlStatus.PROCESSING.value,
                        owner_id,
                        lease_expires_at,
                        claim_token,
                        row["command_id"],
                        PageControlStatus.PENDING.value,
                        PageControlStatus.PROCESSING.value,
                        observed_at,
                    ),
                ).rowcount
                if changed == 1:
                    claims.append(
                        PageControlClaim(
                            command=_COMMAND_ADAPTER.validate_json(row["payload_json"]),
                            owner_id=owner_id,
                            claim_token=claim_token,
                        )
                    )
            connection.commit()
        return tuple(claims)

    def complete(
        self,
        command_id: str,
        *,
        result: JsonValue | None = None,
        error: str | None = None,
        status: PageControlStatus | None = None,
        owner_id: str | None = None,
        claim_token: str | None = None,
    ) -> PageControlReceipt:
        status = status or (
            PageControlStatus.FAILED if error is not None else PageControlStatus.SUCCEEDED
        )
        if status not in _PAGE_CONTROL_TERMINAL_STATUSES:
            raise ValueError("command completion status must be terminal")
        completed_at = datetime.now(UTC).isoformat(timespec="microseconds")
        owner_predicate = ""
        owner_values: tuple[str, ...] = ()
        if owner_id is not None or claim_token is not None:
            if owner_id is None or claim_token is None:
                raise ValueError("owner_id and claim_token must be provided together")
            owner_predicate = " AND processing_owner = ? AND claim_token = ?"
            owner_values = (owner_id, claim_token)
        with self._connect() as connection:
            kind = connection.execute(
                "SELECT command_kind FROM page_control_command WHERE command_id = ?",
                (command_id,),
            ).fetchone()
            if kind is not None and kind["command_kind"] in _SCREEN_QUERY_PRIVATE_KINDS:
                raise ValueError("screen query requires atomic completion")
            if kind is not None and kind["command_kind"] in _MANUAL_WATCHLIST_KINDS:
                raise ValueError("watchlist command requires atomic completion")
            if kind is not None and kind["command_kind"] in _PRICE_RULE_KINDS:
                raise ValueError("price rule command requires atomic completion")
            if kind is not None and kind["command_kind"] in _CONDITION_RULE_KINDS:
                raise ValueError("condition rule command requires atomic completion")
            changed = connection.execute(
                f"""
                UPDATE page_control_command
                SET status = ?, completed_at = ?, result_json = ?, error = ?,
                    processing_owner = NULL, lease_expires_at = NULL, claim_token = NULL
                WHERE command_id = ? AND status = ?{owner_predicate}
                """,
                (
                    status.value,
                    completed_at,
                    None if result is None else json.dumps(result, ensure_ascii=True),
                    error,
                    command_id,
                    PageControlStatus.PROCESSING.value,
                    *owner_values,
                ),
            ).rowcount
            if changed != 1:
                row = connection.execute(
                    "SELECT * FROM page_control_command WHERE command_id = ?",
                    (command_id,),
                ).fetchone()
                if row is not None and PageControlStatus(row["status"]) in (
                    _PAGE_CONTROL_TERMINAL_STATUSES
                ):
                    return self._receipt(row)
                raise RuntimeError("stale page control claim cannot complete command")
        receipt = self.receipt(command_id)
        assert receipt is not None
        return receipt

    def release_claim_for_retry(
        self,
        command_id: str,
        *,
        owner_id: str,
        claim_token: str,
    ) -> PageControlReceipt:
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            changed = connection.execute(
                """
                UPDATE page_control_command
                SET status = ?, completed_at = NULL, result_json = NULL, error = NULL,
                    processing_owner = NULL, lease_expires_at = NULL, claim_token = NULL
                WHERE command_id = ? AND status = ?
                  AND processing_owner = ? AND claim_token = ?
                  AND EXISTS (
                    SELECT 1 FROM page_control_effect AS effect
                    WHERE effect.command_id = page_control_command.command_id
                      AND effect.status = ?
                      AND effect.owner_id = ? AND effect.claim_token = ?
                  )
                """,
                (
                    PageControlStatus.PENDING.value,
                    command_id,
                    PageControlStatus.PROCESSING.value,
                    owner_id,
                    claim_token,
                    PageControlEffectStatus.STARTED.value,
                    owner_id,
                    claim_token,
                ),
            ).rowcount
            if changed != 1:
                connection.rollback()
                raise RuntimeError("stale page control claim cannot be released for retry")
            connection.commit()
        receipt = self.receipt(command_id)
        assert receipt is not None
        return receipt

    def begin_effect(
        self,
        command: PageControlCommandValue,
        *,
        owner_id: str,
        claim_token: str,
        now: datetime | None = None,
    ) -> tuple[PageControlEffectRecord, bool]:
        if isinstance(command, (AddWatchlistItem, RemoveWatchlistItem)):
            raise ValueError("watchlist command requires atomic completion")
        if isinstance(
            command, (SavePriceAlertRule, SetPriceAlertRuleEnabled, DeletePriceAlertRule)
        ):
            raise ValueError("price rule command requires atomic completion")
        command_hash = _command_hash(command)
        observed_at = _normalize_utc(now or datetime.now(UTC)).isoformat(timespec="microseconds")
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                """
                SELECT command_hash, status, processing_owner, claim_token
                FROM page_control_command WHERE command_id = ?
                """,
                (command.command_id,),
            ).fetchone()
            if row is None:
                raise RuntimeError("page control command disappeared")
            if row["command_hash"] != command_hash:
                raise ValueError("command payload hash changed")
            if (
                PageControlStatus(row["status"]) != PageControlStatus.PROCESSING
                or row["processing_owner"] != owner_id
                or row["claim_token"] != claim_token
            ):
                raise RuntimeError("stale page control claim cannot start effect")
            existing = connection.execute(
                "SELECT * FROM page_control_effect WHERE command_id = ?",
                (command.command_id,),
            ).fetchone()
            created = False
            if existing is None:
                connection.execute(
                    """
                    INSERT INTO page_control_effect(
                        command_id, command_hash, effect_kind, status,
                        owner_id, claim_token, started_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        command.command_id,
                        command_hash,
                        command.kind,
                        PageControlEffectStatus.STARTED.value,
                        owner_id,
                        claim_token,
                        observed_at,
                    ),
                )
                created = True
            else:
                if existing["command_hash"] != command_hash:
                    raise ValueError("effect command hash mismatch")
                if PageControlEffectStatus(existing["status"]) == PageControlEffectStatus.STARTED:
                    connection.execute(
                        """
                        UPDATE page_control_effect
                        SET owner_id = ?, claim_token = ?
                        WHERE command_id = ? AND status = ?
                        """,
                        (
                            owner_id,
                            claim_token,
                            command.command_id,
                            PageControlEffectStatus.STARTED.value,
                        ),
                    )
            connection.commit()
        effect = self.effect(command.command_id)
        assert effect is not None
        return effect, created

    def finish_effect(
        self,
        command_id: str,
        *,
        status: PageControlEffectStatus,
        result: JsonValue | None = None,
        error: str | None = None,
        owner_id: str,
        claim_token: str,
    ) -> PageControlEffectRecord:
        if status not in _PAGE_CONTROL_EFFECT_TERMINAL_STATUSES:
            raise ValueError("effect completion status must be terminal")
        completed_at = datetime.now(UTC).isoformat(timespec="microseconds")
        with self._connect() as connection:
            changed = connection.execute(
                """
                UPDATE page_control_effect
                SET status = ?, completed_at = ?, result_json = ?, error = ?
                WHERE command_id = ? AND status = ? AND owner_id = ? AND claim_token = ?
                """,
                (
                    status.value,
                    completed_at,
                    None if result is None else json.dumps(result, ensure_ascii=True),
                    error,
                    command_id,
                    PageControlEffectStatus.STARTED.value,
                    owner_id,
                    claim_token,
                ),
            ).rowcount
            if changed != 1:
                row = connection.execute(
                    "SELECT * FROM page_control_effect WHERE command_id = ?",
                    (command_id,),
                ).fetchone()
                if row is not None and PageControlEffectStatus(row["status"]) in (
                    _PAGE_CONTROL_EFFECT_TERMINAL_STATUSES
                ):
                    return self._effect_record(row)
                raise RuntimeError("stale page control claim cannot finish effect")
        effect = self.effect(command_id)
        assert effect is not None
        return effect

    def record_started_effect_result(
        self,
        command_id: str,
        *,
        result: JsonValue,
        owner_id: str,
        claim_token: str,
    ) -> PageControlEffectRecord:
        with self._connect() as connection:
            command_row = connection.execute("SELECT * FROM page_control_command WHERE command_id=?", (command_id,)).fetchone()
            if command_row is not None and self._row_authorization(command_row) is not None:
                # Preserve the actual original owner marker before terminal result
                # replaces result_json. This is not a synthesized job or new ledger.
                connection.execute("UPDATE page_control_effect SET original_admission_json=COALESCE(original_admission_json, ?) WHERE command_id=? AND status=? AND owner_id=? AND claim_token=?",
                    (json.dumps(result, ensure_ascii=True), command_id, PageControlEffectStatus.STARTED.value, owner_id, claim_token))
            changed = connection.execute(
                """
                UPDATE page_control_effect
                SET result_json = ?
                WHERE command_id = ? AND status = ? AND owner_id = ? AND claim_token = ?
                """,
                (
                    json.dumps(result, ensure_ascii=True),
                    command_id,
                    PageControlEffectStatus.STARTED.value,
                    owner_id,
                    claim_token,
                ),
            ).rowcount
            if changed != 1:
                raise RuntimeError("stale page control claim cannot update effect")
        effect = self.effect(command_id)
        assert effect is not None
        return effect

    def receipt(self, command_id: str) -> PageControlReceipt | None:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM page_control_command WHERE command_id = ?",
                (command_id,),
            ).fetchone()
        return None if row is None else self._receipt(row)

    def effect(self, command_id: str) -> PageControlEffectRecord | None:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM page_control_effect WHERE command_id = ?",
                (command_id,),
            ).fetchone()
        return None if row is None else self._effect_record(row)

    def audit(self, command_id: str) -> PageControlCommandAudit | None:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM page_control_command WHERE command_id = ?",
                (command_id,),
            ).fetchone()
        if row is None:
            return None
        return PageControlCommandAudit(
            command_id=row["command_id"],
            command_kind=row["command_kind"],
            command_hash=row["command_hash"],
            status=PageControlStatus(row["status"]),
            result=(None if row["result_json"] is None else json.loads(row["result_json"])),
        )

    def latest_succeeded_canvas_mutation(
        self,
        canvas_name: str,
    ) -> PageControlCommandValue | None:
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT command_kind, command_hash, payload_json
                FROM page_control_command
                WHERE status = ?
                  AND command_kind IN (?, ?, ?, ?, ?, ?, ?)
                ORDER BY rowid DESC
                """,
                (
                    PageControlStatus.SUCCEEDED.value,
                    "save_canvas",
                    "create_canvas",
                    "delete_canvas",
                    "set_canvas_pool_refs",
                    "add_pool_to_canvas",
                    "save_user_pool",
                    "fork_builtin_pool",
                ),
            ).fetchall()
        for row in rows:
            command = _COMMAND_ADAPTER.validate_json(row["payload_json"])
            if command.kind != row["command_kind"] or _command_hash(command) != row["command_hash"]:
                raise ValueError("PageControl canvas mutation authority is malformed")
            affected_canvas = (
                command.name
                if isinstance(command, (SaveCanvas, CreateCanvas, DeleteCanvas, SetCanvasPoolRefs))
                else command.canvas_name
            )
            if affected_canvas == canvas_name:
                return command
        return None

    @staticmethod
    def _receipt(row: sqlite3.Row) -> PageControlReceipt:
        return PageControlReceipt(
            command_id=row["command_id"],
            status=PageControlStatus(row["status"]),
            enqueued_at=datetime.fromisoformat(row["enqueued_at"]),
            completed_at=(
                None if row["completed_at"] is None else datetime.fromisoformat(row["completed_at"])
            ),
            result=(None if row["result_json"] is None else json.loads(row["result_json"])),
            error=row["error"],
        )

    @staticmethod
    def _effect_record(row: sqlite3.Row) -> PageControlEffectRecord:
        return PageControlEffectRecord(
            command_id=row["command_id"],
            command_hash=row["command_hash"],
            effect_kind=row["effect_kind"],
            status=PageControlEffectStatus(row["status"]),
            owner_id=row["owner_id"],
            claim_token=row["claim_token"],
            result=(None if row["result_json"] is None else json.loads(row["result_json"])),
            error=row["error"],
        )

    def enqueue_trusted_task_control(self, command: OwnedTaskControl) -> PageControlReceipt:
        if type(command) not in TASK_CONTROL_OWNED_TYPES:
            raise TypeError("task controls require exact owned commands")
        return self._enqueue(command, require_task_control_trust=True)

    def lookup_task_control_command(
        self, request: TaskControlRequest, *, authenticated_actor_id: str,
    ) -> tuple[OwnedTaskControl, PageControlReceipt] | None:
        if type(request) not in TASK_CONTROL_PUBLIC_TYPES:
            raise TypeError("task lookup requires an ownerless original request")
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM page_control_command WHERE command_id = ?", (request.command_id,),
            ).fetchone()
        if row is None:
            return None
        try:
            stored = _COMMAND_ADAPTER.validate_json(row["payload_json"])
        except ValueError as exc:
            raise PageControlCommandConflictError("stored task command is invalid") from exc
        if (
            type(stored) not in TASK_CONTROL_OWNED_TYPES
            or stored.owner_id != authenticated_actor_id
            or row["command_kind"] != request.kind
            or stored.original() != request
            or _command_hash(stored) != row["command_hash"]
        ):
            raise PageControlCommandConflictError("command_id already exists with different payload or actor")
        return stored, self._receipt(row)


class PageControlConsumer:
    """The only component permitted to mutate page-managed local artifacts."""

    def __init__(
        self,
        *,
        outbox: PageControlOutbox,
        data_dir: Path,
        log_dir: Path,
        allowed_lab_export_roots: tuple[Path, ...] = (),
        lab_backend: LabPageControlBackend | None = None,
        portfolio_backend: PortfolioPageControlBackend | None = None,
        minute_backend: MinutePageControlBackend | None = None,
        minute_study_backend: MinuteParameterStudyCommandWriter | None = None,
        minute_native_report_backend: MinuteNativeReportCommandWriter | None = None,
        experiment_backend: ExperimentPageControlBackend | None = None,
        backfill_plan_backend: BackfillPlanPageControlBackend | None = None,
        data_audit_report_backend: DataAuditReportPageControlBackend | None = None,
        data_center_execution_backend: DataCenterExecutionPageControlBackend | None = None,
        formula_market_backend: FormulaMarketPageControlBackend | None = None,
        formula_pool_backend: FormulaPoolPageControlBackend | None = None,
        factor_definition_backend: FactorDefinitionPageControlBackend | None = None,
        factor_run_backend: FactorRunPageControlBackend | None = None,
        factor_tracking_backend: FactorTrackingPageControlBackend | None = None,
        strategy_authoring_backend: StrategyAuthoringPageControlBackend | None = None,
        strategy_promotion_backend: StrategyPromotionPageControlBackend | None = None,
        paper_portfolio_backend: PaperPortfolioPageControlBackend | None = None,
        task_control_backend: TaskControlPageControlBackend | None = None,
        screen_query_history: ScreenQueryHistory | None = None,
        screen_query_executor: Callable[[ScreenQueryDefinition], ScreenRunData] | None = None,
        daily_writer_capability: Callable[[], DailyWriterCapability | None] | None = None,
        daily_run_evidence: Callable[[], tuple[PublishedDailyScreenEvidence, ...]] | None = None,
        condition_rule_scope: ConditionRuleScopeResolver | None = None,
        clock: Callable[[], datetime] | None = None,
        lease_seconds: int = _DEFAULT_LEASE_SECONDS,
        consumer_id: str | None = None,
        consumer_service_id: str = DEFAULT_PAGE_CONTROL_SERVICE_ID,
        canvas_publication_signer: CanvasPublicationSigner | None = None,
        canvas_publication_keyring: CanvasPublicationKeyring | None = None,
    ) -> None:
        self.outbox = outbox
        self.data_dir = Path(os.path.abspath(data_dir))
        self.log_dir = Path(os.path.abspath(log_dir))
        self.allowed_lab_export_roots = tuple(
            Path(os.path.abspath(path)) for path in allowed_lab_export_roots
        )
        self.lab_backend = lab_backend
        self.portfolio_backend = portfolio_backend
        self.minute_backend = minute_backend
        self.minute_study_backend = minute_study_backend
        self.minute_native_report_backend = minute_native_report_backend
        self.experiment_backend = experiment_backend
        self.backfill_plan_backend = backfill_plan_backend
        self.data_audit_report_backend = data_audit_report_backend
        self.data_center_execution_backend = data_center_execution_backend
        self.formula_market_backend = formula_market_backend
        self.formula_pool_backend = formula_pool_backend
        self.factor_definition_backend = factor_definition_backend
        self.factor_run_backend = factor_run_backend
        self.factor_tracking_backend = factor_tracking_backend
        self.strategy_authoring_backend = strategy_authoring_backend
        self.strategy_promotion_backend = strategy_promotion_backend
        self.paper_portfolio_backend = paper_portfolio_backend
        self.screen_query_history = screen_query_history
        self.screen_query_executor = screen_query_executor
        self.daily_writer_capability = daily_writer_capability
        self.daily_run_evidence = daily_run_evidence
        self.condition_rule_scope = condition_rule_scope
        if task_control_backend is not None:
            from rquant.task_control import TaskControlPageControlBackend

            if type(task_control_backend) is not TaskControlPageControlBackend or task_control_backend.journal.outbox is not outbox:
                raise TypeError("task controls require the same original concrete PageControl journal")
        self.task_control_backend = task_control_backend
        self.clock = clock or (lambda: datetime.now(UTC))
        self.lease_seconds = lease_seconds
        self.consumer_service_id = consumer_service_id
        self.consumer_id = consumer_id or _default_page_control_consumer_id(
            consumer_service_id=consumer_service_id,
            outbox_path=outbox.path,
        )
        self.canvas_publication_signer = canvas_publication_signer
        self.canvas_publication_keyring = canvas_publication_keyring

    def trusted_daily_writer_capability(self) -> DailyWriterCapability | None:
        from rquant.screen.daily_inputs import daily_writer_contract_fingerprint

        if self.daily_writer_capability is None:
            return None
        try:
            proof = self.daily_writer_capability()
            if (
                type(proof) is not DailyWriterCapability
                or proof.writer_contract_fingerprint != daily_writer_contract_fingerprint()
                or proof.completed_at > self.clock()
            ):
                return None
            return proof
        except (OSError, ValueError, RuntimeError):
            return None

    def drain(self, *, limit: int) -> tuple[PageControlReceipt, ...]:
        with _PageControlExecutionMutex(self._consumer_mutex_path()) as acquired:
            if not acquired:
                return ()
            return self._drain_locked(limit=limit)

    def drain_minute_commands(self, *, limit: int = 1) -> tuple[PageControlReceipt, ...]:
        if self.minute_backend is None and self.minute_native_report_backend is None:
            return ()
        with _PageControlExecutionMutex(self._consumer_mutex_path()) as acquired:
            if not acquired:
                return ()
            return self._complete_claims(self.outbox.claim_records(
                limit=limit, owner_id=self.consumer_id, lease_seconds=self.lease_seconds, now=self.clock(),
                target_command_kinds=(("export_minute_replay_zip",) if self.minute_backend is None else
                    ("submit_minute_replay", "export_minute_replay_zip", "submit_minute_parameter_study"))))

    def _selected_minute_backend(self, command: MinuteCommand) -> MinutePageControlBackend:
        native = self.minute_native_report_backend
        if type(command) is ExportMinuteReplayZip and native is not None:
            from rquant.minute_backtest_commands import MinuteCommandWriter
            from rquant.minute_backtest_native_report_runtime import MinuteNativeReportCommandWriter
            from rquant.web.models.collaboration import ResultOwnerQuery

            if type(native) is not MinuteNativeReportCommandWriter or native.owner_authority is None:
                raise PermissionError("native minute export lacks its original bound authority")
            actor = self.outbox.trusted_command_actor(command.command_id, _command_hash(command))
            if actor != command.actor_id:
                raise PermissionError("native minute export requires its original authenticated command")
            original = self.minute_backend
            if type(original) is MinuteCommandWriter:
                if original.owner_authority is not native.owner_authority:
                    raise PermissionError("original minute export requires its own bound authority")
                original.installation.verify_current()
                original_context = original.installation.reader.get_command_context(command.job_id)
                if original_context is not None:
                    try:
                        original.owner_authority._trusted_result_owner(ResultOwnerQuery(domain="minute",
                            job_id=str(command.job_id), spec_hash=original_context.job.spec_hash),
                            authenticated_actor_id=actor)
                    except LookupError:
                        pass
                    else:
                        return original
            native.runtime.verify_current()
            context = native.runtime.reader.get_command_context(command.job_id)
            if context is None:
                raise LookupError("native minute original target is unavailable")
            try:
                native.owner_authority._trusted_result_owner(ResultOwnerQuery(domain="minute",
                    job_id=str(command.job_id), spec_hash=context.job.spec_hash), authenticated_actor_id=actor)
            except LookupError:
                return native
            if type(self.minute_backend) is not MinuteCommandWriter or self.minute_backend.owner_authority is not native.owner_authority:
                raise PermissionError("original submitted minute export lacks its own bound writer")
            raise PermissionError("original submitted minute target is absent from its own Lab")
        if self.minute_backend is None:
            raise RuntimeError("minute writer is unavailable")
        return self.minute_backend

    def drain_price_rule_command(
        self, command: _OwnedPriceAlertRuleValue
    ) -> tuple[PageControlReceipt, ...]:
        with _PageControlExecutionMutex(self._consumer_mutex_path()) as acquired:
            if not acquired:
                return ()
            claims = self.outbox.claim_records(
                limit=1,
                owner_id=self.consumer_id,
                lease_seconds=self.lease_seconds,
                now=self.clock(),
                target_command_id=command.command_id,
            )
            if not claims:
                return ()
            claim = claims[0]
            if claim.command != command:
                raise PageControlCommandConflictError("price rule command changed before claim")
            return (self.outbox.complete_price_rule(claim, now=self.clock()),)

    def drain_condition_rule_command(
        self, command: _OwnedConditionAlertRuleValue
    ) -> tuple[PageControlReceipt, ...]:
        with _PageControlExecutionMutex(self._consumer_mutex_path()) as acquired:
            if not acquired:
                return ()
            claims = self.outbox.claim_records(
                limit=1,
                owner_id=self.consumer_id,
                lease_seconds=self.lease_seconds,
                now=self.clock(),
                target_command_id=command.command_id,
            )
            if not claims:
                return ()
            claim = claims[0]
            if claim.command != command:
                raise PageControlCommandConflictError("condition request changed before claim")
            return (
                self.outbox.complete_condition_rule(
                    claim, now=self.clock(), resolve_scope=self.condition_rule_scope
                ),
            )

    def _complete_screen_query_claim(self, claim: PageControlClaim) -> PageControlReceipt:
        self.outbox.require_current_command_role(claim.command)
        if self.screen_query_history is None:
            raise RuntimeError("private screening is not configured")
        data = None
        code = None
        if type(claim.command) is _OwnedExecuteScreenQuery:
            if self.screen_query_executor is None:
                raise RuntimeError("private screening executor is not configured")
            self.screen_query_history.mark_started(claim, now=self.clock())
            from rquant.web.screen_service import ScreenApplicationError

            try:
                data = self.screen_query_executor(claim.command.definition)
            except ScreenApplicationError as error:
                code = "source_expired" if error.status_code == 409 else "source_unavailable"
        self.outbox.require_current_command_role(claim.command)
        return self.screen_query_history.complete(
            claim, now=self.clock(), data=data, failure_code=code
        )

    def drain_screen_query_command(
        self, command: _OwnedExecuteScreenQuery | _OwnedSaveNlPreset
    ) -> tuple[PageControlReceipt, ...]:
        with _PageControlExecutionMutex(self._consumer_mutex_path()) as acquired:
            if not acquired:
                return ()
            claims = self.outbox.claim_records(
                limit=1,
                owner_id=self.consumer_id,
                lease_seconds=self.lease_seconds,
                now=self.clock(),
                target_command_id=command.command_id,
            )
            if not claims:
                return ()
            if claims[0].command != command:
                raise PageControlCommandConflictError("screen command changed before claim")
            return (self._complete_screen_query_claim(claims[0]),)

    def drain_research_query_command(
        self, command: _OwnedSaveResearchQuery
    ) -> tuple[PageControlReceipt, ...]:
        with _PageControlExecutionMutex(self._consumer_mutex_path()) as acquired:
            if not acquired:
                return ()
            claims = self.outbox.claim_records(
                limit=1,
                owner_id=self.consumer_id,
                lease_seconds=self.lease_seconds,
                now=self.clock(),
                target_command_id=command.command_id,
            )
            if not claims:
                return ()
            if claims[0].command != command:
                raise PageControlCommandConflictError("query command changed before claim")
            return (self.outbox.complete_research_query(claims[0], now=self.clock()),)

    def drain_factor_archive_command(
        self, command: _OwnedArchiveFactor
    ) -> tuple[PageControlReceipt, ...]:
        return self.drain_factor_definition_command(command)

    def drain_strategy_authoring_command(
        self, command: OwnedStrategyTemplateCommand
    ) -> tuple[PageControlReceipt, ...]:
        with _PageControlExecutionMutex(self._consumer_mutex_path()) as acquired:
            if not acquired:
                return ()
            claims = self.outbox.claim_records(
                limit=1,
                owner_id=self.consumer_id,
                lease_seconds=self.lease_seconds,
                now=self.clock(),
                target_command_id=command.command_id,
            )
            if not claims:
                return ()
            if claims[0].command != command:
                raise PageControlCommandConflictError("strategy command changed before claim")
            return self._complete_claims(claims)

    def drain_strategy_promotion_command(self, command: OwnedStrategyPromotionCommand) -> tuple[PageControlReceipt, ...]:
        with _PageControlExecutionMutex(self._consumer_mutex_path()) as acquired:
            if not acquired:
                return ()
            claims = self.outbox.claim_records(limit=1, owner_id=self.consumer_id,
                lease_seconds=self.lease_seconds, now=self.clock(), target_command_id=command.command_id)
            if not claims:
                return ()
            if claims[0].command != command:
                raise PageControlCommandConflictError("original promotion changed before claim")
            return self._complete_claims(claims)

    def drain_paper_portfolio_command(
        self, command: OwnedPaperPortfolioCommand
    ) -> tuple[PageControlReceipt, ...]:
        with _PageControlExecutionMutex(self._consumer_mutex_path()) as acquired:
            if not acquired:
                return ()
            claims = self.outbox.claim_records(
                limit=1, owner_id=self.consumer_id, lease_seconds=self.lease_seconds,
                now=self.clock(), target_command_id=command.command_id,
            )
            if not claims:
                return ()
            if claims[0].command != command:
                raise PageControlCommandConflictError("paper command changed before claim")
            return self._complete_claims(claims)


    def drain_factor_definition_command(
        self, command: _OwnedFactorDefinitionValue | _OwnedSubmitFactorRun | _OwnedSetFactorTracked
    ) -> tuple[PageControlReceipt, ...]:
        with _PageControlExecutionMutex(self._consumer_mutex_path()) as acquired:
            if not acquired:
                return ()
            claims = self.outbox.claim_records(
                limit=1,
                owner_id=self.consumer_id,
                lease_seconds=self.lease_seconds,
                now=self.clock(),
                target_command_id=command.command_id,
            )
            if not claims:
                return ()
            claim = claims[0]
            if claim.command != command:
                raise PageControlCommandConflictError("factor command changed before claim")
            return self._complete_claims((claim,))

    def _drain_locked(self, *, limit: int) -> tuple[PageControlReceipt, ...]:
        return self._complete_claims(
            self.outbox.claim_records(
                limit=limit,
                owner_id=self.consumer_id,
                lease_seconds=self.lease_seconds,
                now=self.clock(),
            )
        )

    def _complete_claims(
        self, claims: tuple[PageControlClaim, ...]
    ) -> tuple[PageControlReceipt, ...]:
        receipts: list[PageControlReceipt] = []
        for claim in claims:
            try:
                fence = self._promotion_command_fence(claim.command) if type(claim.command) in PROMOTION_OWNED_TYPES else self.outbox.command_fence(claim.command)
                with fence:
                    receipts.append(self._complete_claim_locked(claim))
            except PermissionError:
                receipts.append(self.outbox.complete(claim.command.command_id,
                    error="current_permission_denied", owner_id=claim.owner_id,
                    claim_token=claim.claim_token))
        return tuple(receipts)

    @contextmanager
    def _promotion_command_fence(self, command: OwnedStrategyPromotionCommand) -> Iterator[None]:
        with self.outbox.collaboration.locked():
            self._strategy_promotion_backend().validate(command)
            actor = self.outbox.trusted_command_actor(command.command_id, _command_hash(command))
            if actor != command.owner_id:
                raise PermissionError("original manual command actor differs")
            token = _TRUSTED_COLLABORATION_ACTOR.set(actor)
            try:
                yield
            finally:
                _TRUSTED_COLLABORATION_ACTOR.reset(token)

    def _complete_claim_locked(self, claim: PageControlClaim) -> PageControlReceipt:
        if type(claim.command) in (_OwnedExecuteScreenQuery, _OwnedSaveNlPreset):
            return self._complete_screen_query_claim(claim)
        if isinstance(claim.command, _OwnedSaveResearchQuery):
            return self.outbox.complete_research_query(claim, now=self.clock())
        if isinstance(claim.command, (AddWatchlistItem, RemoveWatchlistItem)):
            return self.outbox.complete_watchlist(claim, now=self.clock())
        if isinstance(claim.command, (_OwnedSavePriceAlertRule, _OwnedSetPriceAlertRuleEnabled, _OwnedDeletePriceAlertRule)):
            return self.outbox.complete_price_rule(claim, now=self.clock())
        if type(claim.command) in _CONDITION_OWNED_TYPES:
            return self.outbox.complete_condition_rule(claim, now=self.clock(), resolve_scope=self.condition_rule_scope)
        if isinstance(claim.command, AckAlert):
            try:
                self._assert_command_time(claim.command)
            except ValueError as exc:
                return self.outbox.complete(claim.command.command_id, error=f"{type(exc).__name__}: {exc}",
                    owner_id=claim.owner_id, claim_token=claim.claim_token)
            return self.outbox.complete_ack(claim)
        try:
            outcome = self._execute_claim(claim)
        except _RetryableUncertainEffectError:
            return self.outbox.release_claim_for_retry(claim.command.command_id,
                owner_id=claim.owner_id, claim_token=claim.claim_token)
        except Exception as exc:
            return self.outbox.complete(claim.command.command_id, error=f"{type(exc).__name__}: {exc}",
                owner_id=claim.owner_id, claim_token=claim.claim_token)
        return self.outbox.complete(claim.command.command_id, result=outcome.result, error=outcome.error,
            status=outcome.status, owner_id=claim.owner_id, claim_token=claim.claim_token)

    def _consumer_mutex_path(self) -> Path:
        return self.outbox.path.with_name(f"{self.outbox.path.name}{_CONSUMER_MUTEX_SUFFIX}")

    def _assert_command_time(self, command: PageControlCommandValue) -> None:
        observed_at = _normalize_utc(self.clock())
        if command.requested_at > observed_at + _MAX_REQUEST_FUTURE_SKEW:
            raise ValueError("page control command requested_at exceeds allowed future clock skew")

    def _execute_claim(self, claim: PageControlClaim) -> _ExecutionOutcome:
        command = claim.command
        if type(command) is SetUserRoleCommand:
            if self.outbox.trusted_command_actor(command.command_id, _command_hash(command)) != command.actor_id:
                raise PermissionError("original role command actor is unavailable")
        elif type(command) in PROMOTION_OWNED_TYPES:
            backend = self._strategy_promotion_backend()
            backend.validate(command)
            if self.outbox.trusted_command_actor(command.command_id, _command_hash(command)) != command.owner_id:
                raise PermissionError("original promotion private actor differs")
            try:
                recovered = backend.recover(command)
            except Exception as exc:
                raise _RetryableUncertainEffectError("原评估结果暂时无法核验。") from exc
            prior_effect = self.outbox.effect(command.command_id)
            if recovered is None and prior_effect is None:
                self.outbox.require_current_command_role(command)
                backend.authorize(command.owner_id, command.original())
        else:
            self.outbox.require_current_command_role(command)
        self._assert_command_time(command)
        if type(command) is _OwnedSubmitFactorRun:
            self._require_factor_job_owner(command)
        self._require_owned_lab_target(command)
        effect, created = self.outbox.begin_effect(
            command,
            owner_id=claim.owner_id,
            claim_token=claim.claim_token,
            now=self.clock(),
        )
        terminal = self._outcome_from_effect(effect)
        if terminal is not None:
            return terminal
        if type(command) is SubmitMinuteParameterStudy:
            return self._execute_minute_study_claim(claim, effect=effect)
        if type(command) is SetUserRoleCommand:
            return self._execute_role_change(claim, effect)
        if type(command) in TASK_CONTROL_OWNED_TYPES:
            marker = {"contract": "task-control-identity/v1", "identity": command.metadata_identity.model_dump(mode="json")}
            try:
                self._task_control_backend().validate(command)
                if effect.result is None:
                    effect = self.outbox.record_started_effect_result(command.command_id, result=marker,
                        owner_id=claim.owner_id, claim_token=claim.claim_token)
                if effect.result != marker:
                    raise ValueError("task original metadata identity differs")
            except Exception as exc:
                raise _RetryableUncertainEffectError(f"task original identity cannot be verified: {exc}") from exc
        if isinstance(command, EXPERIMENT_COMMAND_TYPES) and effect.result is None:
            try:
                if self.experiment_backend is None:
                    raise RuntimeError("formal experiment writer is unavailable")
                marker = self.experiment_backend.freeze(command)
            except ExperimentPreparationUncertainError as exc:
                raise _RetryableUncertainEffectError(str(exc)) from exc
            except Exception as exc:
                effect = self.outbox.finish_effect(
                    command.command_id,
                    status=PageControlEffectStatus.FAILED,
                    error=f"{type(exc).__name__}: {exc}",
                    owner_id=claim.owner_id,
                    claim_token=claim.claim_token,
                )
                outcome = self._outcome_from_effect(effect)
                assert outcome is not None
                return outcome
            try:
                effect = self.outbox.record_started_effect_result(
                    command.command_id,
                    result=marker,
                    owner_id=claim.owner_id,
                    claim_token=claim.claim_token,
                )
            except Exception as exc:
                # freeze may already have committed an exact grant or note.
                # Reuse this command even when its effect marker receipt was lost.
                raise _RetryableUncertainEffectError(
                    "original experiment admission marker needs recovery"
                ) from exc
        if isinstance(command, (OwnedSaveStrategyTemplate, OwnedArchiveStrategyTemplate, OwnedRunStrategyTemplate)):
            marker = {"contract": "strategy-authoring-identity/v1", "identity": command.metadata_identity.model_dump(mode="json")}
            try:
                self._strategy_authoring_backend().validate(command)
                if effect.result is None:
                    effect = self.outbox.record_started_effect_result(
                        command.command_id, result=marker,
                        owner_id=claim.owner_id, claim_token=claim.claim_token,
                    )
                if effect.result != marker:
                    raise ValueError("strategy original metadata identity differs")
            except Exception as exc:
                raise _RetryableUncertainEffectError(f"strategy original identity cannot be verified: {exc}") from exc
        if type(command) in PROMOTION_OWNED_TYPES:
            marker = {"contract": "strategy-manual-promotion-identity/v1",
                "identity": command.metadata_identity.model_dump(mode="json"),
                "original_request_hash": command.original().request_hash}
            try:
                self._strategy_promotion_backend().validate(command)
                if effect.result is None:
                    effect = self.outbox.record_started_effect_result(command.command_id, result=marker,
                        owner_id=claim.owner_id, claim_token=claim.claim_token)
                if effect.result != marker:
                    raise ValueError("original promotion effect marker differs")
            except Exception as exc:
                raise _RetryableUncertainEffectError("原评估回执待核验。") from exc
        if type(command) in (OwnedSetPaperAccountPaused, OwnedSavePaperPortfolioConfiguration, OwnedRunPaperPortfolioResearch):
            marker = {"contract": "paper-portfolio-identity/v1", "identity": command.metadata_identity.model_dump(mode="json")}
            try:
                self._paper_portfolio_backend().validate(command)
                if effect.result is None:
                    effect = self.outbox.record_started_effect_result(
                        command.command_id, result=marker,
                        owner_id=claim.owner_id, claim_token=claim.claim_token,
                    )
                if effect.result != marker:
                    raise ValueError("paper original metadata identity differs")
            except Exception as exc:
                raise _RetryableUncertainEffectError(f"paper original identity cannot be verified: {exc}") from exc
        if (
            isinstance(command, (SubmitPortfolioBacktest, ExportPortfolioBacktestZip))
            and effect.result is None
        ):
            try:
                if self.portfolio_backend is None:
                    raise RuntimeError("portfolio writer is unavailable")
                marker = self.portfolio_backend.freeze(command)
                effect = self.outbox.record_started_effect_result(
                    command.command_id,
                    result=marker,
                    owner_id=claim.owner_id,
                    claim_token=claim.claim_token,
                )
            except Exception as exc:
                effect = self.outbox.finish_effect(
                    command.command_id,
                    status=PageControlEffectStatus.FAILED,
                    error=f"{type(exc).__name__}: {exc}",
                    owner_id=claim.owner_id,
                    claim_token=claim.claim_token,
                )
                outcome = self._outcome_from_effect(effect)
                assert outcome is not None
                return outcome
        if isinstance(command, (SubmitMinuteReplay, ExportMinuteReplayZip)) and effect.result is None:
            try:
                marker = self._selected_minute_backend(command).freeze(command)
                effect = self.outbox.record_started_effect_result(command.command_id, result=marker,
                    owner_id=claim.owner_id, claim_token=claim.claim_token)
            except Exception as exc:
                effect = self.outbox.finish_effect(command.command_id, status=PageControlEffectStatus.FAILED,
                    error=f"{type(exc).__name__}: {exc}", owner_id=claim.owner_id, claim_token=claim.claim_token)
                outcome = self._outcome_from_effect(effect)
                assert outcome is not None
                return outcome
        if isinstance(command, _OwnedSetFactorTracked):
            marker = {
                "contract": "factor-tracking-identities/v1",
                "registry_identity": command.registry_identity.model_dump(mode="json"),
                "tracking_identity": command.tracking_identity.model_dump(mode="json"),
            }
            if effect.result is None:
                self._factor_tracking_backend().validate(command)
                effect = self.outbox.record_started_effect_result(
                    command.command_id,
                    result=marker,
                    owner_id=claim.owner_id,
                    claim_token=claim.claim_token,
                )
            if effect.result != marker:
                raise _RetryableUncertainEffectError("tracking original identities differ")
        if isinstance(command, _OwnedSubmitFactorRun):
            if effect.result is None:
                self._factor_run_backend().validate(command)
                effect = self.outbox.record_started_effect_result(
                    command.command_id,
                    result={
                        "contract": "factor-run-identities/v1",
                        "registry_identity": command.registry_identity.model_dump(mode="json"),
                        "ledger_identity": command.ledger_identity.model_dump(mode="json"),
                    },
                    owner_id=claim.owner_id,
                    claim_token=claim.claim_token,
                )
            if effect.result != {
                "contract": "factor-run-identities/v1",
                "registry_identity": command.registry_identity.model_dump(mode="json"),
                "ledger_identity": command.ledger_identity.model_dump(mode="json"),
            }:
                raise _RetryableUncertainEffectError("factor run original identities differ")
        if isinstance(command, (_OwnedSaveFactorDefinition, _OwnedArchiveFactor)):
            if effect.result is None:
                try:
                    identity = self._factor_definition_backend().identity()
                    if command.registry_identity is not None:
                        if identity != command.registry_identity:
                            raise ValueError("factor registry changed after admission")
                        identity = command.registry_identity
                    effect = self.outbox.record_started_effect_result(
                        command.command_id,
                        result={
                            "contract": _FACTOR_REGISTRY_EFFECT_IDENTITY,
                            "identity": identity.model_dump(mode="json"),
                        },
                        owner_id=claim.owner_id,
                        claim_token=claim.claim_token,
                    )
                except Exception as exc:
                    if self._must_recover_before_failure(command, created=created):
                        raise _RetryableUncertainEffectError(
                            f"factor registry identity cannot be recorded: {exc}"
                        ) from exc
                    effect = self.outbox.finish_effect(
                        command.command_id,
                        status=PageControlEffectStatus.FAILED,
                        error=f"{type(exc).__name__}: {exc}",
                        owner_id=claim.owner_id,
                        claim_token=claim.claim_token,
                    )
                    outcome = self._outcome_from_effect(effect)
                    assert outcome is not None
                    return outcome
            else:
                try:
                    self._factor_effect_identity(effect)
                except Exception as exc:
                    raise _RetryableUncertainEffectError(
                        f"factor registry effect identity is invalid: {exc}"
                    ) from exc
        local_fence_targets = self._local_effect_fence_targets(command)
        if created and local_fence_targets:
            try:
                local_fence = self._local_effect_fence(local_fence_targets)
                effect = self.outbox.record_started_effect_result(
                    command.command_id,
                    result=local_fence,
                    owner_id=claim.owner_id,
                    claim_token=claim.claim_token,
                )
            except Exception as exc:
                effect = self.outbox.finish_effect(
                    command.command_id,
                    status=PageControlEffectStatus.FAILED,
                    error=f"{type(exc).__name__}: {exc}",
                    owner_id=claim.owner_id,
                    claim_token=claim.claim_token,
                )
                outcome = self._outcome_from_effect(effect)
                assert outcome is not None
                return outcome
        if local_fence_targets:
            mismatch_reason = self._local_effect_fence_mismatch_reason(
                effect,
                local_fence_targets,
            )
            if mismatch_reason is not None:
                result = _ambiguous_local_effect_result(command, reason=mismatch_reason)
                effect = self.outbox.finish_effect(
                    command.command_id,
                    status=PageControlEffectStatus.AMBIGUOUS,
                    result=result,
                    error=mismatch_reason,
                    owner_id=claim.owner_id,
                    claim_token=claim.claim_token,
                )
                outcome = self._outcome_from_effect(effect)
                assert outcome is not None
                return outcome
        try:
            bindings = self._bind_local_effect_directories(
                effect,
                local_fence_targets,
            )
        except Exception as exc:
            reason = f"local filesystem effect directory binding failed: {exc}"
            result = _ambiguous_local_effect_result(command, reason=reason)
            effect = self.outbox.finish_effect(
                command.command_id,
                status=PageControlEffectStatus.AMBIGUOUS,
                result=result,
                error=reason,
                owner_id=claim.owner_id,
                claim_token=claim.claim_token,
            )
            outcome = self._outcome_from_effect(effect)
            assert outcome is not None
            return outcome
        binding_token = _ACTIVE_EFFECT_DIRECTORY_BINDINGS.set(bindings)
        try:
            return self._execute_bound_claim(claim, effect=effect, created=created)
        finally:
            _ACTIVE_EFFECT_DIRECTORY_BINDINGS.reset(binding_token)
            for binding in bindings.values():
                binding.close()

    def _execute_bound_claim(
        self,
        claim: PageControlClaim,
        *,
        effect: PageControlEffectRecord,
        created: bool,
    ) -> _ExecutionOutcome:
        command = claim.command
        if not created or isinstance(
            command,
            (
                _OwnedSaveFactorDefinition,
                _OwnedArchiveFactor,
                _OwnedSubmitFactorRun,
                _OwnedSetFactorTracked,
            ),
        ):
            try:
                recovered = self._recover_started_effect(command)
            except Exception as exc:
                if self._must_recover_before_failure(command, created=created):
                    raise _RetryableUncertainEffectError(f"{type(exc).__name__}: {exc}") from exc
                effect = self.outbox.finish_effect(
                    command.command_id,
                    status=PageControlEffectStatus.FAILED,
                    error=f"{type(exc).__name__}: {exc}",
                    owner_id=claim.owner_id,
                    claim_token=claim.claim_token,
                )
                outcome = self._outcome_from_effect(effect)
                assert outcome is not None
                return outcome
            if recovered is not None:
                self._verify_bound_effect_directories()
                effect = self.outbox.finish_effect(
                    command.command_id,
                    status=PageControlEffectStatus.SUCCEEDED,
                    result=recovered,
                    owner_id=claim.owner_id,
                    claim_token=claim.claim_token,
                )
                outcome = self._outcome_from_effect(effect)
                assert outcome is not None
                return outcome
            if type(command) in PROMOTION_OWNED_TYPES:
                try:
                    resumed = self._strategy_promotion_backend().recover_partial_walk_forward(command)
                except Exception as exc:
                    raise _RetryableUncertainEffectError("original WF child recovery is unconfirmed") from exc
                if resumed is not None:
                    effect = self.outbox.finish_effect(command.command_id, status=PageControlEffectStatus.SUCCEEDED, result=resumed, owner_id=claim.owner_id, claim_token=claim.claim_token)
                    outcome = self._outcome_from_effect(effect)
                    assert outcome is not None
                    return outcome
                effect = self.outbox.finish_effect(command.command_id,
                    status=PageControlEffectStatus.AMBIGUOUS,
                    error="原操作结果待确认，请保留这次操作。",
                    owner_id=claim.owner_id, claim_token=claim.claim_token)
                outcome = self._outcome_from_effect(effect)
                assert outcome is not None
                return outcome
            if _is_external_lab_effect(command):
                result = _ambiguous_lab_effect_result(command)
                effect = self.outbox.finish_effect(
                    command.command_id,
                    status=PageControlEffectStatus.AMBIGUOUS,
                    result=result,
                    error="external Lab effect started without a durable result",
                    owner_id=claim.owner_id,
                    claim_token=claim.claim_token,
                )
                outcome = self._outcome_from_effect(effect)
                assert outcome is not None
                return outcome
        try:
            result = self._execute(command)
        except Exception as exc:
            try:
                recovered = self._recover_started_effect(command)
            except Exception as recovery_exc:
                if self._must_recover_before_failure(command, created=created):
                    raise _RetryableUncertainEffectError(
                        f"{type(exc).__name__}: {exc}; recovery failed: "
                        f"{type(recovery_exc).__name__}: {recovery_exc}"
                    ) from recovery_exc
                recovered = None
            if recovered is not None:
                self._verify_bound_effect_directories()
                effect = self.outbox.finish_effect(
                    command.command_id,
                    status=PageControlEffectStatus.SUCCEEDED,
                    result=recovered,
                    owner_id=claim.owner_id,
                    claim_token=claim.claim_token,
                )
                outcome = self._outcome_from_effect(effect)
                assert outcome is not None
                return outcome
            effect = self.outbox.finish_effect(
                command.command_id,
                status=PageControlEffectStatus.FAILED,
                error=f"{type(exc).__name__}: {exc}",
                owner_id=claim.owner_id,
                claim_token=claim.claim_token,
            )
            outcome = self._outcome_from_effect(effect)
            assert outcome is not None
            return outcome
        self._verify_bound_effect_directories()
        effect = self.outbox.finish_effect(
            command.command_id,
            status=PageControlEffectStatus.SUCCEEDED,
            result=result,
            owner_id=claim.owner_id,
            claim_token=claim.claim_token,
        )
        outcome = self._outcome_from_effect(effect)
        assert outcome is not None
        return outcome

    def _bind_local_effect_directories(
        self,
        effect: PageControlEffectRecord,
        targets: tuple[_LocalEffectFenceTarget, ...],
    ) -> dict[Path, _BoundManagedDirectory]:
        if not targets:
            return {}
        result = effect.result
        if not isinstance(result, dict) or not isinstance(result.get("targets"), list):
            raise ValueError("started directory fence is unavailable")
        raw_by_path = {
            Path(os.path.abspath(str(item["path"]))): item
            for item in result["targets"]
            if isinstance(item, Mapping) and isinstance(item.get("path"), str)
        }
        bindings: dict[Path, _BoundManagedDirectory] = {}
        try:
            for target in targets:
                path = Path(os.path.abspath(target.path))
                raw = raw_by_path[path]
                if raw.get("missing") is True:
                    continue
                binding = _bind_managed_directory(path, create=False)
                observed = os.fstat(binding.descriptor)
                if observed.st_dev != raw.get("st_dev") or observed.st_ino != raw.get("st_ino"):
                    binding.close()
                    raise ValueError(f"fenced directory generation changed: {path}")
                bindings[path] = binding
            return bindings
        except Exception:
            for binding in bindings.values():
                binding.close()
            raise

    @staticmethod
    def _verify_bound_effect_directories() -> None:
        for binding in (_ACTIVE_EFFECT_DIRECTORY_BINDINGS.get() or {}).values():
            binding.verify()

    @staticmethod
    def _open_effect_directory(path: Path, *, create: bool) -> int:
        normalized = Path(os.path.abspath(path))
        binding = (_ACTIVE_EFFECT_DIRECTORY_BINDINGS.get() or {}).get(normalized)
        if binding is not None:
            return binding.duplicate()
        return (
            _open_or_create_managed_directory(normalized)
            if create
            else _open_existing_managed_directory(normalized)
        )

    @staticmethod
    def _bound_effect_directory_descriptor(path: Path) -> int | None:
        binding = (_ACTIVE_EFFECT_DIRECTORY_BINDINGS.get() or {}).get(Path(os.path.abspath(path)))
        if binding is None:
            return None
        binding.verify()
        return binding.descriptor

    def _execute_minute_study_claim(
        self, claim: PageControlClaim, *, effect: PageControlEffectRecord,
    ) -> _ExecutionOutcome:
        from rquant.minute_backtest_parameter_study_journal import MinuteParameterStudySubmissionUncertainError

        command = claim.command
        backend = self.minute_study_backend
        if type(command) is not SubmitMinuteParameterStudy or backend is None:
            raise RuntimeError("original minute study writer is unavailable")
        try:
            self.outbox.require_active_claim(command, owner_id=claim.owner_id, claim_token=claim.claim_token)
            recovering = effect.result is not None
            if not recovering:
                marker = backend.freeze(command)
                self.outbox.require_active_claim(command, owner_id=claim.owner_id, claim_token=claim.claim_token)
                effect = self.outbox.record_started_effect_result(command.command_id, result=marker,
                    owner_id=claim.owner_id, claim_token=claim.claim_token)
            if effect.result is None:
                raise RuntimeError("original minute study admission is unavailable")
            self.outbox.require_active_claim(command, owner_id=claim.owner_id, claim_token=claim.claim_token)
            result = backend.recover(command, effect.result) if recovering else backend.submit(command, effect.result)
            if result is None:
                raise _RetryableUncertainEffectError("original minute study child receipt is unconfirmed")
            self.outbox.require_active_claim(command, owner_id=claim.owner_id, claim_token=claim.claim_token)
        except (_RetryableUncertainEffectError, MinuteParameterStudySubmissionUncertainError) as exc:
            raise _RetryableUncertainEffectError("original minute study submission is unconfirmed") from exc
        except Exception as exc:
            effect = self.outbox.finish_effect(command.command_id, status=PageControlEffectStatus.FAILED,
                error=f"{type(exc).__name__}: {exc}", owner_id=claim.owner_id, claim_token=claim.claim_token)
            outcome = self._outcome_from_effect(effect)
            assert outcome is not None
            return outcome
        effect = self.outbox.finish_effect(command.command_id, status=PageControlEffectStatus.SUCCEEDED,
            result=result, owner_id=claim.owner_id, claim_token=claim.claim_token)
        outcome = self._outcome_from_effect(effect)
        assert outcome is not None
        return outcome

    def _must_recover_before_failure(
        self, command: PageControlCommandValue, *, created: bool
    ) -> bool:
        if type(command) in PROMOTION_OWNED_TYPES:
            return True
        if type(command) in TASK_CONTROL_OWNED_TYPES:
            try:
                return self._task_control_backend().has_effect(command)
            except Exception:
                return True
        if isinstance(command, EXPERIMENT_COMMAND_TYPES):
            effect = self.outbox.effect(command.command_id)
            return effect is not None and effect.result is not None
        if type(command) in (OwnedSetPaperAccountPaused, OwnedSavePaperPortfolioConfiguration, OwnedRunPaperPortfolioResearch):
            try:
                return self._paper_portfolio_backend().has_effect(command)
            except Exception:
                return True
        if isinstance(command, (OwnedSaveStrategyTemplate, OwnedArchiveStrategyTemplate, OwnedRunStrategyTemplate)):
            return not created or self.strategy_authoring_backend is not None
        if isinstance(command, (SubmitPortfolioBacktest, ExportPortfolioBacktestZip)):
            effect = self.outbox.effect(command.command_id)
            return effect is not None and effect.result is not None
        if isinstance(command, (SubmitMinuteReplay, ExportMinuteReplayZip)):
            effect = self.outbox.effect(command.command_id)
            return effect is not None and effect.result is not None
        if isinstance(command, _OwnedSetFactorTracked):
            return not created or self.factor_tracking_backend is not None
        if isinstance(command, _OwnedSubmitFactorRun):
            return not created or self.factor_run_backend is not None
        if isinstance(command, (_OwnedSaveFactorDefinition, _OwnedArchiveFactor)):
            return not created or self.factor_definition_backend is not None
        if isinstance(command, SubmitDataAuditReport):
            # A started command may have queued a task before its receipt was lost.
            # A first attempt without a configured backend cannot have done so.
            return not created or self.data_audit_report_backend is not None
        if isinstance(command, SubmitFormulaMarketRun):
            return not created or self.formula_market_backend is not None
        if isinstance(command, SaveFormulaPoolV1):
            return not created or self.formula_pool_backend is not None
        return self._has_committed_local_mutation(command)

    def _has_committed_local_mutation(self, command: PageControlCommandValue) -> bool:
        if isinstance(command, CreateCanvas):
            try:
                record, _publication = self._read_verified_canvas_catalog(
                    command.name, require_current_head=False
                )
            except Exception:
                return False
            return (
                record.command_id == command.command_id
                and record.description == command.description
                and not record.pool_refs
                and record.source == "page_control"
            )
        if isinstance(command, (SaveUserPoolV2, SaveUserPoolV3)):
            try:
                if isinstance(command, SaveUserPoolV3):
                    return self._recover_user_pool_v3_result(command) is not None
                return self._recover_user_pool_v2_result(command) is not None
            except Exception:
                return False
        if not isinstance(command, DeleteCanvas):
            return False
        try:
            head = self._current_canvas_head(command.name)
        except Exception:
            return False
        return (
            head is not None
            and head.state == "deleted"
            and head.receipt.claims.command.command_id == command.command_id
            and head.authority_command_kind == command.kind
            and head.authority_command_hash == _command_hash(command)
        )

    @staticmethod
    def _outcome_from_effect(
        effect: PageControlEffectRecord,
    ) -> _ExecutionOutcome | None:
        if effect.status == PageControlEffectStatus.STARTED:
            return None
        if effect.status == PageControlEffectStatus.SUCCEEDED:
            return _ExecutionOutcome(PageControlStatus.SUCCEEDED, effect.result)
        if effect.status == PageControlEffectStatus.AMBIGUOUS:
            return _ExecutionOutcome(
                PageControlStatus.AMBIGUOUS,
                effect.result,
                effect.error,
            )
        return _ExecutionOutcome(PageControlStatus.FAILED, effect.result, effect.error)

    def _execute_role_change(self, claim: PageControlClaim, effect: PageControlEffectRecord) -> _ExecutionOutcome:
        from rquant.collaboration_commands import confirm_set_user_role
        from rquant.collaboration_roles import RoleState

        command = claim.command
        assert type(command) is SetUserRoleCommand
        authority = self.outbox.collaboration
        if authority is None or authority.mode != "enforced":
            raise PermissionError("current role authority is unavailable")
        try:
            with authority.locked() as directory:
                current = authority._read_locked(directory)
                if effect.result is None:
                    after = confirm_set_user_role(current, command,
                        original_preparation=command.preparation, now=self.clock())
                    marker = {"contract": "page-control-role-cas/v1", "command_sha256": _command_hash(command),
                        "before": current.model_dump(mode="json"), "after": after.model_dump(mode="json")}
                    effect = self.outbox.record_started_effect_result(command.command_id, result=marker,
                        owner_id=claim.owner_id, claim_token=claim.claim_token)
                marker = effect.result
                if not isinstance(marker, dict) or set(marker) != {"contract", "command_sha256", "before", "after"} or marker["contract"] != "page-control-role-cas/v1" or marker["command_sha256"] != _command_hash(command):
                    raise PermissionError("original role effect binding differs")
                before = RoleState.model_validate_json(json.dumps(marker["before"]))
                after = RoleState.model_validate_json(json.dumps(marker["after"]))
                expected = confirm_set_user_role(before, command,
                    original_preparation=command.preparation, now=command.preparation.confirmation.issued_at)
                if expected != after:
                    raise PermissionError("original role effect result differs")
                if current == before:
                    if not any(e.username == command.actor_id and e.role == "admin" for e in current.users):
                        raise PermissionError("current admin permission is required")
                    authority._write_locked(directory, after)
                elif current != after:
                    raise PermissionError("role CAS generation differs")
            effect = self.outbox.finish_effect(command.command_id,
                status=PageControlEffectStatus.SUCCEEDED,
                result={"status": "role_changed", "revision": after.revision,
                    "state_sha256": after.content_sha256, "target_id": command.target_id, "role": command.new_role},
                owner_id=claim.owner_id, claim_token=claim.claim_token)
        except (ValueError, PermissionError) as exc:
            effect = self.outbox.finish_effect(command.command_id,
                status=PageControlEffectStatus.FAILED, error=f"{type(exc).__name__}: role change rejected",
                owner_id=claim.owner_id, claim_token=claim.claim_token)
        outcome = self._outcome_from_effect(effect)
        assert outcome is not None
        return outcome

    def _execute(self, command: PageControlCommandValue) -> JsonValue:
        if type(command) in PROMOTION_OWNED_TYPES:
            return self._strategy_promotion_backend().submit(command)
        self.outbox.require_current_command_role(command)
        if isinstance(command, (SubmitMinuteReplay, ExportMinuteReplayZip)):
            effect = self.outbox.effect(command.command_id)
            if effect is None or effect.result is None:
                raise RuntimeError("minute frozen admission is unavailable")
            return self._selected_minute_backend(command).submit(command, effect.result)
        if type(command) in TASK_CONTROL_OWNED_TYPES:
            return self._task_control_backend().submit(command)
        if isinstance(command, EXPERIMENT_COMMAND_TYPES):
            effect = self.outbox.effect(command.command_id)
            if self.experiment_backend is None or effect is None or effect.result is None:
                raise RuntimeError("formal experiment admission is unavailable")
            return self.experiment_backend.submit(command, effect.result)
        if type(command) in (OwnedSetPaperAccountPaused, OwnedSavePaperPortfolioConfiguration, OwnedRunPaperPortfolioResearch):
            return self._paper_portfolio_backend().submit(command)
        if isinstance(command, (OwnedSaveStrategyTemplate, OwnedArchiveStrategyTemplate, OwnedRunStrategyTemplate)):
            return self._strategy_authoring_backend().submit(command)
        if isinstance(command, (SubmitPortfolioBacktest, ExportPortfolioBacktestZip)):
            effect = self.outbox.effect(command.command_id)
            if self.portfolio_backend is None or effect is None or effect.result is None:
                raise RuntimeError("portfolio frozen admission is unavailable")
            return self.portfolio_backend.submit(command, effect.result)
        if isinstance(command, _OwnedSetFactorTracked):
            return self._factor_tracking_backend().submit(command)
        if isinstance(command, _OwnedSubmitFactorRun):
            return self._factor_run_backend().submit(command)
        if isinstance(command, (_OwnedSaveFactorDefinition, _OwnedArchiveFactor)):
            return self._factor_definition_backend().submit(
                command, expected_identity=self._expected_factor_identity(command.command_id)
            )
        if isinstance(command, CreateCanvas):
            return self._create_canvas(command)
        if isinstance(command, SaveCanvas):
            return self._save_canvas(command)
        if isinstance(command, DeleteCanvas):
            return self._delete_canvas(command)
        if isinstance(command, SetCanvasPoolRefs):
            existing, _publication = self._read_verified_canvas_catalog(command.name)
            save = SaveCanvas(
                command_id=command.command_id,
                requested_at=command.requested_at,
                name=command.name,
                description=existing.description,
                pool_refs=command.pool_refs,
                source="canvas_edit",
            )
            return self._save_canvas(save, identity_command=command)
        if isinstance(command, AddPoolToCanvas):
            return self._add_verified_pool_to_canvas(command)
        if isinstance(command, SaveUserPool):
            result = self._save_user_pool(command)
            if command.canvas_name is not None and command.canvas_name != "__default__":
                canvas_result = self._add_pool_to_canvas(
                    command.canvas_name,
                    f"user/{command.base_name}",
                    identity_command=command,
                )
                if isinstance(result, dict):
                    result = dict(result)
                    result["canvas_result"] = canvas_result
            return result
        if isinstance(command, SaveUserPoolV2):
            return self._save_user_pool_v2(command)
        if isinstance(command, SaveUserPoolV3):
            return self._save_user_pool_v3(command)
        if isinstance(command, SaveFormulaPoolV1):
            return self._formula_pool_backend().submit(command)
        if isinstance(command, DeleteUserPool):
            return {"deleted": self._delete(self._user_pool_path(command.base_name))}
        if isinstance(command, ForkBuiltinPool):
            return self._fork_builtin(command)
        if isinstance(command, SaveNlPreset):
            if not command.overwrite and self._managed_json_exists(
                self._user_pool_path(command.name)
            ):
                raise FileExistsError(f"preset already exists: {command.name}")
            save = SaveUserPool(
                command_id=command.command_id,
                requested_at=command.requested_at,
                base_name=command.name,
                description=command.description,
                rule_calls=command.rule_calls,
                include_columns=command.include_columns,
                source="nl_input",
            )
            return self._save_user_pool(save, identity_command=command)
        if isinstance(command, AppendNlQueryLog):
            return self._append_nl_log(command)
        if isinstance(command, InitializeLabExports):
            return self._initialize_lab_exports(command)
        if isinstance(command, SubmitLabCommand):
            return self._lab_backend().submit_command(
                command.command,
                interaction_key=command.interaction_key,
            )
        if isinstance(command, SubmitBackfillPlan):
            return self._backfill_plan_backend().submit(command)
        if isinstance(command, SubmitDataAuditReport):
            return self._data_audit_report_backend().submit(command)
        if isinstance(command, DATA_CENTER_EXECUTION_COMMAND_TYPES):
            return self._data_center_execution_backend().submit(command)
        if isinstance(command, SubmitFormulaMarketRun):
            return self._formula_market_backend().submit(command)
        if isinstance(command, ExportLabArtifactZip):
            return self._lab_backend().export_zip(command.job_id)
        if isinstance(command, DiscardLabArtifactZip):
            return self._lab_backend().discard_zip(command)
        raise TypeError(f"unsupported page control command: {type(command).__name__}")

    def _lab_backend(self) -> LabPageControlBackend:
        if self.lab_backend is None:
            raise RuntimeError("Lab page control backend is unavailable")
        return self.lab_backend

    def _backfill_plan_backend(self) -> BackfillPlanPageControlBackend:
        if self.backfill_plan_backend is None:
            raise RuntimeError("backfill plan backend is unavailable")
        return self.backfill_plan_backend

    def _data_audit_report_backend(self) -> DataAuditReportPageControlBackend:
        if self.data_audit_report_backend is None:
            raise RuntimeError("data audit report backend is unavailable")
        return self.data_audit_report_backend

    def _data_center_execution_backend(self) -> DataCenterExecutionPageControlBackend:
        if self.data_center_execution_backend is None:
            raise RuntimeError('data center execution backend is unavailable')
        return self.data_center_execution_backend

    def _formula_market_backend(self) -> FormulaMarketPageControlBackend:
        if self.formula_market_backend is None:
            raise RuntimeError("formula market backend is unavailable")
        return self.formula_market_backend

    def _formula_pool_backend(self) -> FormulaPoolPageControlBackend:
        if self.formula_pool_backend is None:
            raise RuntimeError("formula pool backend is unavailable")
        return self.formula_pool_backend

    def _require_owned_lab_target(self, command: PageControlCommandValue) -> None:
        authority = self.outbox.collaboration
        if authority is None or authority.mode != "enforced":
            return
        from rquant.lab_job_protocol import SubmitJobCommand
        from rquant.lab_job_center import LabCommandSubmissionFacade
        from rquant.lab_page_control import LabPageControlWriter
        from rquant.portfolio_backtest_commands import PortfolioCommandWriter

        expected_hash, required_domain = None, None
        if type(command) is ExportPortfolioBacktestZip:
            backend = self.portfolio_backend
            if type(backend) is not PortfolioCommandWriter:
                raise PermissionError("original portfolio target authority is unavailable")
            expected_hash, required_domain = command.result_hash, "portfolio"
            job_id, commands = command.job_id, backend.commands
        elif type(command) is ExportMinuteReplayZip:
            from rquant.minute_backtest_commands import MinuteCommandWriter
            backend = self._selected_minute_backend(command)
            if backend is self.minute_native_report_backend:
                from rquant.minute_experiment_result_owner import MinuteExperimentProvenance
                from rquant.web.models.collaboration import MinuteReportOwnerQuery

                actor = self.outbox.trusted_command_actor(command.command_id, _command_hash(command))
                context = backend.runtime.reader.get_command_context(command.job_id)
                if actor != command.actor_id or context is None:
                    raise PermissionError("native minute original target or actor is unavailable")
                proof = backend.owner_authority._trusted_minute_report_owner(MinuteReportOwnerQuery(
                    job_id=str(command.job_id), spec_hash=context.job.spec_hash), authenticated_actor_id=actor)
                sealed = backend.runtime.reader.get_artifact_preview_authority(command.job_id)
                if type(proof) is not MinuteExperimentProvenance or sealed is None or sealed.evidence.complete_result_hash != command.result_hash:
                    raise PermissionError("native minute original owner or sealed target differs")
                if backend.runtime.reader.get_command_context(command.job_id) != context:
                    raise PermissionError("native minute original target changed before effect")
                backend.runtime.verify_current()
                return
            if type(backend) is not MinuteCommandWriter:
                raise PermissionError("original minute target authority is unavailable")
            expected_hash, required_domain = command.result_hash, "minute"
            job_id, commands = command.job_id, backend.installation.commands
        elif type(command) in (ExportLabArtifactZip, DiscardLabArtifactZip, SubmitLabCommand):
            if type(command) is SubmitLabCommand and type(command.command) is SubmitJobCommand:
                return
            backend = self.lab_backend
            if type(backend) is not LabPageControlWriter:
                raise PermissionError("original Lab target authority is unavailable")
            job_id = command.command.job_id if type(command) is SubmitLabCommand else command.job_id
            commands = backend.commands
        else:
            return
        if type(commands) is not LabCommandSubmissionFacade:
            raise PermissionError("original Lab command owner is unavailable")
        actor = self.outbox.trusted_command_actor(command.command_id, _command_hash(command))
        context = commands.reader.get_command_context(job_id)
        if actor is None or context is None:
            raise PermissionError("original owned target is unavailable")
        domains = (required_domain,) if required_domain is not None else ("portfolio", "strategy")
        owned = False
        with self.outbox._connect() as connection:
            connection.execute("PRAGMA query_only = ON")
            connection.execute("BEGIN")
            for domain in domains:
                kind = {"portfolio": "submit_portfolio_backtest", "strategy": "run_strategy_template", "minute": "submit_minute_replay"}[domain]
                for row in self.outbox._result_submission_rows(connection, kind):
                    proof = self.outbox._result_submission_binding(row, authority)
                    if (proof is not None and UUID(proof.job_id) == job_id
                            and proof.spec_hash == context.job.spec_hash and proof.owner_id == actor):
                        owned = True
        if not owned:
            raise PermissionError("original target belongs to another or unknown owner")
        if expected_hash is not None:
            sealed = commands.reader.get_artifact_preview_authority(job_id)
            if sealed is None or sealed.evidence.complete_result_hash != expected_hash:
                raise PermissionError("original sealed target differs")
        if commands.reader.get_command_context(job_id) != context:
            raise PermissionError("original target changed before effect")

    def _require_factor_job_owner(self, command: _OwnedSubmitFactorRun, *, actor_id: str | None = None) -> None:
        authority = self.outbox.collaboration
        if authority is None or authority.mode != "enforced":
            return
        actor = actor_id or self.outbox.trusted_command_actor(command.command_id, _command_hash(command))
        if actor is None or actor != command.actor_id:
            raise PermissionError("original factor submit actor is unavailable")
        authority.require_command(actor, command.kind)
        self._factor_run_backend().validate(command)
        origin = self.outbox._factor_job_origin(command)
        if origin is None or origin[0] == command.command_id:
            return
        with self.outbox._connect() as connection:
            connection.execute("PRAGMA query_only = ON")
            connection.execute("BEGIN")
            for row in self.outbox._result_submission_rows(connection, command.kind):
                if row["command_id"] != origin[0]:
                    continue
                previous = _COMMAND_ADAPTER.validate_json(row["payload_json"])
                if (type(previous) is not _OwnedSubmitFactorRun
                        or previous.ledger_identity != command.ledger_identity or previous.spec != command.spec):
                    raise PermissionError("original factor submission origin differs")
                self._factor_run_backend().validate(previous)
                proof = self.outbox._result_submission_binding(row, authority,
                    restored_private_owner=previous.actor_id)
                if proof is not None and proof.owner_id == actor:
                    return
                break
        raise PermissionError("original factor result belongs to another or unknown owner")

    def _factor_run_backend(self) -> FactorRunPageControlBackend:
        if self.factor_run_backend is None:
            raise ValueError("factor run backend is not configured")
        return self.factor_run_backend

    def _factor_tracking_backend(self) -> FactorTrackingPageControlBackend:
        if self.factor_tracking_backend is None:
            raise ValueError("tracking backend is unavailable")
        return self.factor_tracking_backend

    def _factor_definition_backend(self) -> FactorDefinitionPageControlBackend:
        if self.factor_definition_backend is None:
            raise RuntimeError("factor definition backend is unavailable")
        return self.factor_definition_backend

    def _strategy_authoring_backend(self) -> StrategyAuthoringPageControlBackend:
        if self.strategy_authoring_backend is None:
            raise RuntimeError("strategy authoring backend is unavailable")
        return self.strategy_authoring_backend

    def _strategy_promotion_backend(self) -> StrategyPromotionPageControlBackend:
        from rquant.strategy_promotion import StrategyPromotionPageControlBackend
        if type(self.strategy_promotion_backend) is not StrategyPromotionPageControlBackend:
            raise RuntimeError("人工评估暂未开放。")
        return self.strategy_promotion_backend

    @staticmethod
    def _factor_effect_identity(effect: PageControlEffectRecord) -> FactorRegistryIdentity:
        result = effect.result
        if (
            not isinstance(result, dict)
            or set(result) != {"contract", "identity"}
            or result.get("contract") != _FACTOR_REGISTRY_EFFECT_IDENTITY
        ):
            raise ValueError("factor registry effect identity is unavailable")
        return FactorRegistryIdentity.model_validate(result["identity"])

    def _expected_factor_identity(self, command_id: str) -> FactorRegistryIdentity:
        effect = self.outbox.effect(command_id)
        if effect is None or effect.status is not PageControlEffectStatus.STARTED:
            raise RuntimeError("factor registry effect is not started")
        return self._factor_effect_identity(effect)

    def _local_effect_fence_targets(
        self,
        command: PageControlCommandValue,
    ) -> tuple[_LocalEffectFenceTarget, ...]:
        if isinstance(command, (SaveCanvas, CreateCanvas)):
            return self._canvas_publication_fence_targets(command.name)
        if isinstance(command, SetCanvasPoolRefs):
            return self._canvas_publication_fence_targets(command.name)
        if isinstance(command, AddPoolToCanvas):
            return (
                _LocalEffectFenceTarget(
                    role="user_pool_directory",
                    path=self._user_pool_path(command.pool_name.removeprefix("user/")).parent,
                    create=False,
                ),
                *self._canvas_publication_fence_targets(command.canvas_name),
            )
        if isinstance(command, SaveUserPool):
            targets = [
                _LocalEffectFenceTarget(
                    role="user_pool_directory",
                    path=self._user_pool_path(command.base_name).parent,
                )
            ]
            if command.canvas_name is not None and command.canvas_name != "__default__":
                targets.extend(self._canvas_publication_fence_targets(command.canvas_name))
            return tuple(targets)
        if isinstance(command, (SaveUserPoolV2, SaveUserPoolV3)):
            return (
                _LocalEffectFenceTarget(
                    role="user_pool_directory",
                    path=self._user_pool_path(command.base_name).parent,
                ),
            )
        if isinstance(command, SaveNlPreset):
            return (
                _LocalEffectFenceTarget(
                    role="user_pool_directory",
                    path=self._user_pool_path(command.name).parent,
                ),
            )
        if isinstance(command, ForkBuiltinPool):
            targets = [
                _LocalEffectFenceTarget(
                    role="user_pool_directory",
                    path=self._user_pool_path(command.target_base_name).parent,
                )
            ]
            if command.canvas_name is not None and command.canvas_name != "__default__":
                targets.extend(self._canvas_publication_fence_targets(command.canvas_name))
            return tuple(targets)
        if isinstance(command, AppendNlQueryLog):
            return (
                _LocalEffectFenceTarget(
                    role="nl_query_log_directory",
                    path=self.log_dir,
                ),
            )
        if isinstance(command, DeleteCanvas):
            return self._canvas_publication_fence_targets(
                command.name,
                create_canvas=False,
            )
        if isinstance(command, DeleteUserPool):
            return (
                _LocalEffectFenceTarget(
                    role="user_pool_directory",
                    path=self._user_pool_path(command.base_name).parent,
                    create=False,
                ),
            )
        return ()

    def _canvas_publication_fence_targets(
        self,
        canvas_name: str,
        *,
        create_canvas: bool = True,
    ) -> tuple[_LocalEffectFenceTarget, ...]:
        return (
            _LocalEffectFenceTarget(
                role="canvas_directory",
                path=self._canvas_path(canvas_name).parent,
                create=create_canvas,
            ),
            _LocalEffectFenceTarget(
                role="canvas_receipt_directory",
                path=self.data_dir / "canvas-publication-receipts",
            ),
            _LocalEffectFenceTarget(
                role="canvas_head_directory",
                path=self._canvas_head_root(canvas_name),
            ),
            _LocalEffectFenceTarget(
                role="canvas_watermark_directory",
                path=self._canvas_watermark_root(canvas_name),
            ),
        )

    def _local_effect_fence(
        self,
        targets: tuple[_LocalEffectFenceTarget, ...],
    ) -> dict[str, object]:
        return {
            "schema_version": _LOCAL_FILESYSTEM_FENCE_SCHEMA_VERSION,
            "kind": "local_filesystem_fence",
            "targets": [self._local_effect_target_identity(target) for target in targets],
        }

    @staticmethod
    def _local_effect_target_identity(
        target: _LocalEffectFenceTarget,
    ) -> dict[str, object]:
        path = Path(os.path.abspath(target.path))
        descriptor: int | None = None
        try:
            try:
                descriptor = (
                    _open_or_create_managed_directory(path)
                    if target.create
                    else _open_existing_managed_directory(path)
                )
            except FileNotFoundError:
                if target.create:
                    raise
                return {
                    "role": target.role,
                    "path": str(path),
                    "missing": True,
                }
            _verify_open_directory_matches_path(path, descriptor)
            observed = os.fstat(descriptor)
            return {
                "role": target.role,
                "path": str(path),
                "st_dev": observed.st_dev,
                "st_ino": observed.st_ino,
            }
        finally:
            if descriptor is not None:
                os.close(descriptor)

    def _local_effect_fence_mismatch_reason(
        self,
        effect: PageControlEffectRecord,
        targets: tuple[_LocalEffectFenceTarget, ...],
    ) -> str | None:
        result = effect.result
        if not isinstance(result, dict):
            return "local filesystem effect lacks a started directory fence"
        if result.get("schema_version") != _LOCAL_FILESYSTEM_FENCE_SCHEMA_VERSION:
            return "local filesystem effect has an unsupported directory fence"
        if result.get("kind") != "local_filesystem_fence":
            return "local filesystem effect has an unsupported fence kind"
        raw_targets = result.get("targets")
        if not isinstance(raw_targets, list):
            return "local filesystem effect has an invalid directory fence"
        expected_paths = {str(Path(os.path.abspath(target.path))) for target in targets}
        observed_by_path: dict[str, Mapping[str, object]] = {}
        for raw_target in raw_targets:
            if not isinstance(raw_target, Mapping):
                return "local filesystem effect has an invalid directory fence target"
            path_value = raw_target.get("path")
            if not isinstance(path_value, str):
                return "local filesystem effect has an invalid directory fence path"
            observed_by_path[str(Path(os.path.abspath(path_value)))] = raw_target
        if set(observed_by_path) != expected_paths:
            return "local filesystem effect directory fence targets do not match command"
        for target in targets:
            reason = self._local_effect_target_mismatch_reason(
                target,
                observed_by_path[str(Path(os.path.abspath(target.path)))],
            )
            if reason is not None:
                return reason
        return None

    @staticmethod
    def _local_effect_target_mismatch_reason(
        target: _LocalEffectFenceTarget,
        observed_target: Mapping[str, object],
    ) -> str | None:
        path = Path(os.path.abspath(target.path))
        if observed_target.get("missing") is True:
            try:
                os.stat(path, follow_symlinks=False)
            except FileNotFoundError:
                return None
            return "local filesystem effect target directory appeared after start"
        st_dev = observed_target.get("st_dev")
        st_ino = observed_target.get("st_ino")
        if not isinstance(st_dev, int) or not isinstance(st_ino, int):
            return "local filesystem effect has an invalid directory identity"
        binding: _BoundManagedDirectory | None = None
        try:
            binding = _bind_managed_directory(path, create=False)
            binding.verify()
            current = os.fstat(binding.descriptor)
        except Exception:
            return "local filesystem effect target directory cannot be verified"
        finally:
            if binding is not None:
                binding.close()
        if current.st_dev != st_dev or current.st_ino != st_ino:
            return "local filesystem effect target directory changed after start"
        return None

    def _paper_portfolio_backend(self) -> PaperPortfolioPageControlBackend:
        if self.paper_portfolio_backend is None:
            raise RuntimeError("paper portfolio backend is unavailable")
        return self.paper_portfolio_backend


    def _recover_started_effect(self, command: PageControlCommandValue) -> JsonValue | None:
        if isinstance(command, (SubmitMinuteReplay, ExportMinuteReplayZip)):
            effect = self.outbox.effect(command.command_id)
            if effect is None or effect.result is None:
                raise RuntimeError("minute original admission is unavailable")
            return self._selected_minute_backend(command).recover(command, effect.result)
        if type(command) in PROMOTION_OWNED_TYPES:
            return self._strategy_promotion_backend().recover(command)
        if type(command) in TASK_CONTROL_OWNED_TYPES:
            return self._task_control_backend().recover(command)
        if isinstance(command, EXPERIMENT_COMMAND_TYPES):
            effect = self.outbox.effect(command.command_id)
            if self.experiment_backend is None or effect is None or effect.result is None:
                raise RuntimeError("original experiment admission is unavailable")
            return self.experiment_backend.recover(command, effect.result)
        if type(command) in (OwnedSetPaperAccountPaused, OwnedSavePaperPortfolioConfiguration, OwnedRunPaperPortfolioResearch):
            return self._paper_portfolio_backend().recover(command)
        if isinstance(command, (OwnedSaveStrategyTemplate, OwnedArchiveStrategyTemplate, OwnedRunStrategyTemplate)):
            return self._strategy_authoring_backend().recover(command)
        if isinstance(command, (SubmitPortfolioBacktest, ExportPortfolioBacktestZip)):
            effect = self.outbox.effect(command.command_id)
            if self.portfolio_backend is None or effect is None or effect.result is None:
                raise RuntimeError("portfolio original admission is unavailable")
            return self.portfolio_backend.recover(command, effect.result)
        if isinstance(command, _OwnedSetFactorTracked):
            return self._factor_tracking_backend().recover(command)
        if isinstance(command, _OwnedSubmitFactorRun):
            return self._factor_run_backend().recover(command)
        if isinstance(command, (_OwnedSaveFactorDefinition, _OwnedArchiveFactor)):
            return self._factor_definition_backend().recover(
                command, expected_identity=self._expected_factor_identity(command.command_id)
            )
        if isinstance(command, SubmitBackfillPlan):
            return self._backfill_plan_backend().recover(command)
        if isinstance(command, SubmitDataAuditReport):
            return self._data_audit_report_backend().recover(command)
        if isinstance(command, DATA_CENTER_EXECUTION_COMMAND_TYPES):
            return self._data_center_execution_backend().recover(command)
        if isinstance(command, SubmitFormulaMarketRun):
            return self._formula_market_backend().recover(command)
        if isinstance(command, SaveFormulaPoolV1):
            return self._formula_pool_backend().recover(command)
        if isinstance(command, CreateCanvas):
            return self._recover_create_canvas_result(command)
        if isinstance(command, SaveCanvas):
            return self._recover_canvas_result(self._canvas_path(command.name), command)
        if isinstance(command, SetCanvasPoolRefs):
            return self._recover_canvas_result(self._canvas_path(command.name), command)
        if isinstance(command, AddPoolToCanvas):
            recovered = self._recover_canvas_result(self._canvas_path(command.canvas_name), command)
            return None if recovered is None else self._attach_result(command, recovered)
        if isinstance(command, SaveUserPool):
            result = self._recover_user_pool_result(command, identity_command=command)
            if result is None:
                return None
            if command.canvas_name is not None and command.canvas_name != "__default__":
                canvas_result = self._recover_canvas_pool_link(command)
                if canvas_result is None:
                    canvas_result = self._add_pool_to_canvas(
                        command.canvas_name,
                        f"user/{command.base_name}",
                        identity_command=command,
                    )
                if isinstance(result, dict):
                    result = dict(result)
                    result["canvas_result"] = canvas_result
            return result
        if isinstance(command, SaveUserPoolV2):
            return self._recover_user_pool_v2_result(command)
        if isinstance(command, SaveUserPoolV3):
            return self._recover_user_pool_v3_result(command)
        if isinstance(command, SaveNlPreset):
            save = SaveUserPool(
                command_id=command.command_id,
                requested_at=command.requested_at,
                base_name=command.name,
                description=command.description,
                rule_calls=command.rule_calls,
                include_columns=command.include_columns,
                source="nl_input",
            )
            return self._recover_user_pool_result(save, identity_command=command)
        if isinstance(command, ForkBuiltinPool):
            save = self._fork_builtin_save_command(command)
            result = self._recover_user_pool_result(save, identity_command=command)
            if result is None:
                return None
            if command.canvas_name is not None and command.canvas_name != "__default__":
                canvas_result = self._recover_canvas_pool_link(command)
                if canvas_result is None:
                    canvas_result = self._add_pool_to_canvas(
                        command.canvas_name,
                        f"user/{command.target_base_name}",
                        identity_command=command,
                    )
                if isinstance(result, dict):
                    result = dict(result)
                    result["canvas_result"] = canvas_result
            return result
        if isinstance(command, AppendNlQueryLog):
            if _managed_jsonl_contains_command_id(
                self.log_dir / "nl_queries.jsonl",
                command.command_id,
            ):
                return {"path": str(self.log_dir / "nl_queries.jsonl")}
            return None
        if isinstance(command, DeleteCanvas):
            return self._recover_delete_canvas(command)
        if isinstance(command, DeleteUserPool):
            return self._recover_delete_result(self._user_pool_path(command.base_name))
        if isinstance(command, InitializeLabExports):
            return self._recover_lab_exports(command)
        return None

    def _save_canvas(
        self,
        command: SaveCanvas,
        *,
        identity_command: PageControlCommandValue | None = None,
    ) -> JsonValue:
        if command.name == "__default__":
            raise ValueError("default canvas is virtual and cannot be persisted")
        identity = command if identity_command is None else identity_command
        path = self._canvas_path(command.name)
        existing: CanvasPublicationCatalogRecord | None = None
        catalog_exists = self._managed_json_exists(path)
        current_head = self._current_canvas_head(command.name)
        if catalog_exists:
            existing, _existing_publication = self._read_verified_canvas_catalog(
                command.name,
                expected_head=current_head,
            )
        else:
            self._assert_canvas_head_matches_latest_authority(
                command.name,
                current_head,
            )
            if current_head is not None and current_head.state == "active":
                raise ValueError("canvas current head is active but catalog is missing")
        if (
            current_head is not None
            and identity.requested_at < current_head.receipt.claims.command.requested_at
        ):
            raise ValueError("canvas current head is newer than the requested update")
        publication_command = self._canvas_publication_command(
            command,
            identity_command=identity,
        )
        requested_at = publication_command.requested_at
        created_at = existing.created_at if existing is not None else requested_at
        signer, keyring = self._require_canvas_publication_authority()
        if getattr(signer, "key_id", None) != keyring.active_key_id:
            raise ValueError("CanvasPublicationReceipt signer key must be active")
        publication = signer.issue_publication(
            build_canvas_publication_claims(
                command=publication_command,
                catalog_created_at=created_at,
                catalog_updated_at=requested_at,
                consumer_service_id=self.consumer_service_id,
                consumer_instance_id=self.consumer_id,
            )
        )
        if not keyring.verify_publication_receipt(publication, require_active=True):
            raise ValueError("CanvasPublicationReceipt active signature verification failed")
        self._canvas_publication_receipt_store().write_immutable(publication)
        self._atomic_json(
            path,
            publication.claims.catalog_record.model_dump(mode="json"),
            command_id=identity.command_id,
        )
        self._publish_canvas_head(
            identity_command=identity,
            canvas_name=command.name,
            state="active",
            publication_receipt_id=publication.receipt_id,
        )
        return self._canvas_publication_result(path, publication)

    def _create_canvas(self, command: CreateCanvas) -> JsonValue:
        if command.name == "__default__":
            raise ValueError("default canvas is virtual and cannot be persisted")
        path = self._canvas_path(command.name)
        current = self._current_canvas_head(command.name)
        if current is not None or self._managed_json_exists(path):
            raise FileExistsError("canvas name is already occupied")
        watermark = self._current_canvas_watermark(command.name)
        self._assert_canvas_watermark_matches_head(current, watermark)
        self._assert_canvas_head_matches_latest_authority(command.name, current)
        save = SaveCanvas(
            command_id=command.command_id,
            requested_at=command.requested_at,
            name=command.name,
            description=command.description,
            pool_refs=(),
            source="page_control",
        )
        return self._created_canvas_result(
            command, self._save_canvas(save, identity_command=command)
        )

    def _recover_create_canvas_result(self, command: CreateCanvas) -> JsonValue | None:
        path = self._canvas_path(command.name)
        if not self._managed_json_exists(path):
            return None
        record, publication = self._read_verified_canvas_catalog(
            command.name, require_current_head=False
        )
        if record.command_id != command.command_id:
            return None
        if (
            record.description != command.description
            or record.pool_refs
            or record.source != "page_control"
        ):
            raise ValueError("created canvas differs from the original command")
        current = self._current_canvas_head(command.name)
        watermark = self._current_canvas_watermark(command.name)
        if current is None and watermark is not None:
            raise ValueError("canvas immutable watermark exists without its current head")
        if current is not None and (
            current.receipt.claims.command.command_id != command.command_id
            or current.authority_command_kind != command.kind
            or current.authority_command_hash != _command_hash(command)
            or current.publication_receipt_id != publication.receipt_id
            or current.sequence != 1
            or current.previous_head_receipt_id is not None
        ):
            raise ValueError("created canvas conflicts with current head authority")
        if current is not None and watermark is None:
            self._publish_canvas_watermark(current)
        recovered = self._recover_canvas_result(path, command)
        return None if recovered is None else self._created_canvas_result(command, recovered)

    @staticmethod
    def _created_canvas_result(command: CreateCanvas, result: JsonValue) -> dict[str, JsonValue]:
        assert isinstance(result, dict)
        return {**result, "canvas_name": command.name}

    def _save_user_pool(
        self,
        command: SaveUserPool,
        *,
        identity_command: PageControlCommandValue | None = None,
    ) -> JsonValue:
        identity = command if identity_command is None else identity_command
        self._assert_no_formula_pool_name(command.base_name)
        path = self._user_pool_path(command.base_name)
        if self._managed_json_exists(path) and self._read_json(path).get("schema_version") in (
            2,
            3,
        ):
            raise ValueError("versioned pool definition requires matching save command")
        payload = {
            "name": command.base_name,
            "description": command.description,
            "rules": [rule.model_dump(mode="json") for rule in command.rule_calls],
            "include_columns": list(command.include_columns),
            "updated_at": command.requested_at.astimezone(UTC).isoformat(timespec="seconds"),
            "source": command.source,
            "command_id": identity.command_id,
            "command_hash": _command_hash(identity),
        }
        self._atomic_json(path, payload, command_id=identity.command_id)
        return {"path": str(path)}

    def _save_user_pool_v2(self, command: SaveUserPoolV2) -> JsonValue:
        self._assert_no_formula_pool_name(command.base_name)
        from rquant.llm.dispatch import build_rules
        from rquant.llm.schemas import ScreenPlan, Stage
        from rquant.presets import BUILTIN_PRESET_SCREENS, load_user_presets

        path = self._user_pool_path(command.base_name)
        current = self._read_json(path) if self._managed_json_exists(path) else None
        if current is not None and current.get("schema_version") == 3:
            raise ValueError("ranked pool definition requires save_user_pool_v3")
        current_version = None if current is None else canonical_sha256(current)
        if command.expected_version != current_version:
            raise ValueError("pool version conflict: definition changed since it was read")
        if not command.display_name.strip():
            raise ValueError("pool display name is required")
        if command.depends_on is None:
            if command.delay_days != 0:
                raise ValueError("delay_days must be 0 without a parent pool")
        elif not 1 <= command.delay_days <= 252:
            raise ValueError("delay_days must be 1..252 with a parent pool")

        build_rules(
            ScreenPlan(
                trade_date="1900-01-01",
                stages=[Stage(label="saved", rules=list(command.rule_calls))],
                include_columns=list(command.include_columns),
            )
        )
        candidate_name = f"user/{command.base_name}"
        if command.depends_on == candidate_name:
            raise ValueError("pool cannot depend on itself")
        available = dict(BUILTIN_PRESET_SCREENS)
        available.update(load_user_presets(path.parent))
        parent_name = command.depends_on
        visited = {candidate_name}
        while parent_name is not None:
            if parent_name in visited:
                raise ValueError("pool dependency cycle")
            visited.add(parent_name)
            parent = available.get(parent_name)
            if parent is None:
                raise ValueError(f"parent pool does not exist or is invalid: {parent_name}")
            parent_name = parent.depends_on

        payload = {
            "schema_version": 2,
            "name": command.base_name,
            "display_name": command.display_name.strip(),
            "description": command.description,
            "rules": [rule.model_dump(mode="json") for rule in command.rule_calls],
            "include_columns": list(command.include_columns),
            "depends_on": command.depends_on,
            "delay_days": command.delay_days,
            "updated_at": command.requested_at.astimezone(UTC).isoformat(timespec="seconds"),
            "source": "page_control_v2",
            "command_id": command.command_id,
            "command_hash": _command_hash(command),
        }
        self._atomic_json(path, payload, command_id=command.command_id)
        return {"path": str(path), "version": canonical_sha256(payload)}

    def _save_user_pool_v3(self, command: SaveUserPoolV3) -> JsonValue:
        self._assert_no_formula_pool_name(command.base_name)
        from rquant.llm.dispatch import build_rules
        from rquant.llm.schemas import ScreenPlan, Stage
        from rquant.presets import BUILTIN_PRESET_SCREENS, load_user_presets
        from rquant.screen.dynamic_ma import requested_dynamic_ma
        from rquant.screen.dynamic_rsi import requested_dynamic_rsi
        from rquant.screen.loader import FUNDAMENTAL_COLS_MAP, _selected_sources
        from rquant.screen.rules import required_rule_columns

        path = self._user_pool_path(command.base_name)
        current = self._read_json(path) if self._managed_json_exists(path) else None
        current_version = None if current is None else canonical_sha256(current)
        if command.expected_version != current_version:
            raise ValueError("pool version conflict: definition changed since it was read")
        if not command.display_name.strip():
            raise ValueError("pool display name is required")
        if command.depends_on is None:
            if command.delay_days != 0:
                raise ValueError("delay_days must be 0 without a parent pool")
        elif not 1 <= command.delay_days <= 252:
            raise ValueError("delay_days must be 1..252 with a parent pool")

        rules = build_rules(
            ScreenPlan(
                trade_date="1900-01-01",
                stages=[Stage(label="saved", rules=list(command.rule_calls))],
                include_columns=list(command.include_columns),
            )
        )
        columns = required_rule_columns(rules) | frozenset(command.include_columns)
        try:
            from rquant.screen.daily_inputs import validate_daily_columns

            validate_daily_columns(rules, columns)
            _selected_sources(columns, 500)
            fundamentals = set(FUNDAMENTAL_COLS_MAP.values())
            unsupported = any(column.split("[", 1)[0] in fundamentals for column in columns)
            dynamic_rsi = any(
                period not in (6, 14) for period, _offset in requested_dynamic_rsi(columns).values()
            )
            dynamic_ma = bool(requested_dynamic_ma(columns))
        except ValueError as error:
            raise ValueError("pool conditions are not reproducible by the daily writer") from error
        if unsupported or dynamic_rsi or dynamic_ma:
            from rquant.screen.daily_inputs import validate_daily_columns

            validate_daily_columns(rules, columns)
            if self.trusted_daily_writer_capability() is None:
                raise ValueError(
                    "pool conditions are not reproducible by the installed daily writer"
                )

        candidate_name = f"user/{command.base_name}"
        if command.depends_on == candidate_name:
            raise ValueError("pool cannot depend on itself")
        available = dict(BUILTIN_PRESET_SCREENS)
        available.update(load_user_presets(path.parent))
        parent_name = command.depends_on
        visited = {candidate_name}
        while parent_name is not None:
            if parent_name in visited:
                raise ValueError("pool dependency cycle")
            visited.add(parent_name)
            parent = available.get(parent_name)
            if parent is None:
                raise ValueError(f"parent pool does not exist or is invalid: {parent_name}")
            parent_name = parent.depends_on

        payload = self._user_pool_v3_payload(command)
        self._atomic_json(path, payload, command_id=command.command_id)
        return {"path": str(path), "version": canonical_sha256(payload)}

    @staticmethod
    def _user_pool_v3_payload(command: SaveUserPoolV3) -> dict[str, JsonValue]:
        return {
            "schema_version": 3,
            "name": command.base_name,
            "display_name": command.display_name.strip(),
            "description": command.description,
            "rules": [rule.model_dump(mode="json") for rule in command.rule_calls],
            "include_columns": list(command.include_columns),
            "depends_on": command.depends_on,
            "delay_days": command.delay_days,
            "ranking": None if command.ranking is None else command.ranking.model_dump(mode="json"),
            "updated_at": command.requested_at.astimezone(UTC).isoformat(timespec="seconds"),
            "source": "page_control_v3",
            "command_id": command.command_id,
            "command_hash": _command_hash(command),
        }

    def _fork_builtin(self, command: ForkBuiltinPool) -> JsonValue:
        save = self._fork_builtin_save_command(command)
        path = self._user_pool_path(command.target_base_name)
        if self._managed_json_exists(path):
            recovered = self._recover_user_pool_result(save, identity_command=command)
            if recovered is not None:
                if command.canvas_name is not None and command.canvas_name != "__default__":
                    canvas_result = self._recover_canvas_pool_link(command)
                    if canvas_result is None:
                        canvas_result = self._add_pool_to_canvas(
                            command.canvas_name,
                            f"user/{command.target_base_name}",
                            identity_command=command,
                        )
                    if isinstance(recovered, dict):
                        recovered = dict(recovered)
                        recovered["canvas_result"] = canvas_result
                return recovered
            raise FileExistsError(f"user/{command.target_base_name} already exists")
        result = self._save_user_pool(save, identity_command=command)
        if command.canvas_name is not None and command.canvas_name != "__default__":
            canvas_result = self._add_pool_to_canvas(
                command.canvas_name,
                f"user/{command.target_base_name}",
                identity_command=command,
            )
            if isinstance(result, dict):
                result = dict(result)
                result["canvas_result"] = canvas_result
        return result

    def _fork_builtin_save_command(self, command: ForkBuiltinPool) -> SaveUserPool:
        from rquant.presets import PRESET_SCREENS

        if command.builtin_name not in PRESET_SCREENS:
            raise KeyError(f"unknown preset: {command.builtin_name}")
        preset = PRESET_SCREENS[command.builtin_name]
        if not preset.rule_calls:
            raise ValueError("builtin preset has no immutable rule metadata")
        return SaveUserPool(
            command_id=command.command_id,
            requested_at=command.requested_at,
            base_name=command.target_base_name,
            description=f"Fork from builtin/{command.builtin_name}: {preset.description}",
            rule_calls=tuple(preset.rule_calls),
            include_columns=tuple(preset.include_columns),
            source="fork_from_builtin",
            canvas_name=command.canvas_name,
        )

    def _add_pool_to_canvas(
        self,
        canvas_name: str,
        pool_name: str,
        *,
        identity_command: PageControlCommandValue | None = None,
    ) -> JsonValue:
        current, _publication = self._read_verified_canvas_catalog(canvas_name)
        pool_refs = list(current.pool_refs)
        if pool_name not in pool_refs:
            pool_refs.append(pool_name)
        save = SaveCanvas(
            command_id=f"canvas-{canonical_sha256([canvas_name, pool_name])}",
            requested_at=datetime.now(UTC),
            name=canvas_name,
            description=current.description,
            pool_refs=tuple(str(value) for value in pool_refs),
            source="canvas_edit",
        )
        return self._save_canvas(save, identity_command=identity_command)

    def _add_verified_pool_to_canvas(self, command: AddPoolToCanvas) -> JsonValue:
        base_name = command.pool_name.removeprefix("user/")
        path = self._user_pool_path(base_name)
        if not self._managed_json_exists(path):
            raise ValueError("pool definition is unavailable")
        current = self._read_json(path)
        versioned_source = (current.get("schema_version"), current.get("source")) in {
            (2, "page_control_v2"),
            (3, "page_control_v3"),
        } and (current.get("schema_version") != 3 or "ranking" in current)
        if (
            not versioned_source
            or current.get("name") != base_name
            or canonical_sha256(current) != command.expected_pool_version
        ):
            raise ValueError("pool version conflict: definition changed since save")
        if current["schema_version"] == 3 and current.get("ranking") is not None:
            PoolRankingPlan.model_validate(current.get("ranking"))
        result = self._add_pool_to_canvas(
            command.canvas_name,
            command.pool_name,
            identity_command=command,
        )
        return self._attach_result(command, result)

    @staticmethod
    def _attach_result(command: AddPoolToCanvas, result: JsonValue) -> dict[str, JsonValue]:
        assert isinstance(result, dict)
        return {
            **result,
            "canvas_name": command.canvas_name,
            "pool_name": command.pool_name,
            "pool_version": command.expected_pool_version,
        }

    def _recover_canvas_result(
        self,
        path: Path,
        identity_command: PageControlCommandValue,
    ) -> JsonValue | None:
        if not self._managed_json_exists(path):
            return None
        try:
            record, publication = self._read_verified_canvas_catalog(
                path.stem,
                expected_head=None,
                require_current_head=False,
            )
        except ValueError:
            raise
        if record.command_id != identity_command.command_id:
            return None
        if publication.claims.command.command_id != identity_command.command_id:
            return None
        self._publish_canvas_head(
            identity_command=identity_command,
            canvas_name=record.name,
            state="active",
            publication_receipt_id=publication.receipt_id,
        )
        return self._canvas_publication_result(path, publication)

    def _recover_user_pool_result(
        self,
        command: SaveUserPool,
        *,
        identity_command: PageControlCommandValue,
    ) -> JsonValue | None:
        path = self._user_pool_path(command.base_name)
        if not self._managed_json_exists(path):
            return None
        raw = self._read_json(path)
        if raw.get("command_id") != identity_command.command_id:
            return None
        if raw.get("command_hash") != _command_hash(identity_command):
            return None
        return {"path": str(path)}

    def _recover_user_pool_v2_result(self, command: SaveUserPoolV2) -> JsonValue | None:
        path = self._user_pool_path(command.base_name)
        if not self._managed_json_exists(path):
            return None
        raw = self._read_json(path)
        if raw.get("command_id") != command.command_id:
            return None
        if raw.get("command_hash") != _command_hash(command):
            return None
        return {"path": str(path), "version": canonical_sha256(raw)}

    def _recover_user_pool_v3_result(self, command: SaveUserPoolV3) -> JsonValue | None:
        path = self._user_pool_path(command.base_name)
        if not self._managed_json_exists(path):
            return None
        raw = self._read_json(path)
        if raw != self._user_pool_v3_payload(command):
            return None
        return {"path": str(path), "version": canonical_sha256(raw)}

    def _recover_canvas_pool_link(
        self,
        command: SaveUserPool | ForkBuiltinPool,
    ) -> JsonValue | None:
        canvas_name = command.canvas_name
        if canvas_name is None or canvas_name == "__default__":
            return None
        path = self._canvas_path(canvas_name)
        recovered = self._recover_canvas_result(path, command)
        if recovered is None:
            return None
        raw = self._read_json(path)
        base_name = (
            command.base_name if isinstance(command, SaveUserPool) else command.target_base_name
        )
        if f"user/{base_name}" not in raw.get("pool_refs", []):
            return None
        return recovered

    def _read_verified_canvas_catalog(
        self,
        canvas_name: str,
        *,
        expected_head: CanvasCurrentHead | None = None,
        require_current_head: bool = True,
    ) -> tuple[CanvasPublicationCatalogRecord, CanvasPublicationReceipt]:
        path = self._canvas_path(canvas_name)
        try:
            raw = self._read_json(path)
            record = CanvasPublicationCatalogRecord.model_validate(raw)
            publication = self._canvas_publication_receipt_store().read(
                record.publication_receipt_id
            )
            _signer, keyring = self._require_canvas_publication_authority()
            if not keyring.verify_publication_receipt(publication, require_active=True):
                raise ValueError("CanvasPublicationReceipt active signature verification failed")
            if publication.claims.catalog_record != record:
                raise ValueError("CanvasPublicationReceipt catalog semantics do not match")
            if publication.claims.command.name != canvas_name:
                raise ValueError("CanvasPublicationReceipt canvas name does not match")
            if require_current_head:
                head = expected_head or self._current_canvas_head(canvas_name)
                if (
                    head is None
                    or head.state != "active"
                    or head.publication_receipt_id != publication.receipt_id
                ):
                    raise ValueError(
                        "CanvasPublicationReceipt catalog semantics do not match current head"
                    )
                self._assert_canvas_head_matches_latest_authority(canvas_name, head)
            return record, publication
        except Exception as exc:
            if isinstance(exc, ValueError) and "catalog semantics" in str(exc):
                raise
            raise ValueError(
                f"CanvasPublicationReceipt catalog semantics cannot be verified: {exc}"
            ) from exc

    def _canvas_head_root(self, canvas_name: str) -> Path:
        return (
            self.data_dir
            / "canvas-publication-heads"
            / _validated_name(
                canvas_name,
                label="canvas name",
            )
        )

    def _canvas_watermark_root(self, canvas_name: str) -> Path:
        return (
            self.data_dir
            / _CANVAS_WATERMARK_DIRECTORY
            / _validated_name(
                canvas_name,
                label="canvas name",
            )
        )

    def _current_canvas_head(self, canvas_name: str) -> CanvasCurrentHead | None:
        _signer, keyring = self._require_canvas_publication_authority()
        canvas_root = self._canvas_head_root(canvas_name)
        return read_canvas_current_head(
            self.data_dir / "canvas-publication-heads",
            canvas_name,
            keyring,
            directory_descriptor=self._bound_effect_directory_descriptor(canvas_root),
        )

    def _current_canvas_watermark(self, canvas_name: str) -> CanvasCurrentHead | None:
        _signer, keyring = self._require_canvas_publication_authority()
        canvas_root = self._canvas_watermark_root(canvas_name)
        return read_canvas_current_head(
            self.data_dir / _CANVAS_WATERMARK_DIRECTORY,
            canvas_name,
            keyring,
            directory_descriptor=self._bound_effect_directory_descriptor(canvas_root),
        )

    @staticmethod
    def _assert_canvas_watermark_matches_head(
        head: CanvasCurrentHead | None,
        watermark: CanvasCurrentHead | None,
    ) -> None:
        if head is None and watermark is None:
            return
        if (
            head is None
            or watermark is None
            or head.receipt.receipt_id != watermark.receipt.receipt_id
            or head.sequence != watermark.sequence
            or head.state != watermark.state
            or head.publication_receipt_id != watermark.publication_receipt_id
        ):
            raise ValueError("canvas current head does not match immutable watermark authority")

    def _assert_canvas_head_matches_latest_authority(
        self,
        canvas_name: str,
        head: CanvasCurrentHead | None,
    ) -> None:
        latest = self.outbox.latest_succeeded_canvas_mutation(canvas_name)
        if latest is None:
            if head is None:
                return
            raise ValueError("canvas current head lacks authoritative PageControl command history")
        if (
            head is None
            or head.receipt.claims.command.command_id != latest.command_id
            or head.authority_command_kind != latest.kind
            or head.authority_command_hash != _command_hash(latest)
        ):
            raise ValueError(
                "canvas current head does not match latest PageControl command authority"
            )

    def _publish_canvas_head(
        self,
        *,
        identity_command: PageControlCommandValue,
        canvas_name: str,
        state: Literal["active", "deleted"],
        publication_receipt_id: str | None,
    ) -> CanvasCurrentHead:
        signer, keyring = self._require_canvas_publication_authority()
        if getattr(signer, "key_id", None) != keyring.active_key_id:
            raise ValueError("CanvasPublicationReceipt signer key must be active")
        current = self._current_canvas_head(canvas_name)
        watermark = self._current_canvas_watermark(canvas_name)
        self._assert_canvas_watermark_matches_head(current, watermark)
        authority_hash = _command_hash(identity_command)
        if current is not None and current.receipt.claims.command.command_id == (
            identity_command.command_id
        ):
            if (
                current.state != state
                or current.publication_receipt_id != publication_receipt_id
                or current.authority_command_kind != identity_command.kind
                or current.authority_command_hash != authority_hash
            ):
                raise ValueError("canvas current head command identity conflicts")
            self._publish_canvas_watermark(current)
            return current
        if (
            current is not None
            and identity_command.requested_at < current.receipt.claims.command.requested_at
        ):
            raise ValueError("canvas current head is newer than the requested update")
        payload = {
            "authority_command_hash": authority_hash,
            "authority_command_kind": identity_command.kind,
            "canvas_name": canvas_name,
            "contract": _CANVAS_HEAD_CONTRACT,
            "previous_head_receipt_id": (None if current is None else current.receipt.receipt_id),
            "publication_receipt_id": publication_receipt_id,
            "sequence": 1 if current is None else current.sequence + 1,
            "state": state,
        }
        description = json.dumps(
            payload,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        )
        head_command = CanvasPublicationCommand(
            command_id=identity_command.command_id,
            requested_at=identity_command.requested_at,
            name=canvas_name,
            description=description,
            pool_refs=(),
            source=_CANVAS_HEAD_SOURCE,
        )
        head_receipt = signer.issue_publication(
            build_canvas_publication_claims(
                command=head_command,
                catalog_created_at=identity_command.requested_at,
                catalog_updated_at=identity_command.requested_at,
                consumer_service_id=self.consumer_service_id,
                consumer_instance_id=self.consumer_id,
            )
        )
        if not keyring.verify_publication_receipt(head_receipt, require_active=True):
            raise ValueError("canvas current head active signature verification failed")
        head_root = self._canvas_head_root(canvas_name)
        CanvasPublicationReceiptStore(
            head_root,
            directory_descriptor=self._bound_effect_directory_descriptor(head_root),
        ).write_immutable(head_receipt)
        published = self._current_canvas_head(canvas_name)
        if published is None or published.receipt.receipt_id != head_receipt.receipt_id:
            raise ValueError("canvas current head publication did not become authoritative")
        self._publish_canvas_watermark(published)
        return published

    def _publish_canvas_watermark(self, head: CanvasCurrentHead) -> None:
        current = self._current_canvas_watermark(head.receipt.claims.command.name)
        if current is not None and current.receipt.receipt_id == head.receipt.receipt_id:
            return
        expected_sequence = 1 if current is None else current.sequence + 1
        expected_previous = None if current is None else current.receipt.receipt_id
        if head.sequence != expected_sequence or head.previous_head_receipt_id != expected_previous:
            raise ValueError("canvas immutable watermark would fork or roll back")
        watermark_root = self._canvas_watermark_root(head.receipt.claims.command.name)
        CanvasPublicationReceiptStore(
            watermark_root,
            directory_descriptor=self._bound_effect_directory_descriptor(watermark_root),
        ).write_immutable(head.receipt)
        published = self._current_canvas_watermark(head.receipt.claims.command.name)
        if published is None or published.receipt.receipt_id != head.receipt.receipt_id:
            raise ValueError("canvas immutable watermark publication failed")

    def _delete_canvas(self, command: DeleteCanvas) -> JsonValue:
        path = self._canvas_path(command.name)
        catalog_exists = self._managed_json_exists(path)
        current = self._current_canvas_head(command.name)
        if catalog_exists:
            self._read_verified_canvas_catalog(command.name, expected_head=current)
        else:
            self._assert_canvas_head_matches_latest_authority(command.name, current)
            if current is not None and current.state == "active":
                raise ValueError("canvas current head is active but catalog is missing")
        self._publish_canvas_head(
            identity_command=command,
            canvas_name=command.name,
            state="deleted",
            publication_receipt_id=None,
        )
        self._delete(path)
        return {"deleted": True}

    def _recover_delete_canvas(self, command: DeleteCanvas) -> JsonValue | None:
        current = self._current_canvas_head(command.name)
        if (
            current is None
            or current.receipt.claims.command.command_id != command.command_id
            or current.state != "deleted"
            or current.authority_command_kind != command.kind
            or current.authority_command_hash != _command_hash(command)
        ):
            return None
        self._publish_canvas_watermark(current)
        path = self._canvas_path(command.name)
        if self._managed_json_exists(path):
            self._delete(path)
        return {"deleted": True}

    def _require_canvas_publication_authority(
        self,
    ) -> tuple[CanvasPublicationSigner, CanvasPublicationKeyring]:
        if self.canvas_publication_signer is None or self.canvas_publication_keyring is None:
            raise RuntimeError("CanvasPublicationReceipt signer and public keyring are required")
        return self.canvas_publication_signer, self.canvas_publication_keyring

    def _canvas_publication_receipt_store(self) -> CanvasPublicationReceiptStore:
        root = self.data_dir / "canvas-publication-receipts"
        return CanvasPublicationReceiptStore(
            root,
            directory_descriptor=self._bound_effect_directory_descriptor(root),
        )

    @staticmethod
    def _canvas_publication_command(
        command: SaveCanvas,
        *,
        identity_command: PageControlCommandValue,
    ) -> CanvasPublicationCommand:
        return CanvasPublicationCommand(
            command_id=identity_command.command_id,
            requested_at=identity_command.requested_at,
            name=command.name,
            description=command.description,
            pool_refs=command.pool_refs,
            source=command.source,
        )

    @staticmethod
    def _canvas_publication_result(
        path: Path,
        publication: CanvasPublicationReceipt,
    ) -> dict[str, object]:
        claims = publication.claims
        return {
            "path": str(path),
            "command_hash": claims.command_hash,
            "source_identity_hash": claims.source_identity_hash,
            "record_hash": claims.catalog_record_hash,
            "publication_generation_id": claims.generation_id,
            "publication_receipt_id": publication.receipt_id,
            "publication_receipt_hash": publication.receipt_hash,
            "publication_effect_id": claims.effect_id,
            "publication_key_id": publication.key_id,
            "publication_receipt_path": str(
                path.parent.parent
                / "canvas-publication-receipts"
                / f"{publication.receipt_id}.json"
            ),
        }

    def _recover_delete_result(self, path: Path) -> JsonValue | None:
        if not self._managed_json_exists(path):
            return {"deleted": True}
        return None

    def _recover_lab_exports(self, command: InitializeLabExports) -> JsonValue | None:
        requested = (
            Path(os.path.abspath(command.export_root)),
            Path(os.path.abspath(command.runtime_root)),
        )
        if any(path not in self.allowed_lab_export_roots for path in requested):
            return None
        descriptors: list[int] = []
        try:
            for path in requested:
                if path.exists():
                    descriptor = _open_existing_managed_directory(path)
                else:
                    descriptor = _open_or_create_managed_directory(path)
                _verify_open_directory_matches_path(path, descriptor)
                descriptors.append(descriptor)
            for descriptor in descriptors:
                _fsync_descriptor(descriptor)
            for path, descriptor in zip(requested, descriptors, strict=True):
                _verify_open_directory_matches_path(path, descriptor)
        finally:
            for descriptor in descriptors:
                os.close(descriptor)
        return {"paths": [str(path) for path in requested]}

    def _append_nl_log(self, command: AppendNlQueryLog) -> JsonValue:
        path = self.log_dir / "nl_queries.jsonl"
        record = {
            "command_id": command.command_id,
            "ts": command.requested_at.astimezone(UTC).isoformat(timespec="seconds"),
            "query": command.query,
            "plan": command.plan,
            "outcome": command.outcome,
            "error": command.error,
        }
        _append_managed_jsonl(path, record, command_id=command.command_id)
        return {"path": str(path)}

    def _initialize_lab_exports(self, command: InitializeLabExports) -> JsonValue:
        requested = (
            Path(os.path.abspath(command.export_root)),
            Path(os.path.abspath(command.runtime_root)),
        )
        if any(path not in self.allowed_lab_export_roots for path in requested):
            raise ValueError("Lab export directory is not allowlisted")
        descriptors: list[int] = []
        try:
            for path in requested:
                path.mkdir(
                    parents=True,
                    mode=_PRIVATE_DIRECTORY_MODE,
                    exist_ok=True,
                )
                descriptor = _open_or_create_managed_directory(path)
                _verify_open_directory_matches_path(path, descriptor)
                descriptors.append(descriptor)
            for path, descriptor in zip(requested, descriptors, strict=True):
                _verify_open_directory_matches_path(path, descriptor)
                _fsync_descriptor(descriptor)
                _verify_open_directory_matches_path(path, descriptor)
        finally:
            for descriptor in descriptors:
                os.close(descriptor)
        return {"paths": [str(path) for path in requested]}

    def _canvas_path(self, name: str) -> Path:
        return self.data_dir / "canvases" / f"{_validated_name(name, label='canvas name')}.json"

    def _user_pool_path(self, name: str) -> Path:
        return self.data_dir / "user_presets" / f"{_validated_name(name, label='pool name')}.json"

    def _assert_no_formula_pool_name(self, name: str) -> None:
        validated = _validated_name(name, label="pool name")
        path = self.data_dir / "formula_pools" / f"{validated}.json"
        if self._managed_json_exists(path):
            raise FileExistsError("formula pool already uses this user/ name")

    @staticmethod
    def _read_json(path: Path) -> dict[str, object]:
        descriptor = PageControlConsumer._open_effect_directory(
            path.parent,
            create=False,
        )
        value = json.loads(
            _read_managed_file(path, directory_descriptor=descriptor).decode("utf-8")
        )
        if not isinstance(value, dict):
            raise ValueError(f"expected JSON object: {path}")
        return value

    @staticmethod
    def _delete(path: Path) -> bool:
        if not PageControlConsumer._managed_json_exists(path):
            return False
        descriptor = PageControlConsumer._open_effect_directory(
            path.parent,
            create=False,
        )
        try:
            _verify_open_directory_matches_path(path.parent, descriptor)
            os.unlink(path.name, dir_fd=descriptor)
            _verify_open_directory_matches_path(path.parent, descriptor)
            _fsync_descriptor(descriptor)
            _verify_open_directory_matches_path(path.parent, descriptor)
            return True
        finally:
            os.close(descriptor)

    @staticmethod
    def _atomic_json(path: Path, payload: object, *, command_id: str) -> None:
        descriptor = PageControlConsumer._open_effect_directory(
            path.parent,
            create=True,
        )
        temp_name = f".{path.name}.{canonical_sha256(command_id)[:12]}.{uuid4().hex}.tmp"
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        file_descriptor: int | None = None
        try:
            _verify_open_directory_matches_path(path.parent, descriptor)
            file_descriptor = os.open(
                temp_name,
                flags,
                _PRIVATE_FILE_MODE,
                dir_fd=descriptor,
            )
            payload_bytes = (json.dumps(payload, ensure_ascii=False, indent=2) + "\n").encode(
                "utf-8"
            )
            with os.fdopen(file_descriptor, "wb", closefd=False) as handle:
                handle.write(payload_bytes)
                handle.flush()
            os.fsync(file_descriptor)
            os.fchmod(file_descriptor, _PRIVATE_FILE_MODE)
            _verify_open_directory_matches_path(path.parent, descriptor)
            os.replace(
                temp_name,
                path.name,
                src_dir_fd=descriptor,
                dst_dir_fd=descriptor,
            )
            _verify_open_directory_matches_path(path.parent, descriptor)
            _fsync_descriptor(descriptor)
            _verify_open_directory_matches_path(path.parent, descriptor)
        except Exception:
            with suppress(FileNotFoundError):
                os.unlink(temp_name, dir_fd=descriptor)
            raise
        finally:
            if file_descriptor is not None:
                os.close(file_descriptor)
            os.close(descriptor)

    @staticmethod
    def _managed_json_exists(path: Path) -> bool:
        try:
            descriptor = PageControlConsumer._open_effect_directory(
                path.parent,
                create=False,
            )
        except FileNotFoundError:
            return False
        try:
            _verify_open_directory_matches_path(path.parent, descriptor)
            try:
                observed = os.stat(path.name, dir_fd=descriptor, follow_symlinks=False)
            except FileNotFoundError:
                return False
            if stat.S_ISLNK(observed.st_mode):
                raise ValueError(f"managed JSON file cannot be a symlink: {path}")
            if not stat.S_ISREG(observed.st_mode):
                raise ValueError(f"managed JSON path is not a regular file: {path}")
            _verify_open_directory_matches_path(path.parent, descriptor)
            return True
        finally:
            os.close(descriptor)

    def drain_task_control_command(self, command: OwnedTaskControl) -> tuple[PageControlReceipt, ...]:
        with _PageControlExecutionMutex(self._consumer_mutex_path()) as acquired:
            if not acquired:
                return ()
            claims = self.outbox.claim_records(limit=1, owner_id=self.consumer_id, lease_seconds=self.lease_seconds,
                now=self.clock(), target_command_id=command.command_id)
            if not claims:
                return ()
            if claims[0].command != command:
                raise PageControlCommandConflictError("task original command changed before claim")
            return self._complete_claims(claims)

    def _task_control_backend(self) -> TaskControlPageControlBackend:
        if self.task_control_backend is None:
            raise RuntimeError("task control backend is unavailable")
        return self.task_control_backend


class PageControlService:
    """Synchronous service boundary backed by the durable control outbox."""

    def __init__(
        self,
        *,
        outbox: PageControlOutbox,
        consumer: PageControlConsumer,
        ai_assistance: AIAssistanceOwner | None = None,
        collaboration: PageControlRoleAuthority | None = None,
    ) -> None:
        self.outbox = outbox
        self.consumer = consumer
        self._defer_minute_commands = False
        if ai_assistance is not None and ai_assistance.outbox is not outbox:
            raise ValueError("AI owner must share the original PageControl SQLite")
        self.ai_assistance = ai_assistance
        self.collaboration = collaboration or PageControlRoleAuthority()
        self.collaboration.bind_outbox(outbox.path)
        self.outbox.collaboration = self.collaboration
        from rquant.minute_backtest_commands import MinuteCommandWriter
        if type(consumer.minute_backend) is MinuteCommandWriter:
            consumer.minute_backend.bind_owner_authority(self)
        if consumer.minute_native_report_backend is not None:
            from rquant.minute_backtest_native_report_runtime import MinuteNativeReportCommandWriter

            if type(consumer.minute_native_report_backend) is not MinuteNativeReportCommandWriter:
                raise TypeError("native minute reports require their original concrete command writer")
            consumer.minute_native_report_backend.bind_owner_authority(self)
        if consumer.minute_study_backend is not None:
            from rquant.minute_backtest_parameter_study_journal import MinuteParameterStudyCommandWriter

            if type(consumer.minute_study_backend) is not MinuteParameterStudyCommandWriter:
                raise TypeError("minute study requires its original concrete command writer")
            consumer.minute_study_backend.bind_owner_authority(self)
        if consumer.strategy_promotion_backend is not None:
            backend = consumer._strategy_promotion_backend()
            if backend.domain.roles is not self.collaboration or self.collaboration.mode != "enforced":
                raise ValueError("manual promotion must use the same original current-role authority")

    def submit(self, command: PageControlCommandValue) -> PageControlReceipt:
        if isinstance(command, PROMOTION_PUBLIC_TYPES):
            raise ValueError("manual promotion requires trusted private submission")
        if self.collaboration.mode == "enforced" and _TRUSTED_COLLABORATION_ACTOR.get() is None:
            raise PermissionError("command requires the original trusted private ingress")
        if type(command) is SetUserRoleCommand:
            raise PermissionError("role command requires its original private confirmation")
        if isinstance(
            command, (SaveStrategyTemplate, ArchiveStrategyTemplate, RunStrategyTemplate)
        ):
            raise ValueError("strategy authoring requires trusted submission")
        if isinstance(command, SaveResearchQuery):
            raise ValueError("research queries require trusted submission")
        if isinstance(command, (AddWatchlistItem, RemoveWatchlistItem)):
            raise ValueError("watchlist commands require trusted submission")
        if isinstance(
            command, (SavePriceAlertRule, SetPriceAlertRuleEnabled, DeletePriceAlertRule)
        ):
            raise ValueError("price rule commands require trusted submission")
        if isinstance(
            command, (SaveFactorDefinition, ArchiveFactor, SubmitFactorRun, SetFactorTracked)
        ):
            raise ValueError("factor commands require trusted submission")
        if isinstance(command, AckAlert):
            receipt = self.lookup_ack_command(command)
            if receipt is None:
                raise ValueError("ack_alert requires verified Serving eligibility")
        else:
            receipt = self.outbox.enqueue(command)
        if type(command) is SubmitMinuteParameterStudy or (self._defer_minute_commands
                and isinstance(command, (SubmitMinuteReplay, ExportMinuteReplayZip))):
            return receipt
        return self._settle(command, receipt)

    def submit_authorized(self, command: PageControlCommandValue, proof: CommandAuthorization) -> PageControlReceipt:
        verified = self.collaboration.verify_authorization(proof, command.model_dump(mode="json"))
        token = _TRUSTED_COLLABORATION_ACTOR.set(verified.actor_id)
        try:
            return self.submit(command)
        finally:
            _TRUSTED_COLLABORATION_ACTOR.reset(token)

    def submit_role(self, command: SetUserRoleCommand, *, authenticated_actor_id: str,
                    issuance_proof: str) -> PageControlReceipt:
        self.collaboration.require_command(authenticated_actor_id, command.kind)
        audit = self.outbox.audit(command.command_id)
        if audit is not None:
            require_original_role_command(command, command_id=audit.command_id,
                command_hash=audit.command_hash, payload_json=self.outbox.original_command_bytes(command.command_id))
            if self.outbox.trusted_command_actor(command.command_id, audit.command_hash) != authenticated_actor_id:
                raise PermissionError("original role command actor differs")
            receipt = self.outbox.receipt(command.command_id)
            assert receipt is not None
            return self._settle(command, receipt)
        self.collaboration.validate_preparation(command,
            authenticated_actor_id=authenticated_actor_id, issuance_proof=issuance_proof)
        token = _TRUSTED_COLLABORATION_ACTOR.set(authenticated_actor_id)
        try:
            return self._settle(command, self.outbox._enqueue(command))
        finally:
            _TRUSTED_COLLABORATION_ACTOR.reset(token)

    def lookup_role(self, command: SetUserRoleCommand, *, authenticated_actor_id: str) -> PageControlReceipt | None:
        self.collaboration.require_command(authenticated_actor_id, command.kind)
        audit = self.outbox.audit(command.command_id)
        if audit is None:
            return None
        require_original_role_command(command, command_id=audit.command_id,
            command_hash=audit.command_hash, payload_json=self.outbox.original_command_bytes(command.command_id))
        if self.outbox.trusted_command_actor(command.command_id, audit.command_hash) != authenticated_actor_id:
            raise PermissionError("original role command actor differs")
        return self.outbox.receipt(command.command_id)

    def collaboration_request(self, message: object) -> object:
        """Called only by the original FactorDefinitionAdmission peer-UID handler."""
        from rquant.command_audit_projection import read_command_audit
        from rquant.web.models.collaboration import CollaborationMe, CollaborationPrivateRequest, RoleLookupData

        if type(message) is not CollaborationPrivateRequest:
            raise ValueError("exact private collaboration request required")
        actor = message.authenticated_actor_id
        operation = message.operation
        if operation in {"me", "users"}:
            # Keep the complete read coherent while the original CAS writer waits.
            with self.collaboration.locked(read_only=True):
                role = self.collaboration.current_role(actor)
                if operation == "users":
                    self.collaboration.require_operation(actor, "GET", "/api/v1/collaboration/users")
                    return self.collaboration.read_state()
                state = self.collaboration.read_state()
                return CollaborationMe(available=True, mode="enforced", username=actor, role=role,
                    revision=state.revision, state_sha256=state.content_sha256,
                    can_manage_users=role == "admin", can_research=role in {"admin", "researcher"}, can_read_audit=True)
        self.collaboration.current_role(actor)
        if operation == "prepare_role":
            assert message.role_request is not None
            return self.collaboration.prepare_role(message.role_request, authenticated_actor_id=actor)
        if operation == "submit_role":
            assert message.role_submission is not None
            return self.submit_role(message.role_submission.command, authenticated_actor_id=actor,
                issuance_proof=message.role_submission.issuance_proof)
        if operation == "lookup_role":
            assert message.role_lookup is not None
            receipt = self.lookup_role(message.role_lookup.command, authenticated_actor_id=actor)
            return RoleLookupData(found=receipt is not None,
                receipt=None if receipt is None else receipt.model_dump(mode="json"))
        if operation == "authorize_command":
            assert message.original_command is not None
            command = parse_page_control_command(message.original_command)
            # Canonical original body, including defaults, must be forwarded unchanged.
            return self.collaboration.issue_authorization(actor, command.model_dump(mode="json"))
        if operation == "audit":
            assert message.audit_query is not None
            self.collaboration.require_operation(actor, "GET", "/api/v1/collaboration/audit")
            state = self.collaboration.read_state()
            with self.outbox._connect() as connection:
                connection.execute("PRAGMA query_only = ON")
                connection.execute("BEGIN")
                metadata = connection.execute("""
                    SELECT length(CAST(command_id AS BLOB)),length(CAST(command_hash AS BLOB)),
                        length(CAST(status AS BLOB)),length(CAST(completed_at AS BLOB)),
                        length(CAST(authorization_json AS BLOB))
                    FROM page_control_command ORDER BY rowid DESC LIMIT 4097
                """).fetchall()
                if len(metadata) > 4096:
                    raise ValueError("audit source exceeds original command capacity")
                total = 0
                for item in metadata:
                    sizes = tuple(int(value or 0) for value in item)
                    total += sum(sizes)
                    if any(size > 128 for size in sizes[:4]) or sizes[4] > 4096 or total > 4 * 1024 * 1024:
                        raise ValueError("audit source metadata exceeds read capacity")
                facts = connection.execute("SELECT command_id,command_hash,status,completed_at,authorization_json FROM page_control_command ORDER BY rowid DESC LIMIT 4097").fetchall()
                generation = canonical_sha256(tuple(tuple(row) for row in facts))
                def actual_actor(command_id: str, command_hash: str) -> str | None:
                    row = self.outbox._bounded_command_row(connection, command_id)
                    if row is None or row["command_hash"] != command_hash:
                        return None
                    proof = self.outbox._row_authorization(row)
                    return None if proof is None else proof.actor_id
                page = read_command_audit(connection, state=state, viewer_id=actor,
                    source_generation=generation, query=message.audit_query, actor_resolver=actual_actor)
                connection.rollback()
            after = self.collaboration.read_state()
            if after != state:
                raise PermissionError("current audit role generation changed")
            return page
        if operation == "result_owner":
            assert message.owner_query is not None
            return self._trusted_result_owner(message.owner_query, authenticated_actor_id=actor)
        if operation == "study_journal":
            assert message.study_query is not None
            return self._trusted_minute_study_journal(message.study_query, authenticated_actor_id=actor)
        if operation == "minute_report_owner":
            assert message.minute_owner_query is not None
            return self._trusted_minute_report_owner(message.minute_owner_query, authenticated_actor_id=actor)
        raise PermissionError("unknown collaboration operation")

    def _trusted_minute_report_owner(self, query: object, *, authenticated_actor_id: str) -> object:
        from rquant.experiment_platform_projection import ExperimentPrivateResultAuthority
        from rquant.minute_backtest_commands import MinuteCommandWriter
        from rquant.minute_backtest_installation import load_minute_replay_installation
        from rquant.minute_experiment_result_owner import MinuteExperimentResultOwner
        from rquant.web.models.collaboration import MinuteReportOwnerQuery, ResultOwnerQuery

        if type(query) is not MinuteReportOwnerQuery:
            raise ValueError("exact minute report ownership query required")
        with self.collaboration.locked(read_only=True):
            self.collaboration.require_operation(authenticated_actor_id, "GET",
                "/api/v1/backtests/minute-runtime/runs/{job_id}/report.html")
            if query.artifact is not None and query.artifact.private_owner != authenticated_actor_id:
                raise PermissionError("complete minute artifact belongs to another original actor")
            try:
                return self._trusted_result_owner(ResultOwnerQuery(domain="minute", job_id=query.job_id,
                    spec_hash=query.spec_hash), authenticated_actor_id=authenticated_actor_id)
            except LookupError:
                # A missing old submission is distinct from a failed old authorization.
                pass
            native = self.consumer.minute_native_report_backend
            if native is not None:
                from rquant.minute_backtest_native_report_runtime import MinuteNativeReportCommandWriter

                if type(native) is not MinuteNativeReportCommandWriter or native.owner_authority is not self:
                    raise PermissionError("native minute report requires the same original bound backend")
                port = native.runtime.replay_reader(UUID(query.job_id))
                owner = MinuteExperimentResultOwner(private_authority=port.private_authority,
                    jobs=port.reader, roles=self.collaboration)
                provenance = owner.read(UUID(query.job_id), authenticated_actor_id=authenticated_actor_id,
                    expected_spec_hash=query.spec_hash)
                result = provenance if query.artifact is None else owner.bind_sealed(provenance, query.artifact,
                    authenticated_actor_id=authenticated_actor_id)
                native.runtime.verify_current()
                return result
            backend = self.consumer.minute_backend
            if type(backend) is not MinuteCommandWriter or backend.owner_authority is not self:
                raise PermissionError("family minute report requires the same original installed backend")
            installed = backend.installation
            # Legitimate first Lab reads can establish WAL coordination. Capture
            # the readonly Registry entrance only after this original preflight.
            installed.reader.get_artifact_preview_authority(UUID(query.job_id))
            fresh = load_minute_replay_installation(installed.reference.path,
                expected_code_sha=installed.profile.code_sha, clock=installed.clock)
            if (fresh.profile, fresh.reference, fresh.authority) != (
                installed.profile, installed.reference, installed.authority):
                raise PermissionError("complete installed minute authority changed before family report")
            owner = MinuteExperimentResultOwner(private_authority=ExperimentPrivateResultAuthority(fresh.experiments),
                jobs=fresh.reader, roles=self.collaboration)
            provenance = owner.read(UUID(query.job_id), authenticated_actor_id=authenticated_actor_id,
                expected_spec_hash=query.spec_hash)
            result = provenance if query.artifact is None else owner.bind_sealed(provenance, query.artifact,
                authenticated_actor_id=authenticated_actor_id)
            fresh.verify_current()
            installed.verify_current()
            return result

    def _trusted_minute_study_journal(self, query: object, *, authenticated_actor_id: str) -> object:
        from rquant.strict_json import strict_json_loads
        from rquant.web.models.collaboration import MinuteStudyJournalFact, MinuteStudyJournalQuery

        if type(query) is not MinuteStudyJournalQuery:
            raise ValueError("exact original study journal query required")
        command_id = str(query.command_id)
        with self.collaboration.locked(read_only=True):
            self.collaboration.require_operation(authenticated_actor_id, "GET",
                "/api/v1/backtests/minute-runtime/studies/{command_id}")
            self.collaboration.require_outbox_identity()
            with self.outbox._connect() as connection:
                connection.execute("PRAGMA query_only = ON")
                connection.execute("BEGIN")
                row = self.outbox._bounded_command_row(connection, command_id)
                if row is None or row["command_kind"] != "submit_minute_parameter_study":
                    raise LookupError("original study parent is unavailable")
                command = _COMMAND_ADAPTER.validate_json(row["payload_json"])
                proof = self.outbox._row_authorization(row)
                if (type(command) is not SubmitMinuteParameterStudy or proof is None
                        or proof.actor_id != authenticated_actor_id or command.actor_id != authenticated_actor_id
                        or command.command_id != command_id or _command_hash(command) != row["command_hash"]):
                    raise PermissionError("original study authenticated parent differs")
                sizes = connection.execute("""
                    SELECT length(CAST(c.result_json AS BLOB)),length(CAST(e.original_admission_json AS BLOB))
                    FROM page_control_command c LEFT JOIN page_control_effect e ON e.command_id=c.command_id
                    WHERE c.command_id=?
                """, (command_id,)).fetchone()
                if sizes is None or any(int(value or 0) > 1024 * 1024 for value in sizes):
                    raise ValueError("original study journal exceeds control capacity")
                fact = connection.execute("""
                    SELECT c.status,c.enqueued_at,c.completed_at,c.result_json,
                        e.command_hash AS effect_hash,e.effect_kind,
                        CASE WHEN ? THEN e.original_admission_json ELSE NULL END AS admission_json
                    FROM page_control_command c LEFT JOIN page_control_effect e ON e.command_id=c.command_id
                    WHERE c.command_id=?
                """, (query.include_admission, command_id)).fetchone()
                if fact is None or (fact["effect_hash"] is not None and
                        (fact["effect_hash"], fact["effect_kind"]) != (row["command_hash"], command.kind)):
                    raise PermissionError("original study effect has another complete parent")
                admission = fact["admission_json"]
                if admission is not None:
                    strict_json_loads(admission)
                value = MinuteStudyJournalFact(command=command, command_hash=row["command_hash"],
                    status=fact["status"], enqueued_at=datetime.fromisoformat(fact["enqueued_at"]),
                    completed_at=None if fact["completed_at"] is None else datetime.fromisoformat(fact["completed_at"]),
                    result=None if fact["result_json"] is None else strict_json_loads(fact["result_json"]),
                    admission_json=admission)
                connection.rollback()
            self.collaboration.current_role(authenticated_actor_id)
            return value

    def _trusted_result_owner(self, query: object, *, authenticated_actor_id: str) -> ResultOwnerProof:
        from rquant.strict_json import strict_json_loads
        from rquant.web.models.collaboration import ResultOwnerQuery

        if type(query) is not ResultOwnerQuery:
            raise ValueError("exact result ownership query required")
        self.collaboration.current_role(authenticated_actor_id)
        self.collaboration.require_outbox_identity()
        kind = {"factor": "submit_factor_run", "portfolio": "submit_portfolio_backtest", "strategy": "run_strategy_template", "minute": "submit_minute_replay"}[query.domain]
        with self.outbox._connect() as connection:
            connection.execute("PRAGMA query_only = ON")
            connection.execute("BEGIN")
            found = None
            for row in self.outbox._result_submission_rows(connection, kind):
                command = _COMMAND_ADAPTER.validate_json(row["payload_json"])
                result = strict_json_loads(row["verified_effect_result"])
                if not isinstance(result, dict):
                    raise PermissionError("original result acceptance is unavailable")
                if UUID(str(result.get("job_id"))) != UUID(query.job_id):
                    continue
                proof = self.outbox._row_authorization(row)
                restored = None
                # Only original private owned types, their concrete metadata verifier,
                # and the exact original job receipt can restore historic provenance.
                if type(command) is _OwnedSubmitFactorRun:
                    self.consumer._factor_run_backend().validate(command)
                    from rquant.factor.job_ledger import FactorEvaluationJobLedger
                    record = FactorEvaluationJobLedger.open_existing(command.ledger_identity,
                        clock=self.consumer.clock).lookup_command(command.command_id, command.spec.spec_sha256)
                    if (record is None or record.spec != command.spec or record.job_id != result.get("job_id")
                            or result.get("status") != "queued"
                            or result.get("spec_sha256") != record.spec_sha256
                            or result.get("definition_version") != command.spec.adapter_request.formula.definition.version):
                        raise PermissionError("original factor job acceptance differs")
                    restored = command.actor_id if proof is None else None
                elif type(command) is OwnedRunStrategyTemplate:
                    backend = self.consumer._strategy_authoring_backend()
                    backend.validate(command)
                    recovered = backend.store.lookup_command(command.original(), owner_id=command.owner_id,
                        expected_identity=command.metadata_identity)
                    if recovered is None or recovered.model_dump(mode="json") != result:
                        raise PermissionError("original strategy job acceptance differs")
                    restored = command.owner_id if proof is None else None
                candidate = self.outbox._result_submission_binding(row, self.collaboration,
                    restored_private_owner=restored)
                if (candidate is None or candidate.owner_id != authenticated_actor_id
                        or candidate.spec_hash != query.spec_hash or candidate.job_id != query.job_id):
                    continue
                if found is not None and found != candidate:
                    raise PermissionError("original result has ambiguous submission provenance")
                found = candidate
            connection.rollback()
        self.collaboration.current_role(authenticated_actor_id)
        if found is None:
            raise LookupError("original result ownership is unavailable")
        return found

    def _submit_verified_ack(self, command: AckAlert) -> PageControlReceipt:
        """Called only after the local Serving admission checks succeed."""
        return self._settle(command, self.outbox.enqueue_verified_ack(command))

    def _screen_owned(
        self,
        command: ExecuteScreenQuery | SaveNlPreset,
        *,
        authenticated_actor_id: str,
        definition: ScreenPresetDefinition | None = None,
        expected_version: int | None = None,
    ) -> _OwnedExecuteScreenQuery | _OwnedSaveNlPreset:
        if self.consumer.screen_query_history is None:
            raise RuntimeError("private screening is not configured")
        if type(command) is ExecuteScreenQuery:
            return _OwnedExecuteScreenQuery(**command.model_dump(), owner_id=authenticated_actor_id)
        if type(command) is SaveNlPreset and definition is not None:
            payload = command.model_dump()
            payload.pop("kind")
            return _OwnedSaveNlPreset(
                **payload,
                owner_id=authenticated_actor_id,
                definition=definition,
                expected_version=expected_version,
            )
        raise TypeError("screen admission requires an exact ownerless request")

    def _submit_trusted_screen_query(
        self,
        command: ExecuteScreenQuery | SaveNlPreset,
        *,
        authenticated_actor_id: str,
        definition: ScreenPresetDefinition | None = None,
        expected_version: int | None = None,
    ) -> PageControlReceipt:
        owned = self._screen_owned(
            command,
            authenticated_actor_id=authenticated_actor_id,
            definition=definition,
            expected_version=expected_version,
        )
        history = self.consumer.screen_query_history
        history._assert_private_database()
        existing = self.outbox.lookup_screen_query_command(owned)
        if existing is not None:
            return self._settle(owned, existing, screen_query_command=owned)
        history.ensure_capacity()
        return self._settle(
            owned, self.outbox.enqueue_trusted_screen_query(owned), screen_query_command=owned
        )

    def _lookup_trusted_screen_query(
        self,
        command: ExecuteScreenQuery | SaveNlPreset,
        *,
        authenticated_actor_id: str,
        definition: ScreenPresetDefinition | None = None,
        expected_version: int | None = None,
    ) -> PageControlReceipt | None:
        owned = self._screen_owned(
            command,
            authenticated_actor_id=authenticated_actor_id,
            definition=definition,
            expected_version=expected_version,
        )
        return self.outbox.lookup_screen_query_command(owned)

    def _resume_trusted_screen_query(
        self,
        command: ExecuteScreenQuery | SaveNlPreset,
        *,
        authenticated_actor_id: str,
        definition: ScreenPresetDefinition | None = None,
        expected_version: int | None = None,
    ) -> PageControlReceipt | None:
        owned = self._screen_owned(
            command,
            authenticated_actor_id=authenticated_actor_id,
            definition=definition,
            expected_version=expected_version,
        )
        receipt = self.outbox.lookup_screen_query_command(owned)
        return None if receipt is None else self._settle(owned, receipt, screen_query_command=owned)

    def _submit_trusted_research_query(
        self, command: SaveResearchQuery, *, authenticated_actor_id: str
    ) -> PageControlReceipt:
        if type(command) is not SaveResearchQuery:
            raise TypeError("query save requires an unowned command")
        owned = _OwnedSaveResearchQuery(**command.model_dump(), owner_id=authenticated_actor_id)
        return self._settle(
            owned, self.outbox.enqueue_trusted_research_query(owned), research_query_command=owned
        )

    def _resume_trusted_research_query(
        self, command: SaveResearchQuery, *, authenticated_actor_id: str
    ) -> PageControlReceipt | None:
        if type(command) is not SaveResearchQuery:
            raise TypeError("query recovery requires original unowned command")
        owned = _OwnedSaveResearchQuery(**command.model_dump(), owner_id=authenticated_actor_id)
        receipt = self.outbox.lookup_research_query_command(owned)
        return (
            None if receipt is None else self._settle(owned, receipt, research_query_command=owned)
        )

    def _submit_trusted_watchlist(
        self,
        command: AddWatchlistItem | RemoveWatchlistItem,
        *,
        authenticated_owner_id: str,
    ) -> PageControlReceipt:
        """Internal entry point; a future protected boundary supplies the authenticated owner."""
        if not isinstance(command, (AddWatchlistItem, RemoveWatchlistItem)):
            raise TypeError("trusted watchlist submission requires a watchlist command")
        identity = ManualWatchlistKey(
            owner_id=authenticated_owner_id,
            ts_code=command.item.ts_code,
        )
        if identity.owner_id != command.item.owner_id:
            raise ValueError("authenticated owner does not match watchlist command owner")
        return self._settle(command, self.outbox.enqueue_trusted_watchlist(command))

    def _submit_trusted_price_rule(
        self, command: PriceAlertRuleRequestValue, *, authenticated_owner_id: str
    ) -> PageControlReceipt:
        owned = _owned_price_rule_command(command, authenticated_owner_id=authenticated_owner_id)
        return self._settle(
            owned, self.outbox.enqueue_trusted_price_rule(owned), price_rule_command=owned
        )

    def _submit_trusted_condition_rule(
        self, command: ConditionAlertRuleRequestValue, *, authenticated_owner_id: str
    ) -> PageControlReceipt:
        owned = _owned_condition_rule_command(
            command, authenticated_owner_id=authenticated_owner_id
        )
        return self._settle(
            owned, self.outbox.enqueue_trusted_condition_rule(owned), condition_rule_command=owned
        )

    def _lookup_trusted_condition_rule(
        self, command: ConditionAlertRuleRequestValue, *, authenticated_owner_id: str
    ) -> PageControlReceipt | None:
        return self.outbox.lookup_condition_rule_command(
            _owned_condition_rule_command(command, authenticated_owner_id=authenticated_owner_id)
        )

    def _resume_trusted_condition_rule(
        self, command: ConditionAlertRuleRequestValue, *, authenticated_owner_id: str
    ) -> PageControlReceipt:
        owned = _owned_condition_rule_command(
            command, authenticated_owner_id=authenticated_owner_id
        )
        receipt = self.outbox.lookup_condition_rule_command(owned)
        if receipt is None:
            raise KeyError("condition original request not found")
        return self._settle(owned, receipt, condition_rule_command=owned)

    def _submit_trusted_factor_tracking(
        self,
        request: FactorTrackingRequest,
        *,
        authenticated_actor_id: str,
        verified_registry_instance_id: str,
    ) -> PageControlReceipt:
        backend = self.consumer._factor_tracking_backend()
        backend.authorize(authenticated_actor_id)
        matched = self.outbox.lookup_factor_tracking_command(
            request, authenticated_actor_id=authenticated_actor_id
        )
        if matched is not None:
            return self._resume_trusted_factor_tracking(
                request, authenticated_actor_id=authenticated_actor_id
            )
        frozen = backend.compile(
            request, verified_registry_instance_id=verified_registry_instance_id
        )
        owned = _OwnedSetFactorTracked(
            command_id=request.command_id,
            requested_at=request.requested_at,
            request=request,
            actor_id=authenticated_actor_id,
            registry_identity=frozen.registry_identity,
            tracking_identity=frozen.tracking_identity,
        )
        return self._settle(
            owned, self.outbox.enqueue_trusted_factor_tracking(owned), factor_tracking_command=owned
        )

    def _lookup_trusted_factor_tracking(
        self, request: FactorTrackingRequest, *, authenticated_actor_id: str
    ) -> PageControlReceipt | None:
        backend = self.consumer._factor_tracking_backend()
        backend.authorize(authenticated_actor_id)
        matched = self.outbox.lookup_factor_tracking_command(
            request, authenticated_actor_id=authenticated_actor_id
        )
        if matched is None:
            return None
        owned, receipt = matched
        if receipt.status is PageControlStatus.SUCCEEDED:
            recovered = backend.recover(owned)
            if recovered is None or recovered != receipt.result:
                raise PageControlCommandConflictError(
                    "tracking receipt differs from the original authority"
                )
        return receipt

    def _resume_trusted_factor_tracking(
        self, request: FactorTrackingRequest, *, authenticated_actor_id: str
    ) -> PageControlReceipt:
        self.consumer._factor_tracking_backend().authorize(authenticated_actor_id)
        matched = self.outbox.lookup_factor_tracking_command(
            request, authenticated_actor_id=authenticated_actor_id
        )
        if matched is None:
            raise KeyError("tracking command not found")
        owned, receipt = matched
        self._lookup_trusted_factor_tracking(request, authenticated_actor_id=authenticated_actor_id)
        return self._settle(owned, receipt, factor_tracking_command=owned)

    def _submit_trusted_factor_run(
        self,
        request: FactorRunRequest,
        *,
        authenticated_actor_id: str,
        verified_registry_instance_id: str,
    ) -> PageControlReceipt:
        backend = self.consumer._factor_run_backend()
        backend.authorize(authenticated_actor_id)
        matched = self.outbox.lookup_factor_run_command(
            request,
            authenticated_actor_id=authenticated_actor_id,
        )
        if matched is not None:
            owned, receipt = matched
            self._lookup_trusted_factor_run(request, authenticated_actor_id=authenticated_actor_id)
            return self._settle(owned, receipt, factor_run_command=owned)
        plan = backend.compile(request, verified_registry_instance_id=verified_registry_instance_id)
        owned = _OwnedSubmitFactorRun(
            command_id=request.command_id,
            requested_at=request.requested_at,
            request=request,
            actor_id=authenticated_actor_id,
            registry_identity=plan.registry_identity,
            ledger_identity=plan.ledger_identity,
            spec=plan.spec,
            original_request_sha256=request.request_sha256,
        )
        if self.collaboration.mode == "enforced":
            with self.collaboration.locked():
                self.collaboration.require_command(authenticated_actor_id, owned.kind)
                self.consumer._require_factor_job_owner(owned, actor_id=authenticated_actor_id)
                receipt = self.outbox.enqueue_trusted_factor_run(owned)
            return self._settle(owned, receipt, factor_run_command=owned)
        return self._settle(
            owned, self.outbox.enqueue_trusted_factor_run(owned), factor_run_command=owned
        )

    def _lookup_trusted_factor_run(
        self,
        request: FactorRunRequest,
        *,
        authenticated_actor_id: str,
    ) -> PageControlReceipt | None:
        self.consumer._factor_run_backend().authorize(authenticated_actor_id)
        matched = self.outbox.lookup_factor_run_command(
            request, authenticated_actor_id=authenticated_actor_id
        )
        if matched is None:
            return None
        owned, receipt = matched
        if receipt.status is PageControlStatus.SUCCEEDED:
            recovered = self.consumer._factor_run_backend().recover(owned)
            if recovered is None or recovered != receipt.result:
                raise PageControlCommandConflictError(
                    "factor run receipt differs from original ledger"
                )
        return receipt

    def _resume_trusted_factor_run(
        self,
        request: FactorRunRequest,
        *,
        authenticated_actor_id: str,
    ) -> PageControlReceipt:
        self.consumer._factor_run_backend().authorize(authenticated_actor_id)
        matched = self.outbox.lookup_factor_run_command(
            request, authenticated_actor_id=authenticated_actor_id
        )
        if matched is None:
            raise KeyError("factor run command not found")
        owned, receipt = matched
        self._lookup_trusted_factor_run(request, authenticated_actor_id=authenticated_actor_id)
        return self._settle(owned, receipt, factor_run_command=owned)

    def _submit_trusted_factor_definition(
        self, command: FactorDefinitionRequestValue, *, authenticated_actor_id: str
    ) -> PageControlReceipt:
        identity = (
            self.consumer._factor_definition_backend().identity()
            if type(command) is ArchiveFactor
            else None
        )
        owned = _owned_factor_definition_command(
            command, authenticated_actor_id=authenticated_actor_id, registry_identity=identity
        )
        return self._settle(owned, self.outbox.enqueue_trusted_factor_definition(owned))

    def _submit_trusted_factor_archive(
        self,
        command: ArchiveFactor,
        *,
        authenticated_actor_id: str,
        verified_registry_instance_id: str,
    ) -> PageControlReceipt:
        if type(command) is not ArchiveFactor:
            raise TypeError("factor archive admission accepts ArchiveFactor only")
        identity = self.consumer._factor_definition_backend().identity()
        if identity.instance_id != verified_registry_instance_id:
            raise ValueError("factor registry differs from verified Serving projection")
        owned = _owned_factor_definition_command(
            command, authenticated_actor_id=authenticated_actor_id, registry_identity=identity
        )
        if self.consumer._factor_definition_backend().identity() != identity:
            raise ValueError("factor registry changed before archive enqueue")
        return self._settle(
            owned,
            self.outbox.enqueue_trusted_factor_definition(owned),
            factor_archive_command=owned,
        )

    def _submit_trusted_factor_save(
        self,
        draft: FactorSaveDraft,
        *,
        authenticated_actor_id: str,
        verified_registry_instance_id: str,
    ) -> PageControlReceipt:
        checked = FactorSaveDraft.model_validate(draft)
        identity = self.consumer._factor_definition_backend().identity()
        if identity.instance_id != verified_registry_instance_id:
            raise ValueError("factor registry differs from verified Serving projection")
        command = SaveFactorDefinition(
            command_id=checked.command_id,
            requested_at=checked.requested_at,
            definition=build_draft_definition(
                checked,
                authenticated_actor_id=authenticated_actor_id,
                capabilities=self.factor_definition_capabilities(),
            ),
            expected_head=checked.expected_head,
        )
        owned = _owned_factor_definition_command(
            command, authenticated_actor_id=authenticated_actor_id, registry_identity=identity
        )
        assert isinstance(owned, _OwnedSaveFactorDefinition)
        owned = owned.model_copy(
            update={
                "original_request_sha256": draft_sha256(checked),
                "original_generation_id": checked.generation_id,
            }
        )
        if self.consumer._factor_definition_backend().identity() != identity:
            raise ValueError("factor registry changed before save enqueue")
        return self._settle(
            owned,
            self.outbox.enqueue_trusted_factor_definition(owned),
            factor_archive_command=owned,
        )

    def factor_definition_capabilities(self) -> DailyFactorCapabilities:
        configured = getattr(self.consumer.factor_run_backend, "capabilities", None)
        if configured is None:
            return HISTORICAL_DAILY_V1
        try:
            return DailyFactorCapabilities.model_validate(configured())
        except (OSError, ValueError):
            return HISTORICAL_DAILY_V1

    def _lookup_trusted_factor_save(
        self, draft: FactorSaveDraft, *, authenticated_actor_id: str
    ) -> PageControlReceipt | None:
        matched = self.outbox.lookup_factor_save_command(
            draft, authenticated_actor_id=authenticated_actor_id
        )
        return None if matched is None else matched[1]

    def _resume_trusted_factor_save(
        self, draft: FactorSaveDraft, *, authenticated_actor_id: str
    ) -> PageControlReceipt:
        matched = self.outbox.lookup_factor_save_command(
            draft, authenticated_actor_id=authenticated_actor_id
        )
        if matched is None:
            raise KeyError("factor save command not found")
        owned, receipt = matched
        return self._settle(owned, receipt, factor_archive_command=owned)

    def _lookup_trusted_factor_archive(
        self, command: ArchiveFactor, *, authenticated_actor_id: str
    ) -> PageControlReceipt | None:
        matched = self.outbox.lookup_factor_archive_command(
            command, authenticated_actor_id=authenticated_actor_id
        )
        return None if matched is None else matched[1]

    def _resume_trusted_factor_archive(
        self, command: ArchiveFactor, *, authenticated_actor_id: str
    ) -> PageControlReceipt:
        matched = self.outbox.lookup_factor_archive_command(
            command, authenticated_actor_id=authenticated_actor_id
        )
        if matched is None:
            raise KeyError("factor archive command not found")
        owned, receipt = matched
        return self._settle(owned, receipt, factor_archive_command=owned)

    def _lookup_trusted_price_rule(
        self, command: PriceAlertRuleRequestValue, *, authenticated_owner_id: str
    ) -> PageControlReceipt | None:
        owned = _owned_price_rule_command(command, authenticated_owner_id=authenticated_owner_id)
        return self.outbox.lookup_price_rule_command(owned)

    def _lookup_trusted_strategy_authoring(
        self, request: StrategyTemplateCommand, *, authenticated_actor_id: str
    ) -> PageControlReceipt | None:
        self.consumer._strategy_authoring_backend().authorize(authenticated_actor_id)
        matched = self.outbox.lookup_strategy_authoring_command(
            request, authenticated_actor_id=authenticated_actor_id
        )
        return None if matched is None else matched[1]

    def _enqueue_strategy_promotion(self, command: OwnedStrategyPromotionCommand, *, actor_id: str) -> PageControlReceipt:
        if command.owner_id != actor_id:
            raise PermissionError("original private actor differs")
        token = _TRUSTED_COLLABORATION_ACTOR.set(actor_id)
        try:
            return self.outbox.enqueue_trusted_strategy_promotion(command)
        finally:
            _TRUSTED_COLLABORATION_ACTOR.reset(token)

    def _lookup_trusted_strategy_promotion(self, request: StrategyPromotionCommand, *, authenticated_actor_id: str) -> PageControlReceipt | None:
        self.consumer._strategy_promotion_backend().authorize(authenticated_actor_id, request, read=True)
        matched = self.outbox.lookup_strategy_promotion_command(request, authenticated_actor_id=authenticated_actor_id)
        return None if matched is None else matched[1]

    def _resume_trusted_strategy_promotion(self, request: StrategyPromotionCommand, *, authenticated_actor_id: str) -> PageControlReceipt:
        self.consumer._strategy_promotion_backend().authorize(authenticated_actor_id, request, read=True)
        matched = self.outbox.lookup_strategy_promotion_command(request, authenticated_actor_id=authenticated_actor_id)
        if matched is None:
            raise KeyError("原评估操作尚未受理。")
        return self._settle(matched[0], matched[1], strategy_promotion_command=matched[0])

    def _submit_trusted_strategy_promotion(self, request: StrategyPromotionCommand, *, authenticated_actor_id: str,
            verified_metadata_identity: StrategyAuthoringIdentity) -> PageControlReceipt:
        backend = self.consumer._strategy_promotion_backend()
        with self.collaboration.locked():
            backend.authorize(authenticated_actor_id, request, read=True)
            matched = self.outbox.lookup_strategy_promotion_command(request, authenticated_actor_id=authenticated_actor_id)
            if matched is not None:
                return self._settle(matched[0], matched[1], strategy_promotion_command=matched[0])
            owned = backend.compile(request, actor_id=authenticated_actor_id, expected_identity=verified_metadata_identity)
            receipt = self._enqueue_strategy_promotion(owned, actor_id=authenticated_actor_id)
            return self._settle(owned, receipt, strategy_promotion_command=owned)

    def _resume_trusted_strategy_authoring(
        self, request: StrategyTemplateCommand, *, authenticated_actor_id: str
    ) -> PageControlReceipt:
        self.consumer._strategy_authoring_backend().authorize(authenticated_actor_id)
        matched = self.outbox.lookup_strategy_authoring_command(
            request, authenticated_actor_id=authenticated_actor_id
        )
        if matched is None:
            raise KeyError("strategy original command not found")
        owned, receipt = matched
        return self._settle(owned, receipt, strategy_authoring_command=owned)

    def _submit_trusted_strategy_authoring(
        self,
        request: StrategyTemplateCommand,
        *,
        authenticated_actor_id: str,
        verified_metadata_identity: StrategyAuthoringIdentity,
        catalog: StrategySourceCatalog,
    ) -> PageControlReceipt:
        backend = self.consumer._strategy_authoring_backend()
        backend.authorize(authenticated_actor_id)
        matched = self.outbox.lookup_strategy_authoring_command(
            request, authenticated_actor_id=authenticated_actor_id
        )
        if matched is not None:
            return self._settle(matched[0], matched[1], strategy_authoring_command=matched[0])
        owned = backend.compile(
            request,
            authenticated_actor_id=authenticated_actor_id,
            catalog=catalog,
            expected_identity=verified_metadata_identity,
        )
        backend.validate(owned)
        return self._settle(
            owned,
            self.outbox.enqueue_trusted_strategy_authoring(owned),
            strategy_authoring_command=owned,
        )

    def _lookup_trusted_paper_portfolio(
        self, request: PaperPortfolioCommand, *, authenticated_actor_id: str
    ) -> PageControlReceipt | None:
        backend = self.consumer._paper_portfolio_backend()
        backend.authorize(authenticated_actor_id)
        backend.catalog.for_account(request.account_id, authenticated_actor_id=authenticated_actor_id)
        matched = self.outbox.lookup_paper_portfolio_command(request, authenticated_actor_id=authenticated_actor_id)
        return None if matched is None else matched[1]


    def _resume_trusted_paper_portfolio(
        self, request: PaperPortfolioCommand, *, authenticated_actor_id: str
    ) -> PageControlReceipt:
        backend = self.consumer._paper_portfolio_backend()
        backend.authorize(authenticated_actor_id)
        backend.catalog.for_account(request.account_id, authenticated_actor_id=authenticated_actor_id)
        matched = self.outbox.lookup_paper_portfolio_command(request, authenticated_actor_id=authenticated_actor_id)
        if matched is None:
            raise KeyError("paper original command not found")
        owned, receipt = matched
        return self._settle(owned, receipt, paper_portfolio_command=owned)


    def _submit_trusted_paper_portfolio(
        self, request: PaperPortfolioCommand, *, authenticated_actor_id: str,
        verified_metadata_identity: PaperPortfolioStateIdentity, confirmation_id: str | None = None,
    ) -> PageControlReceipt:
        backend = self.consumer._paper_portfolio_backend()
        backend.authorize(authenticated_actor_id)
        backend.catalog.for_account(request.account_id, authenticated_actor_id=authenticated_actor_id)
        matched = self.outbox.lookup_paper_portfolio_command(request, authenticated_actor_id=authenticated_actor_id)
        if matched is not None:
            return self._settle(matched[0], matched[1], paper_portfolio_command=matched[0])
        owned = backend.compile(request, authenticated_actor_id=authenticated_actor_id,
                                expected_identity=verified_metadata_identity, confirmation_id=confirmation_id)
        backend.validate(owned)
        return self._settle(owned, self.outbox.enqueue_trusted_paper_portfolio(owned), paper_portfolio_command=owned)


    def _resume_trusted_price_rule(
        self, command: PriceAlertRuleRequestValue, *, authenticated_owner_id: str
    ) -> PageControlReceipt:
        owned = _owned_price_rule_command(command, authenticated_owner_id=authenticated_owner_id)
        receipt = self.outbox.lookup_price_rule_command(owned)
        if receipt is None:
            raise KeyError("price rule command not found")
        return self._settle(owned, receipt, price_rule_command=owned)

    def _settle(
        self,
        command: PageControlCommandValue,
        receipt: PageControlReceipt,
        *,
        price_rule_command: _OwnedPriceAlertRuleValue | None = None,
        condition_rule_command: _OwnedConditionAlertRuleValue | None = None,
        factor_archive_command: _OwnedFactorDefinitionValue | None = None,
        factor_run_command: _OwnedSubmitFactorRun | None = None,
        factor_tracking_command: _OwnedSetFactorTracked | None = None,
        research_query_command: _OwnedSaveResearchQuery | None = None,
        screen_query_command: _OwnedExecuteScreenQuery | _OwnedSaveNlPreset | None = None,
        strategy_authoring_command: OwnedStrategyTemplateCommand | None = None,
        strategy_promotion_command: OwnedStrategyPromotionCommand | None = None,
        paper_portfolio_command: OwnedPaperPortfolioCommand | None = None,
        task_control_command: OwnedTaskControl | None = None,
    ) -> PageControlReceipt:
        for _ in range(100):
            if receipt.status in _PAGE_CONTROL_TERMINAL_STATUSES:
                if strategy_promotion_command is not None and receipt.status is PageControlStatus.SUCCEEDED:
                    if self.consumer._strategy_promotion_backend().recover(strategy_promotion_command) != receipt.result:
                        raise RuntimeError("原人工阶段回执与命令日志不同。")
                if task_control_command is not None and receipt.status is PageControlStatus.SUCCEEDED:
                    recovered = self.consumer._task_control_backend().recover(task_control_command)
                    if recovered != receipt.result:
                        raise RuntimeError("task original acceptance receipt differs from journal")
                if paper_portfolio_command is not None and receipt.status is PageControlStatus.SUCCEEDED:
                    recovered = self.consumer._paper_portfolio_backend().recover(paper_portfolio_command)
                    if recovered != receipt.result:
                        raise RuntimeError("paper original metadata receipt differs from journal")
                if (
                    strategy_authoring_command is not None
                    and receipt.status is PageControlStatus.SUCCEEDED
                ):
                    recovered = self.consumer._strategy_authoring_backend().recover(
                        strategy_authoring_command
                    )
                    if recovered != receipt.result:
                        raise RuntimeError(
                            "strategy original metadata receipt differs from journal"
                        )
                return receipt
            if task_control_command is not None:
                drained = self.consumer.drain_task_control_command(task_control_command)
            elif strategy_promotion_command is not None:
                drained = self.consumer.drain_strategy_promotion_command(strategy_promotion_command)
            elif paper_portfolio_command is not None:
                drained = self.consumer.drain_paper_portfolio_command(paper_portfolio_command)
            elif screen_query_command is not None:
                drained = self.consumer.drain_screen_query_command(screen_query_command)
            elif strategy_authoring_command is not None:
                drained = self.consumer.drain_strategy_authoring_command(strategy_authoring_command)
            elif research_query_command is not None:
                drained = self.consumer.drain_research_query_command(research_query_command)
            elif factor_tracking_command is not None:
                drained = self.consumer.drain_factor_definition_command(factor_tracking_command)
            elif factor_run_command is not None:
                drained = self.consumer.drain_factor_definition_command(factor_run_command)
            elif factor_archive_command is not None:
                drained = self.consumer.drain_factor_definition_command(factor_archive_command)
            elif price_rule_command is not None:
                drained = self.consumer.drain_price_rule_command(price_rule_command)
            elif condition_rule_command is not None:
                drained = self.consumer.drain_condition_rule_command(condition_rule_command)
            else:
                drained = self.consumer.drain(limit=100)
            observed = self.outbox.receipt(command.command_id)
            if observed is None:
                raise RuntimeError("page control command disappeared")
            if observed.status is PageControlStatus.PENDING:
                return observed
            if not drained and observed.status is PageControlStatus.PROCESSING:
                return observed
            receipt = observed
        raise RuntimeError("page control command did not reach a terminal state")

    def lookup_ack_command(self, command: AckAlert) -> PageControlReceipt | None:
        return self.outbox.lookup_ack_command(command)

    def _lookup_trusted_task_control(self, request: TaskControlRequest, *, authenticated_actor_id: str) -> PageControlReceipt | None:
        backend = self.consumer._task_control_backend()
        backend.authorize(authenticated_actor_id, request)
        matched = self.outbox.lookup_task_control_command(request, authenticated_actor_id=authenticated_actor_id)
        if matched is None:
            return None
        backend.validate(matched[0])
        return matched[1]

    def _resume_trusted_task_control(self, request: TaskControlRequest, *, authenticated_actor_id: str) -> PageControlReceipt:
        backend = self.consumer._task_control_backend()
        backend.authorize(authenticated_actor_id, request)
        matched = self.outbox.lookup_task_control_command(request, authenticated_actor_id=authenticated_actor_id)
        if matched is None:
            raise KeyError("task original command not found")
        backend.validate(matched[0])
        return self._settle(matched[0], matched[1], task_control_command=matched[0])

    def _submit_trusted_task_control(self, request: TaskControlRequest, *, authenticated_actor_id: str,
                                     verified_metadata_identity: TaskControlIdentity) -> PageControlReceipt:
        backend = self.consumer._task_control_backend()
        backend.authorize(authenticated_actor_id, request)
        matched = self.outbox.lookup_task_control_command(request, authenticated_actor_id=authenticated_actor_id)
        if matched is not None:
            backend.validate(matched[0])
            return self._settle(matched[0], matched[1], task_control_command=matched[0])
        owned = backend.compile(request, authenticated_actor_id=authenticated_actor_id, expected_identity=verified_metadata_identity)
        backend.validate(owned)
        receipt = self.outbox.enqueue_trusted_task_control(owned)
        return self._settle(owned, receipt, task_control_command=owned)


def _private_role_entry(method: Callable[..., object]) -> Callable[..., object]:
    """Intersect actual original private service entries; generic parsers stay closed."""
    @wraps(method)
    def guarded(self: PageControlService, *args: object, **kwargs: object) -> object:
        if self.collaboration.mode != "enforced":
            return method(self, *args, **kwargs)
        actor = kwargs.get("authenticated_actor_id", kwargs.get("authenticated_owner_id"))
        request = args[0] if args else kwargs.get("command", kwargs.get("request", kwargs.get("draft")))
        if method.__name__ == "_submit_verified_ack":
            actor = getattr(request, "actor_id", None)
        if not isinstance(actor, str):
            raise PermissionError("original private actor is unavailable")
        kind = getattr(request, "kind", None)
        if type(request) is FactorRunRequest:
            kind = "submit_factor_run"
        elif "factor_save" in method.__name__:
            kind = "save_factor_definition"
        elif kind == "save_nl_preset" and "screen_query" in method.__name__:
            kind = "save_screen_query_preset"
        if not isinstance(kind, str):
            raise PermissionError("original private command kind is unavailable")
        promotion = method.__name__.endswith("strategy_promotion")
        if promotion:
            self.consumer._strategy_promotion_backend().authorize(actor, request, read=True)
        else:
            self.collaboration.require_command(actor, kind)
        token = _TRUSTED_COLLABORATION_ACTOR.set(actor)
        try:
            result = method(self, *args, **kwargs)
            if promotion:
                self.consumer._strategy_promotion_backend().authorize(actor, request, read=True)
            else:
                self.collaboration.require_command(actor, kind)
            return result
        finally:
            _TRUSTED_COLLABORATION_ACTOR.reset(token)
    return guarded


# Only original concrete trusted service methods; no new entry or dispatcher.
for _private_name in tuple(PageControlService.__dict__):
    if _private_name.startswith(("_submit_trusted_", "_lookup_trusted_", "_resume_trusted_")) or _private_name == "_submit_verified_ack":
        setattr(PageControlService, _private_name, _private_role_entry(getattr(PageControlService, _private_name)))


PageControlTransport = Callable[[dict[str, object]], dict[str, object]]


class PageControlUnavailableError(RuntimeError):
    """The loopback control authority cannot accept a page command right now."""


class PageControlClient:
    """Page-side API client; it has no filesystem persistence capability."""

    def __init__(
        self,
        *,
        endpoint: str | None = None,
        transport: PageControlTransport | None = None,
        lookup_transport: PageControlTransport | None = None,
        timeout_seconds: float = 1.0,
    ) -> None:
        self.endpoint = endpoint or os.environ.get(
            "RQUANT_PAGE_CONTROL_URL",
            "http://127.0.0.1:8767/v1/commands",
        )
        self.transport = transport or self._post
        self.lookup_transport = lookup_transport or self._post_lookup
        self.timeout_seconds = timeout_seconds

    def submit(self, command: PageControlCommandValue) -> PageControlReceipt:
        if isinstance(command, (AddWatchlistItem, RemoveWatchlistItem)):
            raise ValueError("watchlist commands require trusted submission")
        if isinstance(
            command, (SavePriceAlertRule, SetPriceAlertRuleEnabled, DeletePriceAlertRule)
        ):
            raise ValueError("price rule commands require trusted submission")
        if isinstance(command, (SaveFactorDefinition, ArchiveFactor)):
            raise ValueError("factor commands require trusted submission")
        try:
            response = self.transport(command.model_dump(mode="json"))
        except (OSError, TimeoutError, urllib.error.URLError) as exc:
            raise PageControlUnavailableError(
                f"page control service unavailable: {type(exc).__name__}: {exc}"
            ) from exc
        return PageControlReceipt.model_validate(response)

    def lookup_ack_command(self, command: AckAlert) -> PageControlReceipt | None:
        try:
            response = self.lookup_transport(command.model_dump(mode="json"))
        except (OSError, TimeoutError, urllib.error.URLError) as exc:
            raise PageControlUnavailableError(
                f"page control lookup unavailable: {type(exc).__name__}: {exc}"
            ) from exc
        if response == {"found": False}:
            return None
        if (
            isinstance(response, dict)
            and response.get("found") is True
            and set(response)
            == {
                "found",
                "receipt",
            }
        ):
            receipt = PageControlReceipt.model_validate(response["receipt"])
            if receipt.command_id != command.command_id:
                raise ValueError("invalid page control lookup response command_id")
            return receipt
        raise ValueError("invalid page control lookup response")

    def _post(self, payload: dict[str, object]) -> dict[str, object]:
        return self._post_to(self.endpoint, payload)

    def _post_lookup(self, payload: dict[str, object]) -> dict[str, object]:
        endpoint = urlsplit(self.endpoint)
        if (
            endpoint.scheme != "http"
            or endpoint.hostname not in {"127.0.0.1", "::1", "localhost"}
            or endpoint.path != "/v1/commands"
            or endpoint.query
            or endpoint.fragment
            or endpoint.username is not None
            or endpoint.password is not None
        ):
            raise ValueError("PageControl lookup requires the fixed loopback endpoint")
        lookup_url = urlunsplit((endpoint.scheme, endpoint.netloc, "/v1/commands/lookup", "", ""))
        try:
            return self._post_to(lookup_url, payload)
        except urllib.error.HTTPError as exc:
            if exc.code == 409:
                raise ValueError("command conflict") from exc
            raise

    def _post_to(self, endpoint: str, payload: dict[str, object]) -> dict[str, object]:
        request = urllib.request.Request(
            endpoint,
            data=json.dumps(payload, ensure_ascii=True).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(request, timeout=self.timeout_seconds) as response:
            body = json.loads(response.read().decode("utf-8"))
        if not isinstance(body, dict):
            raise RuntimeError("page control service returned a non-object response")
        return body


def _validated_name(value: str, *, label: str) -> str:
    candidate = value.strip()
    if candidate in {"", ".", ".."} or _SAFE_NAME.fullmatch(candidate) is None:
        raise ValueError(f"{label} contains unsafe characters")
    return candidate


def _command_hash(command: PageControlCommandValue) -> str:
    return canonical_sha256(command.model_dump(mode="json"))


def _default_page_control_consumer_id(
    *,
    consumer_service_id: str,
    outbox_path: Path,
) -> str:
    service_id = consumer_service_id.strip()
    if not service_id:
        raise ValueError("consumer_service_id is required")
    canonical_outbox_path = str(Path(os.path.abspath(outbox_path)).resolve(strict=False))
    instance_hash = canonical_sha256(
        {
            "contract": "page-control-consumer-instance/v1",
            "consumer_service_id": service_id,
            "outbox_path": canonical_outbox_path,
        }
    )
    return f"{service_id}:{instance_hash}"


def _normalize_utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("timestamp must be timezone aware")
    return value.astimezone(UTC)


def _parse_canvas_timestamp(value: str) -> datetime:
    return _normalize_utc(datetime.fromisoformat(value.replace("Z", "+00:00")))


def _assert_canvas_receipt_not_future(
    receipt: CanvasPublicationReceipt,
    *,
    observed_at: datetime,
) -> None:
    observed = _normalize_utc(observed_at)
    claims = receipt.claims
    timestamps = (
        ("requested_at", claims.command.requested_at),
        ("created_at", claims.created_at),
        ("catalog created_at", claims.catalog_record.created_at),
        ("catalog updated_at", claims.catalog_record.updated_at),
    )
    for label, value in timestamps:
        if value > observed:
            raise ValueError(f"canvas publication receipt {label} contains future evidence")


def _canvas_source_identity_hash(
    *,
    command_id: str,
    command_hash: str,
    source: str,
) -> str:
    return canonical_sha256(
        {
            "schema_version": _CANVAS_CATALOG_SCHEMA_VERSION,
            "command_id": command_id,
            "command_hash": command_hash,
            "source": source,
        }
    )


def _open_or_create_managed_directory(path: Path) -> int:
    binding = _bind_managed_directory(path, create=True)
    try:
        return os.dup(binding.descriptor)
    finally:
        binding.close()


def _open_existing_managed_directory(path: Path) -> int:
    binding = _bind_managed_directory(path, create=False)
    try:
        return os.dup(binding.descriptor)
    finally:
        binding.close()


def _bind_managed_directory(path: Path, *, create: bool) -> _BoundManagedDirectory:
    normalized = Path(os.path.abspath(path))
    flags = os.O_RDONLY | os.O_CLOEXEC
    if hasattr(os, "O_DIRECTORY"):
        flags |= os.O_DIRECTORY
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    # Linux allows safe traversal through execute-only ancestors with O_PATH.
    ancestor_flags = flags | os.O_PATH if hasattr(os, "O_PATH") else flags
    descriptors: list[int] = []
    component_names: list[str] = []
    try:
        components = normalized.parts[1:]
        descriptors.append(os.open(normalized.anchor, ancestor_flags if components else flags))
        for index, component in enumerate(components):
            parent = descriptors[-1]
            try:
                entry = os.stat(component, dir_fd=parent, follow_symlinks=False)
            except FileNotFoundError:
                if not create:
                    raise
                os.mkdir(component, _PRIVATE_DIRECTORY_MODE, dir_fd=parent)
                entry = os.stat(component, dir_fd=parent, follow_symlinks=False)
            if stat.S_ISLNK(entry.st_mode):
                raise ValueError(f"managed directory ancestor cannot be a symlink: {normalized}")
            if not stat.S_ISDIR(entry.st_mode):
                raise ValueError(f"managed directory ancestor is not a directory: {normalized}")
            descriptor = os.open(
                component,
                flags if index == len(components) - 1 else ancestor_flags,
                dir_fd=parent,
            )
            opened = os.fstat(descriptor)
            if _file_node_tuple(entry) != _file_node_tuple(opened):
                os.close(descriptor)
                raise ValueError(f"managed directory ancestor changed while opening: {normalized}")
            descriptors.append(descriptor)
            component_names.append(component)
            if index == len(components) - 1:
                os.fchmod(descriptor, _PRIVATE_DIRECTORY_MODE)
        binding = _BoundManagedDirectory(
            path=normalized,
            descriptors=tuple(descriptors),
            component_names=tuple(component_names),
        )
        binding.verify()
        return binding
    except FileNotFoundError:
        for descriptor in reversed(descriptors):
            os.close(descriptor)
        raise
    except OSError as exc:
        for descriptor in reversed(descriptors):
            os.close(descriptor)
        raise ValueError(f"managed directory cannot be opened safely: {normalized}") from exc
    except Exception:
        for descriptor in reversed(descriptors):
            os.close(descriptor)
        raise


def _read_managed_file(
    path: Path,
    *,
    directory_descriptor: int | None = None,
) -> bytes:
    directory = (
        _open_existing_managed_directory(path.parent)
        if directory_descriptor is None
        else directory_descriptor
    )
    flags = os.O_RDONLY | os.O_CLOEXEC
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    file_descriptor: int | None = None
    try:
        _verify_open_directory_matches_path(path.parent, directory)
        try:
            file_descriptor = os.open(path.name, flags, dir_fd=directory)
        except OSError as exc:
            raise ValueError(f"managed JSON file cannot be opened safely: {path}") from exc
        observed = os.fstat(file_descriptor)
        if stat.S_ISLNK(observed.st_mode):
            raise ValueError(f"managed JSON file cannot be a symlink: {path}")
        if not stat.S_ISREG(observed.st_mode):
            raise ValueError(f"managed JSON path is not a regular file: {path}")
        chunks: list[bytes] = []
        total = 0
        while chunk := os.read(file_descriptor, 1024 * 1024):
            total += len(chunk)
            if total > _MAX_MANAGED_JSON_BYTES:
                raise ValueError("managed JSON file exceeds its byte budget")
            chunks.append(chunk)
        after = os.fstat(file_descriptor)
        if _file_identity_tuple(after) != _file_identity_tuple(observed):
            raise ValueError(f"managed JSON file changed while reading: {path}")
        _verify_open_directory_matches_path(path.parent, directory)
        return b"".join(chunks)
    finally:
        if file_descriptor is not None:
            os.close(file_descriptor)
        os.close(directory)


def _append_managed_jsonl(
    path: Path,
    record: Mapping[str, object],
    *,
    command_id: str,
) -> None:
    directory = _open_or_create_managed_directory(path.parent)
    flags = os.O_RDWR | os.O_CREAT | os.O_CLOEXEC
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    file_descriptor: int | None = None
    try:
        _verify_open_directory_matches_path(path.parent, directory)
        try:
            file_descriptor = os.open(
                path.name,
                flags,
                _PRIVATE_FILE_MODE,
                dir_fd=directory,
            )
        except OSError as exc:
            raise ValueError(
                f"managed JSONL file cannot be opened safely without following symlinks: {path}"
            ) from exc
        opened = os.fstat(file_descriptor)
        if stat.S_ISLNK(opened.st_mode):
            raise ValueError(f"managed JSONL file cannot be a symlink: {path}")
        if not stat.S_ISREG(opened.st_mode):
            raise ValueError(f"managed JSONL path is not a regular file: {path}")
        os.fchmod(file_descriptor, _PRIVATE_FILE_MODE)
        _verify_open_file_matches_entry(
            directory,
            path.name,
            opened,
            label=f"managed JSONL file: {path}",
        )
        payload = _read_descriptor_bytes(
            file_descriptor,
            byte_limit=_MAX_MANAGED_LOG_BYTES,
            label="managed JSONL file",
        )
        after_read = os.fstat(file_descriptor)
        if _file_identity_tuple(after_read) != _file_identity_tuple(opened):
            raise ValueError(f"managed JSONL file changed while reading: {path}")
        _verify_open_directory_matches_path(path.parent, directory)
        if _jsonl_contains_command_id(payload, command_id):
            _verify_open_directory_matches_path(path.parent, directory)
            return
        line = (
            json.dumps(record, ensure_ascii=False, separators=(",", ":"), sort_keys=True) + "\n"
        ).encode("utf-8")
        os.lseek(file_descriptor, 0, os.SEEK_END)
        written = os.write(file_descriptor, line)
        if written != len(line):
            raise OSError("short write while appending managed JSONL record")
        os.fsync(file_descriptor)
        _verify_open_directory_matches_path(path.parent, directory)
        _verify_open_file_matches_entry(
            directory,
            path.name,
            os.fstat(file_descriptor),
            label=f"managed JSONL file: {path}",
        )
        _verify_open_directory_matches_path(path.parent, directory)
        _fsync_descriptor(directory)
        _verify_open_directory_matches_path(path.parent, directory)
    finally:
        if file_descriptor is not None:
            os.close(file_descriptor)
        os.close(directory)


def _read_descriptor_bytes(
    descriptor: int,
    *,
    byte_limit: int,
    label: str,
) -> bytes:
    os.lseek(descriptor, 0, os.SEEK_SET)
    chunks: list[bytes] = []
    total = 0
    while chunk := os.read(descriptor, 1024 * 1024):
        total += len(chunk)
        if total > byte_limit:
            raise ValueError(f"{label} exceeds its byte budget")
        chunks.append(chunk)
    return b"".join(chunks)


def _jsonl_contains_command_id(payload: bytes, command_id: str) -> bool:
    for line in payload.decode("utf-8").splitlines():
        if not line.strip():
            continue
        raw = json.loads(line)
        if not isinstance(raw, dict):
            raise ValueError("managed JSONL record is not a JSON object")
        if raw.get("command_id") == command_id:
            return True
    return False


def _managed_jsonl_contains_command_id(
    path: Path,
    command_id: str,
) -> bool:
    try:
        directory = _open_existing_managed_directory(path.parent)
    except FileNotFoundError:
        return False
    flags = os.O_RDONLY | os.O_CLOEXEC
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    file_descriptor: int | None = None
    try:
        _verify_open_directory_matches_path(path.parent, directory)
        try:
            file_descriptor = os.open(path.name, flags, dir_fd=directory)
        except FileNotFoundError:
            return False
        except OSError as exc:
            raise ValueError(
                f"managed JSONL file cannot be opened safely without following symlinks: {path}"
            ) from exc
        observed = os.fstat(file_descriptor)
        if stat.S_ISLNK(observed.st_mode):
            raise ValueError(f"managed JSONL file cannot be a symlink: {path}")
        if not stat.S_ISREG(observed.st_mode):
            raise ValueError(f"managed JSONL path is not a regular file: {path}")
        _verify_open_file_matches_entry(
            directory,
            path.name,
            observed,
            label=f"managed JSONL file: {path}",
        )
        payload = _read_descriptor_bytes(
            file_descriptor,
            byte_limit=_MAX_MANAGED_LOG_BYTES,
            label="managed JSONL file",
        )
        after_read = os.fstat(file_descriptor)
        if _file_identity_tuple(after_read) != _file_identity_tuple(observed):
            raise ValueError(f"managed JSONL file changed while reading: {path}")
        _verify_open_directory_matches_path(path.parent, directory)
        return _jsonl_contains_command_id(payload, command_id)
    finally:
        if file_descriptor is not None:
            os.close(file_descriptor)
        os.close(directory)


def _is_external_lab_effect(command: PageControlCommandValue) -> bool:
    return isinstance(
        command,
        (SubmitLabCommand, ExportLabArtifactZip, DiscardLabArtifactZip),
    )


def _ambiguous_lab_effect_result(command: PageControlCommandValue) -> dict[str, object]:
    return {
        "outcome": "ambiguous_completed_at_most_once",
        "command_id": command.command_id,
        "command_kind": command.kind,
        "command_hash": _command_hash(command),
        "reason": (
            "external Lab effect was durably started, but no result was recorded before "
            "the owner was reclaimed"
        ),
    }


def _ambiguous_local_effect_result(
    command: PageControlCommandValue,
    *,
    reason: str,
) -> dict[str, object]:
    return {
        "outcome": "ambiguous_completed_at_most_once",
        "command_id": command.command_id,
        "command_kind": command.kind,
        "command_hash": _command_hash(command),
        "reason": reason,
    }


def _ambiguous_external_effect_result_from_row(row: sqlite3.Row) -> dict[str, object]:
    try:
        command = _COMMAND_ADAPTER.validate_json(row["payload_json"])
    except ValueError:
        return {
            "outcome": "ambiguous_completed_at_most_once",
            "command_id": row["command_id"],
            "command_kind": row["command_kind"],
            "command_hash": row["command_hash"],
            "reason": (
                "legacy external Lab command was processing without a PageControl effect journal"
            ),
        }
    return {
        **_ambiguous_lab_effect_result(command),
        "reason": (
            "legacy external Lab command was processing without a PageControl effect journal"
        ),
    }


class _PageControlExecutionMutex:
    def __init__(self, path: Path) -> None:
        self.path = Path(os.path.abspath(path))
        self.directory_descriptor: int | None = None
        self.file_descriptor: int | None = None
        self.acquired = False

    def __enter__(self) -> bool:
        self.directory_descriptor = _open_or_create_managed_directory(self.path.parent)
        flags = os.O_RDWR | os.O_CREAT | os.O_CLOEXEC
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        try:
            self.file_descriptor = os.open(
                self.path.name,
                flags,
                _PRIVATE_FILE_MODE,
                dir_fd=self.directory_descriptor,
            )
        except OSError as exc:
            self._close()
            raise ValueError(f"consumer mutex cannot be opened safely: {self.path}") from exc
        opened = os.fstat(self.file_descriptor)
        if not stat.S_ISREG(opened.st_mode):
            self._close()
            raise ValueError(f"consumer mutex path is not a regular file: {self.path}")
        try:
            _verify_open_file_matches_entry(
                self.directory_descriptor,
                self.path.name,
                opened,
                label=f"consumer mutex: {self.path}",
            )
        except Exception:
            self._close()
            raise
        if self.path in _HELD_CONSUMER_MUTEXES:
            self._close()
            return False
        try:
            fcntl.flock(self.file_descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            self._close()
            return False
        except OSError as exc:
            if exc.errno in {errno.EACCES, errno.EAGAIN, errno.EWOULDBLOCK}:
                self._close()
                return False
            self._close()
            raise
        try:
            _verify_open_file_matches_entry(
                self.directory_descriptor,
                self.path.name,
                opened,
                label=f"consumer mutex: {self.path}",
            )
        except ValueError:
            self._unlock_and_close()
            return False
        os.fchmod(self.file_descriptor, _PRIVATE_FILE_MODE)
        secured = os.fstat(self.file_descriptor)
        try:
            _verify_open_file_matches_entry(
                self.directory_descriptor,
                self.path.name,
                secured,
                label=f"consumer mutex: {self.path}",
                expected_mode=_PRIVATE_FILE_MODE,
            )
        except ValueError:
            self._unlock_and_close()
            return False
        _HELD_CONSUMER_MUTEXES.add(self.path)
        self.acquired = True
        return True

    def __exit__(self, *_error: object) -> None:
        if self.acquired and self.file_descriptor is not None:
            with suppress(OSError):
                fcntl.flock(self.file_descriptor, fcntl.LOCK_UN)
            _HELD_CONSUMER_MUTEXES.discard(self.path)
            self.acquired = False
        self._close()

    def _unlock_and_close(self) -> None:
        if self.file_descriptor is not None:
            with suppress(OSError):
                fcntl.flock(self.file_descriptor, fcntl.LOCK_UN)
        self._close()

    def _close(self) -> None:
        if self.file_descriptor is not None:
            os.close(self.file_descriptor)
            self.file_descriptor = None
        if self.directory_descriptor is not None:
            os.close(self.directory_descriptor)
            self.directory_descriptor = None


def _verify_open_file_matches_entry(
    directory_descriptor: int,
    file_name: str,
    opened: os.stat_result,
    *,
    label: str,
    expected_mode: int | None = None,
) -> None:
    try:
        entry = os.stat(
            file_name,
            dir_fd=directory_descriptor,
            follow_symlinks=False,
        )
    except FileNotFoundError as exc:
        raise ValueError(f"{label} changed while open") from exc
    if stat.S_ISLNK(entry.st_mode):
        raise ValueError(f"{label} cannot be a symlink")
    if not stat.S_ISREG(entry.st_mode):
        raise ValueError(f"{label} is not a regular file")
    if _file_node_tuple(entry) != _file_node_tuple(opened):
        raise ValueError(f"{label} changed while open")
    if expected_mode is not None and stat.S_IMODE(entry.st_mode) != expected_mode:
        raise ValueError(f"{label} has unsafe permissions")


def _verify_open_directory_matches_path(path: Path, descriptor: int) -> None:
    try:
        entry = os.stat(path, follow_symlinks=False)
    except FileNotFoundError as exc:
        raise ValueError(f"managed directory changed while opening: {path}") from exc
    if stat.S_ISLNK(entry.st_mode):
        raise ValueError(f"managed directory cannot be a symlink: {path}")
    if not stat.S_ISDIR(entry.st_mode):
        raise ValueError(f"managed path is not a directory: {path}")
    opened = os.fstat(descriptor)
    if _file_node_tuple(entry) != _file_node_tuple(opened):
        raise ValueError(f"managed directory changed while opening: {path}")


def _file_node_tuple(value: os.stat_result) -> tuple[int, int]:
    return value.st_dev, value.st_ino


def _file_identity_tuple(value: os.stat_result) -> tuple[int, int, int, int]:
    return value.st_dev, value.st_ino, value.st_size, value.st_mtime_ns


def _fsync_descriptor(descriptor: int) -> None:
    os.fsync(descriptor)


def parse_page_control_command(payload: object) -> PageControlCommandValue:
    if isinstance(payload, SubmitMinuteParameterStudy):
        return SubmitMinuteParameterStudy.model_validate(payload)
    if isinstance(payload, Mapping) and payload.get("kind") == "submit_minute_parameter_study":
        return SubmitMinuteParameterStudy.model_validate_json(canonical_json_bytes(payload))
    if isinstance(payload, SubmitMinuteReplay):
        return SubmitMinuteReplay.model_validate(payload)
    if isinstance(payload, Mapping) and payload.get("kind") == "submit_minute_replay":
        return SubmitMinuteReplay.model_validate_json(canonical_json_bytes(payload))
    if isinstance(payload, ExportMinuteReplayZip):
        return ExportMinuteReplayZip.model_validate(payload)
    if isinstance(payload, Mapping) and payload.get("kind") == "export_minute_replay_zip":
        return ExportMinuteReplayZip.model_validate_json(canonical_json_bytes(payload))
    if isinstance(payload, PROMOTION_PUBLIC_TYPES) or (
        isinstance(payload, Mapping) and payload.get("kind") in _STRATEGY_PROMOTION_KINDS
    ):
        raise ValueError("manual promotion requires trusted private submission")
    if isinstance(payload, SetUserRoleCommand) or (isinstance(payload, Mapping) and payload.get("kind") == "set_user_role"):
        raise ValueError("role command requires trusted private confirmation")
    if isinstance(payload, TASK_CONTROL_PUBLIC_TYPES) or (
        isinstance(payload, Mapping) and payload.get("kind") in TASK_CONTROL_KINDS
    ):
        raise ValueError("task controls require trusted private submission")
    if isinstance(payload, (SetPaperAccountPaused, SavePaperPortfolioConfiguration, RunPaperPortfolioResearch)) or (
        isinstance(payload, Mapping) and payload.get("kind") in _PAPER_PORTFOLIO_KINDS
    ):
        raise ValueError("paper portfolio requires trusted submission")
    if isinstance(payload, _CONDITION_PUBLIC_TYPES) or (
        isinstance(payload, Mapping)
        and isinstance(payload.get("kind"), str)
        and payload.get("kind") in _CONDITION_RULE_KINDS
    ):
        raise ValueError("condition rule requires trusted submission")
    if isinstance(payload, (ExecuteScreenQuery, _OwnedSaveNlPreset, _OwnedAppendNlQueryLog)) or (
        isinstance(payload, Mapping)
        and isinstance(payload.get("kind"), str)
        and payload.get("kind") in _SCREEN_QUERY_PRIVATE_KINDS
    ):
        raise ValueError("screen history requires trusted submission")
    if isinstance(
        payload, (SaveStrategyTemplate, ArchiveStrategyTemplate, RunStrategyTemplate)
    ) or (
        isinstance(payload, Mapping)
        and isinstance(payload.get("kind"), str)
        and payload.get("kind") in _STRATEGY_AUTHORING_KINDS
    ):
        raise ValueError("strategy authoring requires trusted submission")
    if isinstance(payload, SaveResearchQuery) or (
        isinstance(payload, Mapping) and payload.get("kind") == "save_research_query"
    ):
        raise ValueError("research queries require trusted submission")
    if isinstance(
        payload, (SaveFactorDefinition, ArchiveFactor, SubmitFactorRun, SetFactorTracked)
    ):
        raise ValueError("factor commands require trusted submission")
    if isinstance(payload, Mapping):
        kind = payload.get("kind")
        if isinstance(kind, str) and kind in _PRICE_RULE_KINDS:
            raise ValueError("price rule commands require trusted submission")
        if (
            isinstance(kind, str)
            and kind in _FACTOR_DEFINITION_KINDS | _FACTOR_RUN_KINDS | _FACTOR_TRACKING_KINDS
        ):
            raise ValueError("factor commands require trusted submission")
    return _COMMAND_ADAPTER.validate_python(payload)


__all__ = [
    "AckAlert",
    "AddWatchlistItem",
    "AddPoolToCanvas",
    "AlertAcknowledgment",
    "AppendNlQueryLog",
    "ArchiveFactor",
    "CreateCanvas",
    "DEFAULT_PAGE_CONTROL_SERVICE_ID",
    "DeleteCanvas",
    "DeletePriceAlertRule",
    "DeleteUserPool",
    "DiscardLabArtifactZip",
    "ExportLabArtifactZip",
    "ForkBuiltinPool",
    "InitializeLabExports",
    "LabArtifactZipResult",
    "LabPageControlBackend",
    "BackfillPlanPageControlBackend",
    "DataAuditReportPageControlBackend",
    "FormulaMarketPageControlBackend",
    "FormulaPoolPageControlBackend",
    "FactorDefinitionPageControlBackend",
    "FactorDefinitionRequestValue",
    "PageControlCommandValue",
    "PageControlClient",
    "PageControlCommandConflictError",
    "PageControlConsumer",
    "PageControlOutbox",
    "PageControlReceipt",
    "PageControlService",
    "PageControlStatus",
    "PageControlUnavailableError",
    "parse_page_control_command",
    "RemoveWatchlistItem",
    "SaveCanvas",
    "SaveFactorDefinition",
    "SavePriceAlertRule",
    "SaveFormulaPoolV1",
    "SaveNlPreset",
    "SaveUserPool",
    "SaveUserPoolV2",
    "SaveUserPoolV3",
    "SetCanvasPoolRefs",
    "SetPriceAlertRuleEnabled",
    "SubmitLabCommand",
    "SubmitBackfillPlan",
    "SubmitDataAuditReport",
    "SubmitFormulaMarketRun",
]
