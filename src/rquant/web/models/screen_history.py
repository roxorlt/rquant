"""Public typed actions for private screening; identity is supplied by ingress."""

from __future__ import annotations

from typing import Annotated, Literal

from pydantic import Field

from rquant.page_control import PageControlReceipt, SaveNlPreset
from rquant.pool_result_receipt import DailyWriterCapability, PublishedDailyScreenEvidence
from rquant.runtime_contracts import AwareUtcDatetime, RuntimeContractModel
from rquant.screen.alert_draft import ScreenAlertDraft
from rquant.screen.query_contracts import (
    ExecuteScreenQuery,
    ScreenExecutionResults,
    ScreenPresetDefinition,
    ScreenQueryExecution,
    ScreenQueryPreset,
)
from rquant.web.models.screen_alert_draft import (
    ScreenAlertDraftCreateAction,
    ScreenAlertDraftReadAction,
)


class ScreenExecutionView(ScreenQueryExecution):
    owner_id: None = Field(default=None, exclude=True)


class ScreenHistoryView(RuntimeContractModel):
    owner_scope_tag: str
    items: tuple[ScreenExecutionView, ...]
    next_cursor: str | None


class ScreenPresetSaveRequest(RuntimeContractModel):
    command_id: str = Field(min_length=1, max_length=128)
    requested_at: AwareUtcDatetime
    preset: ScreenPresetDefinition
    expected_version: int | None = Field(default=None, ge=1)

    def legacy_command(self) -> SaveNlPreset:
        definition = self.preset.definition
        return SaveNlPreset(
            command_id=self.command_id,
            requested_at=self.requested_at,
            name=self.preset.preset_id,
            description=definition.description,
            rule_calls=definition.conditions,
            overwrite=self.expected_version is not None,
        )


class ScreenExecuteAction(RuntimeContractModel):
    action: Literal["execute"] = "execute"
    command: ExecuteScreenQuery


class ScreenPresetSaveAction(RuntimeContractModel):
    action: Literal["presets_save"] = "presets_save"
    request: ScreenPresetSaveRequest


ScreenOriginalAction = Annotated[
    ScreenExecuteAction | ScreenPresetSaveAction, Field(discriminator="action")
]


class ScreenLookupAction(RuntimeContractModel):
    action: Literal["lookup"] = "lookup"
    original: ScreenOriginalAction


class ScreenResumeAction(RuntimeContractModel):
    action: Literal["resume"] = "resume"
    original: ScreenOriginalAction


class ScreenHistoryAction(RuntimeContractModel):
    action: Literal["history"] = "history"
    limit: int = Field(default=20, ge=1, le=100)
    cursor: str | None = Field(default=None, max_length=1024)


class ScreenPresetsAction(RuntimeContractModel):
    action: Literal["presets"] = "presets"


class ScreenExecutionAction(RuntimeContractModel):
    action: Literal["detail"] = "detail"
    execution_id: str = Field(min_length=1, max_length=128)


class ScreenResultsAction(RuntimeContractModel):
    action: Literal["results"] = "results"
    execution_id: str = Field(min_length=1, max_length=128)
    limit: int = Field(default=20, ge=1, le=100)
    cursor: str | None = Field(default=None, max_length=1024)


ScreenQueryAction = Annotated[
    ScreenExecuteAction
    | ScreenPresetSaveAction
    | ScreenLookupAction
    | ScreenResumeAction
    | ScreenHistoryAction
    | ScreenPresetsAction
    | ScreenExecutionAction
    | ScreenResultsAction
    | ScreenAlertDraftCreateAction
    | ScreenAlertDraftReadAction,
    Field(discriminator="action"),
]


class ScreenQueryReadData(RuntimeContractModel):
    available: bool = True
    owner_scope_tag: str
    receipt: PageControlReceipt | None = None
    history: ScreenHistoryView | None = None
    execution: ScreenExecutionView | None = None
    results: ScreenExecutionResults | None = None
    presets: tuple[ScreenQueryPreset, ...] = ()
    daily_writer_capability: DailyWriterCapability | None = None
    daily_run_evidence: tuple[PublishedDailyScreenEvidence, ...] = ()
    alert_draft: ScreenAlertDraft | None = None
