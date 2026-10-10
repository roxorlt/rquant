"""Typed read model and the two browser-owned pool editor command bodies."""

from __future__ import annotations

from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, JsonValue, model_validator

from rquant.runtime_contracts import AwareUtcDatetime
from rquant.screen.pool_ranking import PoolRankingPlan

_NAME = r"^[\w\u4e00-\u9fff-]+$"
_USER_POOL = r"^user/[\w\u4e00-\u9fff-]+$"
_SHA256 = r"^[0-9a-f]{64}$"


class EditorRuleCall(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    name: str = Field(min_length=1, max_length=64)
    args: dict[str, JsonValue]


class EditablePool(BaseModel):
    model_config = ConfigDict(frozen=True)

    key: str
    display_name: str
    description: str
    version: str
    depends_on: str | None
    delay_days: int
    rule_calls: list[EditorRuleCall]
    include_columns: list[str]
    ranking: PoolRankingPlan | None = None
    save_kind: Literal["save_user_pool_v2", "save_user_pool_v3"] = "save_user_pool_v2"


class BuiltinPoolCopySource(BaseModel):
    model_config = ConfigDict(frozen=True)

    key: str
    display_name: str
    description: str
    version: str
    depends_on: str | None
    delay_mode: Literal["none", "exact", "legacy_window"]
    delay_days: int
    rule_calls: list[EditorRuleCall]
    include_columns: list[str]
    ranking: PoolRankingPlan | None = None
    copyable: bool
    copy_block_reason: str | None


class EditableCanvas(BaseModel):
    model_config = ConfigDict(frozen=True)

    name: str
    description: str
    version: str
    pool_refs: list[str]
    command_id: str
    record_hash: str


class PoolEditorData(BaseModel):
    model_config = ConfigDict(frozen=True)

    state: Literal["ready", "unavailable"]
    pools: list[EditablePool]
    copy_sources: list[BuiltinPoolCopySource]
    canvases: list[EditableCanvas]
    canvas_create_available: bool
    nl_preview_available: bool = False


class PoolNlPreviewRequest(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    pool_key: str = Field(min_length=1, max_length=100)
    generation_id: str = Field(min_length=1, max_length=128)
    expected_version: str = Field(pattern=_SHA256)
    instruction: str = Field(min_length=1, max_length=500)

    @model_validator(mode="after")
    def validate_instruction(self) -> PoolNlPreviewRequest:
        if not self.instruction.strip():
            raise ValueError("instruction must not be blank")
        return self


class PoolRuleChange(BaseModel):
    model_config = ConfigDict(frozen=True)

    kind: Literal["added", "removed", "parameter_changed"]
    label: str
    before: EditorRuleCall | None = None
    after: EditorRuleCall | None = None


class PoolNlPreview(BaseModel):
    model_config = ConfigDict(frozen=True)

    pool_key: str
    base_generation_id: str
    base_version: str
    rule_calls: list[EditorRuleCall]
    changes: list[PoolRuleChange]
    message: str | None = None


class _EditorCommand(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    command_id: str = Field(min_length=1, max_length=128, pattern=r"^[A-Za-z0-9._-]+$")
    requested_at: AwareUtcDatetime


class SavePoolCommand(_EditorCommand):
    kind: Literal["save_user_pool_v2"] = "save_user_pool_v2"
    base_name: str = Field(min_length=1, max_length=80, pattern=_NAME)
    display_name: str = Field(min_length=1, max_length=80)
    description: str = Field(default="", max_length=1_024)
    rule_calls: list[EditorRuleCall] = Field(min_length=1, max_length=32)
    include_columns: list[str] = Field(default_factory=list, max_length=64)
    depends_on: str | None = Field(default=None, max_length=100)
    delay_days: int = Field(default=0, ge=0, le=252)
    expected_version: str | None = Field(default=None, pattern=_SHA256)

    @model_validator(mode="after")
    def validate_parent_delay(self) -> SavePoolCommand:
        if (self.depends_on is None and self.delay_days != 0) or (
            self.depends_on is not None and self.delay_days == 0
        ):
            raise ValueError("parent pool and exact delay must agree")
        return self


class SaveRankedPoolCommand(_EditorCommand):
    kind: Literal["save_user_pool_v3"] = "save_user_pool_v3"
    base_name: str = Field(min_length=1, max_length=80, pattern=_NAME)
    display_name: str = Field(min_length=1, max_length=80)
    description: str = Field(default="", max_length=1_024)
    rule_calls: list[EditorRuleCall] = Field(default_factory=list, max_length=26)
    include_columns: list[str] = Field(default_factory=list, max_length=26)
    depends_on: str | None = Field(default=None, max_length=100)
    delay_days: int = Field(default=0, ge=0, le=252)
    expected_version: str | None = Field(default=None, pattern=_SHA256)
    ranking: PoolRankingPlan | None

    @model_validator(mode="after")
    def validate_parent_delay(self) -> SaveRankedPoolCommand:
        if (self.depends_on is None and self.delay_days != 0) or (
            self.depends_on is not None and self.delay_days == 0
        ):
            raise ValueError("parent pool and exact delay must agree")
        return self


class AttachPoolCommand(_EditorCommand):
    kind: Literal["add_pool_to_canvas"] = "add_pool_to_canvas"
    canvas_name: str = Field(min_length=1, max_length=80, pattern=_NAME)
    pool_name: str = Field(min_length=6, max_length=85, pattern=_USER_POOL)
    expected_pool_version: str = Field(pattern=_SHA256)


class CreateCanvasCommand(_EditorCommand):
    kind: Literal["create_canvas"] = "create_canvas"
    name: str = Field(min_length=1, max_length=80, pattern=_NAME)
    description: str = Field(default="", max_length=1_024)


PoolEditorCommand = Annotated[
    SavePoolCommand | SaveRankedPoolCommand | AttachPoolCommand | CreateCanvasCommand,
    Field(discriminator="kind"),
]


class PoolEditorReceipt(BaseModel):
    model_config = ConfigDict(frozen=True)

    command_id: str
    status: Literal["pending", "processing", "succeeded", "failed", "ambiguous"]
    message: str
    pool_version: str | None = None
    canvas_name: str | None = None
    canvas_record_hash: str | None = None
