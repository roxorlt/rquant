"""Data-only drafts and frozen receipts for the original PageControl host."""

from __future__ import annotations

from typing import Literal, Self
from uuid import UUID

from pydantic import Field, field_validator, model_validator

from rquant.backtest.contracts import Sha256
from rquant.runtime_contracts import AwareUtcDatetime, RuntimeContractModel, canonical_sha256
from rquant.strategy_template import TEMPLATE_ID_PATTERN, StrategyTemplate


class StrategyTemplateHead(RuntimeContractModel):
    version: int = Field(strict=True, ge=1)
    registration_fingerprint: Sha256
    record_hash: Sha256
    spec_fingerprint: Sha256


class StrategyAuthoringIdentity(RuntimeContractModel):
    instance_id: str = Field(pattern=r"^[0-9a-f]{32}$")
    path: str
    st_dev: int
    st_ino: int


class _TemplateCommand(RuntimeContractModel):
    command_id: str
    requested_at: AwareUtcDatetime
    generation_id: str = Field(min_length=1, max_length=128)

    @field_validator("command_id")
    @classmethod
    def validate_command_id(cls, value: str) -> str:
        parsed = UUID(value)
        if str(parsed) != value:
            raise ValueError("command ID must be a canonical UUID")
        return value

    @property
    def request_hash(self) -> str:
        return canonical_sha256(self)


class SaveStrategyTemplate(_TemplateCommand):
    kind: Literal["save_strategy_template"] = "save_strategy_template"
    strategy_id: str | None = Field(default=None, pattern=TEMPLATE_ID_PATTERN)
    expected_head: StrategyTemplateHead | None = None
    name: str = Field(min_length=1, max_length=80)
    change_note: str = Field(default="", max_length=1024)
    rules: StrategyTemplate

    @field_validator("name", "change_note")
    @classmethod
    def validate_printable_copy(cls, value: str) -> str:
        if value and not value.isprintable():
            raise ValueError("strategy copy must be printable")
        return value

    @model_validator(mode="after")
    def validate_lineage(self) -> Self:
        if (self.strategy_id is None) != (self.expected_head is None):
            raise ValueError("existing strategy requires its exact head")
        if self.strategy_id is not None and not self.change_note:
            raise ValueError("new version requires a change note")
        if (
            len(
                self.model_dump_json(exclude={"owner_id", "metadata_identity", "accepted"}).encode()
            )
            > 32 * 1024
        ):
            raise ValueError("complete saved strategy exceeds 32 KiB")
        return self


class ArchiveStrategyTemplate(_TemplateCommand):
    kind: Literal["archive_strategy_template"] = "archive_strategy_template"
    strategy_id: str = Field(pattern=TEMPLATE_ID_PATTERN)
    expected_head: StrategyTemplateHead


class AcceptedStrategyTemplateCommand(RuntimeContractModel):
    owner_id: str
    strategy_id: str = Field(pattern=TEMPLATE_ID_PATTERN)
    original_request_hash: Sha256
    request: SaveStrategyTemplate
    version: int = Field(strict=True, ge=1)
    producer_commit: str = Field(pattern=r"^[0-9a-f]{40}$")
    accepted_at: AwareUtcDatetime
    metadata_identity: StrategyAuthoringIdentity


class AcceptedStrategyTemplateArchive(RuntimeContractModel):
    owner_id: str
    original_request_hash: Sha256
    request: ArchiveStrategyTemplate
    accepted_at: AwareUtcDatetime
    metadata_identity: StrategyAuthoringIdentity


class StrategyTemplateReceipt(RuntimeContractModel):
    owner_id: str
    command_id: str
    action: Literal["save", "archive"]
    strategy_id: str = Field(pattern=TEMPLATE_ID_PATTERN)
    head: StrategyTemplateHead
    original_request_hash: Sha256
    completed_at: AwareUtcDatetime


StrategyTemplateCommand = SaveStrategyTemplate | ArchiveStrategyTemplate


class OwnedSaveStrategyTemplate(SaveStrategyTemplate):
    owner_id: str
    metadata_identity: StrategyAuthoringIdentity
    accepted: AcceptedStrategyTemplateCommand

    @model_validator(mode="after")
    def bind_original_request(self) -> Self:
        original = SaveStrategyTemplate.model_validate(
            self.model_dump(mode="python", exclude={"owner_id", "metadata_identity", "accepted"})
        )
        if (
            self.accepted.owner_id != self.owner_id
            or self.accepted.original_request_hash != original.request_hash
            or self.accepted.request != original
            or self.accepted.metadata_identity != self.metadata_identity
        ):
            raise ValueError("owned strategy save differs from original accepted request")
        return self

    def original(self) -> SaveStrategyTemplate:
        return SaveStrategyTemplate.model_validate(
            self.model_dump(mode="python", exclude={"owner_id", "metadata_identity", "accepted"})
        )


class OwnedArchiveStrategyTemplate(ArchiveStrategyTemplate):
    owner_id: str
    metadata_identity: StrategyAuthoringIdentity
    accepted: AcceptedStrategyTemplateArchive

    @model_validator(mode="after")
    def bind_original_request(self) -> Self:
        original = self.original()
        if (
            self.accepted.owner_id != self.owner_id
            or self.accepted.original_request_hash != original.request_hash
            or self.accepted.request != original
            or self.accepted.metadata_identity != self.metadata_identity
        ):
            raise ValueError("owned strategy archive differs from original accepted request")
        return self

    def original(self) -> ArchiveStrategyTemplate:
        return ArchiveStrategyTemplate.model_validate(
            self.model_dump(mode="python", exclude={"owner_id", "metadata_identity", "accepted"})
        )


OwnedStrategyTemplateCommand = OwnedSaveStrategyTemplate | OwnedArchiveStrategyTemplate
