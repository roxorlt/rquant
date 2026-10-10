"""Immutable owner-private import drafts in the original PageControl database."""

from __future__ import annotations

import secrets
import sqlite3
from contextlib import closing
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Literal, Self

from pydantic import Field, model_validator

from rquant.alert_rule_contracts import (
    ConditionAlertMarketScope,
    ConditionAlertOrigin,
    ConditionAlertSourcePolicy,
)
from rquant.llm.schemas import RuleCall
from rquant.runtime_contracts import (
    AwareUtcDatetime,
    RuntimeContractModel,
    canonical_sha256,
    normalize_aware_utc,
)
from rquant.screen.intraday_reference import Sha256
from rquant.screen.query_contracts import ExecuteScreenQuery, ScreenQueryDefinition
from rquant.web.models.screen import ScreenRankingPlan, ScreenRunData

if TYPE_CHECKING:
    from rquant.screen.query_history import ScreenQueryHistory

MAX_DRAFT_BYTES = 256 * 1024
MAX_ACTIVE_DRAFTS = 512


class ScreenAlertDraftRequest(RuntimeContractModel):
    command_id: str = Field(min_length=1, max_length=128)
    execution_id: str = Field(min_length=1, max_length=128)
    command_hash: Sha256


class ScreenAlertDraftCapabilities(RuntimeContractModel):
    condition_count: int = Field(ge=1, le=26)
    ranking_imported: bool
    consumer_state: Literal["awaiting_consumer"] = "awaiting_consumer"
    message: str = "提醒草稿已生成，尚未生效。"


class ScreenAlertDraft(RuntimeContractModel):
    schema_version: Literal[1] = 1
    draft_id: str = Field(pattern=r"^[0-9a-f]{24}$")
    created_at: AwareUtcDatetime
    expires_at: AwareUtcDatetime
    suggested_name: str = Field(min_length=1, max_length=80)
    origin: ConditionAlertOrigin
    definition: ScreenQueryDefinition
    conditions: tuple[RuleCall, ...] = Field(min_length=1, max_length=26)
    ranking: ScreenRankingPlan | None
    preferred_scope: ConditionAlertMarketScope = ConditionAlertMarketScope()
    source_policy: ConditionAlertSourcePolicy = ConditionAlertSourcePolicy()
    capabilities: ScreenAlertDraftCapabilities
    content_hash: Sha256 | None = None

    @model_validator(mode="after")
    def require_frozen_origin_and_body(self) -> Self:
        if (
            self.expires_at != self.created_at + timedelta(hours=24)
            or self.origin.draft_id != self.draft_id
            or self.conditions != self.definition.conditions
            or self.ranking != self.definition.ranking
            or self.origin.definition_hash != canonical_sha256(self.definition)
            or self.origin.source_identity != self.definition.source_identity
            or self.origin.mode != self.definition.mode
            or self.origin.trade_date != self.definition.trade_date
            or self.origin.cutoff != self.definition.cutoff
            or self.capabilities.condition_count != len(self.conditions)
            or self.capabilities.ranking_imported != (self.ranking is not None)
        ):
            raise ValueError("alert draft origin or definition changed")
        expected = canonical_sha256(self.model_dump(mode="python", exclude={"content_hash"}))
        if self.content_hash is None:
            object.__setattr__(self, "content_hash", expected)
        elif self.content_hash != expected:
            raise ValueError("alert draft content hash changed")
        return self


def install_screen_alert_draft_table(connection: sqlite3.Connection) -> None:
    connection.execute("""CREATE TABLE IF NOT EXISTS screen_alert_draft(
        owner_id TEXT NOT NULL,command_id TEXT NOT NULL,request_hash TEXT NOT NULL,
        draft_id TEXT NOT NULL UNIQUE,body_json TEXT NOT NULL,expires_at TEXT NOT NULL,
        PRIMARY KEY(owner_id,command_id))""")


def _read_draft_body(body: str) -> ScreenAlertDraft:
    if len(body.encode("utf-8")) > MAX_DRAFT_BYTES:
        raise ValueError("alert draft exceeds its byte bound")
    return ScreenAlertDraft.model_validate_json(body)


def create_screen_alert_draft(
    history: ScreenQueryHistory, *, owner_id: str, request: ScreenAlertDraftRequest, now: datetime
) -> ScreenAlertDraft:
    history.scope_tag(owner_id)
    history._assert_private_database()
    observed = normalize_aware_utc(now)
    request_hash = canonical_sha256(request)
    with closing(history.outbox._connect()) as connection, connection:
        connection.execute("BEGIN IMMEDIATE")
        existing = connection.execute(
            "SELECT request_hash,body_json FROM screen_alert_draft "
            "WHERE owner_id=? AND command_id=?",
            [owner_id, request.command_id],
        ).fetchone()
        if existing is not None:
            if existing[0] != request_hash:
                raise ValueError("alert draft command changed")
            prior = _read_draft_body(existing[1])
            if observed >= prior.expires_at:
                raise ValueError("alert draft expired")
            return prior
        history.ensure_capacity()
        row = connection.execute(
            "SELECT c.status,c.command_hash,e.facts_json,e.original_command_json "
            "FROM page_control_command c JOIN screen_query_execution e "
            "ON e.execution_id=c.command_id WHERE e.owner_id=? AND e.execution_id=?",
            [owner_id, request.execution_id],
        ).fetchone()
        if row is None or row[0] != "succeeded" or row[1] != request.command_hash or row[2] is None:
            raise ValueError("alert draft requires an actual owned successful execution")
        execution = history.detail(owner_id, request.execution_id)
        original = ExecuteScreenQuery.model_validate_json(row[3])
        if (
            execution is None
            or execution.status != "succeeded"
            or execution.command_hash != request.command_hash
            or execution.original_command != original
            or original.command_id != request.execution_id
            or original.definition != execution.definition
            or execution.completed_at is None
            or execution.completed_at > observed
            or execution.source is None
            or execution.source.identity != execution.definition.source_identity
            or execution.artifact_sha256 is None
            or execution.member_rank_sha256 is None
            or any(
                value is None
                for value in (execution.base_count, execution.total, execution.unknown_count)
            )
        ):
            raise ValueError("alert draft execution proof is incomplete")
        result = ScreenRunData.model_validate_json(
            history._read_artifact(execution.artifact_sha256)
        )
        if (
            result.status != "ready"
            or result.source != execution.source
            or result.trade_date != execution.definition.trade_date
            or canonical_sha256(result.rows) != execution.member_rank_sha256
            or (result.base_count, result.total, result.unknown_count, result.ranked_count)
            != (
                execution.base_count,
                execution.total,
                execution.unknown_count,
                execution.ranked_count,
            )
        ):
            raise ValueError("alert draft persisted result changed")
        count = connection.execute(
            "SELECT count(*) FROM screen_alert_draft WHERE owner_id=? AND expires_at>?",
            [owner_id, observed.isoformat()],
        ).fetchone()[0]
        if count >= MAX_ACTIVE_DRAFTS:
            raise ValueError("active alert draft bound exceeded")
        identifier = secrets.token_hex(12)
        definition = execution.definition
        minimum = (
            4
            if any(
                value in {"INTRADAY_SPEED_5M[0]", "INTRADAY_VOLUME_RATIO[0]"}
                for call in definition.conditions
                for value in call.args.values()
                if type(value) is str
            )
            else 3
        )
        draft = ScreenAlertDraft(
            draft_id=identifier,
            created_at=observed,
            expires_at=observed + timedelta(hours=24),
            suggested_name=definition.description.strip()[:80] or "选股条件提醒",
            definition=definition,
            conditions=definition.conditions,
            ranking=definition.ranking,
            source_policy=ConditionAlertSourcePolicy(minimum_intraday_contract_version=minimum),
            origin=ConditionAlertOrigin(
                draft_id=identifier,
                execution_id=execution.execution_id,
                command_hash=execution.command_hash,
                definition_hash=canonical_sha256(definition),
                result_digest=execution.artifact_sha256,
                member_rank_digest=execution.member_rank_sha256,
                source_identity=definition.source_identity,
                mode=definition.mode,
                trade_date=definition.trade_date,
                cutoff=definition.cutoff,
            ),
            capabilities=ScreenAlertDraftCapabilities(
                condition_count=len(definition.conditions),
                ranking_imported=definition.ranking is not None,
            ),
        )
        body = draft.model_dump_json()
        if len(body.encode("utf-8")) > MAX_DRAFT_BYTES:
            raise ValueError("alert draft exceeds its byte bound")
        connection.execute(
            "INSERT INTO screen_alert_draft VALUES(?,?,?,?,?,?)",
            [
                owner_id,
                request.command_id,
                request_hash,
                identifier,
                body,
                draft.expires_at.isoformat(),
            ],
        )
        return draft


def read_screen_alert_draft(
    history: ScreenQueryHistory, *, owner_id: str, draft_id: str, now: datetime
) -> ScreenAlertDraft | None:
    history.scope_tag(owner_id)
    history._assert_private_database()
    if len(draft_id) != 24 or any(character not in "0123456789abcdef" for character in draft_id):
        return None
    with closing(history.outbox._connect()) as connection:
        row = connection.execute(
            "SELECT body_json FROM screen_alert_draft WHERE owner_id=? AND draft_id=?",
            [owner_id, draft_id],
        ).fetchone()
    if row is None:
        return None
    draft = _read_draft_body(row[0])
    if draft.draft_id != draft_id:
        raise ValueError("alert draft stored identity changed")
    return draft if normalize_aware_utc(now) < draft.expires_at else None
