"""Owner-bound condition commands on the original PageControl journal."""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Callable
from datetime import datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Protocol

from fastapi import HTTPException, Request
from loguru import logger
from pydantic import BaseModel, ConfigDict

from rquant.alert_rule_contracts import ConditionAlertRuleDefinition, ConditionAlertScopeEvidence
from rquant.condition_alert_rule_store import ConditionAlertRuleRepository, ConditionAlertRuleUpsert
from rquant.page_control import (
    DeleteAlertRule,
    PageControlStatus,
    SaveAlertRule,
    SetAlertRuleEnabled,
)
from rquant.price_alert_admission import (
    PriceAlertAdmissionRejectedError,
    PriceAlertAdmissionUnavailableError,
)
from rquant.price_alert_rule_store import (
    PriceAlertRuleCapacityError,
    PriceAlertRuleKey,
    PriceAlertRuleScopeError,
    PriceAlertRuleVersionConflictError,
)
from rquant.runtime_contracts import normalize_aware_utc
from rquant.web.envelope import ServingState
from rquant.web.models.condition_alert_rules import (
    ConditionAlertRuleCommandReceipt,
    ConditionAlertRuleCommandRequest,
)
from rquant.web.price_alert_commands import _same_pointer, private_meta

if TYPE_CHECKING:
    from rquant.page_control import PageControlClaim, PageControlOutbox, PageControlReceipt


class ConditionRuleScopeResolution(BaseModel):
    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)
    evidence: ConditionAlertScopeEvidence | None
    is_current: Callable[[], bool]
    consumer_ready: bool = False


class ConditionRuleScopeResolver(Protocol):
    def __call__(
        self, owner_id: str, rule: ConditionAlertRuleDefinition, now: datetime
    ) -> ConditionRuleScopeResolution: ...


def condition_scope_resolver(serving_root: Path) -> ConditionRuleScopeResolver:
    from pathlib import Path

    from rquant.condition_alert_runtime_projection import (
        condition_consumer_ready,
        resolve_condition_scope,
    )
    from rquant.serving_publisher import ServingReader
    from rquant.web.serving import BorrowedGeneration

    root = Path(serving_root)

    def resolve(
        owner_id: str, rule: ConditionAlertRuleDefinition, now: datetime
    ) -> ConditionRuleScopeResolution:
        reader = ServingReader(root)
        with reader.acquire_generation() as lease:
            cursor = lease.connection.cursor()
            try:
                borrowed = BorrowedGeneration(
                    manifest=lease.manifest,
                    pointer=lease.pointer,
                    cursor=cursor,
                    fallback_detail=None,
                )
                scope = resolve_condition_scope(borrowed, owner_id=owner_id, rule=rule, now=now)
                ready = condition_consumer_ready(borrowed, now=now)
                pointer = lease.pointer
            finally:
                cursor.close()
        return ConditionRuleScopeResolution(
            evidence=scope,
            is_current=lambda: reader.current_pointer() == pointer,
            consumer_ready=ready,
        )

    return resolve


def _validate_origin(
    connection: sqlite3.Connection,
    *,
    owner_id: str,
    rule: ConditionAlertRuleDefinition,
    previous: ConditionAlertRuleDefinition | None,
    now: datetime,
) -> None:
    import sqlite3

    from rquant.screen.alert_draft import ScreenAlertDraft

    if not isinstance(connection, sqlite3.Connection):
        raise TypeError("condition origin must use the original borrowed transaction")
    origin = rule.origin
    if origin is None:
        return
    if previous is not None and previous.origin == origin:
        return
    if origin.draft_id is None:
        raise PriceAlertRuleScopeError("condition origin requires its owned actual draft")
    row = connection.execute(
        "SELECT body_json FROM screen_alert_draft WHERE owner_id=? AND draft_id=?",
        (owner_id, origin.draft_id),
    ).fetchone()
    draft = None if row is None else ScreenAlertDraft.model_validate_json(row[0])
    if draft is None or draft.origin != origin or draft.created_at > now or draft.expires_at <= now:
        raise PriceAlertRuleScopeError(
            "condition import origin is unavailable or belongs to another owner"
        )


def complete_condition_rule(
    outbox: PageControlOutbox,
    claim: PageControlClaim,
    *,
    now: datetime,
    resolve_scope: ConditionRuleScopeResolver | None,
) -> PageControlReceipt:
    from rquant.page_control import (
        _COMMAND_ADAPTER,
        _CONDITION_OWNED_TYPES,
        PageControlEffectStatus,
        PageControlStatus,
        _command_hash,
        _OwnedSaveAlertRule,
        _OwnedSetAlertRuleEnabled,
    )

    command = claim.command
    if type(command) not in _CONDITION_OWNED_TYPES:
        raise TypeError("condition completion requires exact owned claim")
    observed = normalize_aware_utc(now)
    completed_at = observed.isoformat(timespec="microseconds")
    digest = _command_hash(command)
    rule_id = command.rule.rule_id if type(command) is _OwnedSaveAlertRule else command.rule_id
    action = (
        "save"
        if type(command) is _OwnedSaveAlertRule
        else "set_enabled"
        if type(command) is _OwnedSetAlertRuleEnabled
        else "delete"
    )
    resolution = None
    with outbox._connect() as connection:
        connection.execute("BEGIN IMMEDIATE")
        row = connection.execute(
            "SELECT * FROM page_control_command WHERE command_id=?", (command.command_id,)
        ).fetchone()
        if (
            row is None
            or row["command_kind"] != command.kind
            or row["command_hash"] != digest
            or _COMMAND_ADAPTER.validate_json(row["payload_json"]) != command
        ):
            raise ValueError("condition original request or owner changed")
        if (
            row["status"] != PageControlStatus.PROCESSING.value
            or row["processing_owner"] != claim.owner_id
            or row["claim_token"] != claim.claim_token
            or row["lease_expires_at"] is None
            or row["lease_expires_at"] <= completed_at
        ):
            raise RuntimeError("stale condition claim cannot complete")
        outbox._require_condition_rule_activation(connection, command.owner_id)
        repo = ConditionAlertRuleRepository(connection)
        current = repo.get(PriceAlertRuleKey(owner_id=command.owner_id, rule_id=rule_id))
        status, error = PageControlStatus.SUCCEEDED, None
        try:
            if command.requested_at > observed + timedelta(minutes=5):
                raise PriceAlertRuleScopeError("condition requested time is future")
            if action == "delete":
                entry = repo.delete_current(
                    PriceAlertRuleKey(owner_id=command.owner_id, rule_id=rule_id),
                    expected_version=command.expected_version,
                    now=observed,
                )
            else:
                if action == "save":
                    rule = command.rule
                else:
                    if (
                        current is None
                        or current.deleted
                        or current.version != command.expected_version
                        or current.rule is None
                    ):
                        raise PriceAlertRuleVersionConflictError("condition rule version changed")
                    rule = ConditionAlertRuleDefinition.model_validate(
                        {**current.rule.model_dump(mode="python"), "enabled": command.enabled}
                    )
                _validate_origin(
                    connection,
                    owner_id=command.owner_id,
                    rule=rule,
                    previous=None if current is None else current.rule,
                    now=observed,
                )
                if rule.enabled:
                    if resolve_scope is None:
                        raise PriceAlertRuleScopeError("condition source is not configured")
                    try:
                        resolution = resolve_scope(command.owner_id, rule, observed)
                    except (OSError, ValueError, RuntimeError) as exc:
                        raise PriceAlertRuleScopeError("condition scope is unavailable") from exc
                    if (
                        type(resolution) is not ConditionRuleScopeResolution
                        or not resolution.consumer_ready
                        or not resolution.is_current()
                    ):
                        raise PriceAlertRuleScopeError("condition scope changed")
                entry = repo.upsert(
                    ConditionAlertRuleUpsert(
                        owner_id=command.owner_id,
                        expected_version=command.expected_version,
                        rule=rule,
                    ),
                    now=observed,
                    scope=None if resolution is None else resolution.evidence,
                )
            result = {
                "rule_id": rule_id,
                "action": action,
                "version": entry.version,
                "deleted": entry.deleted,
                "enabled": None if entry.rule is None else entry.rule.enabled,
                "rule_body_hash": None if entry.rule is None else entry.rule.rule_body_hash,
                "scope_version": None
                if resolution is None or resolution.evidence is None
                else resolution.evidence.scope_version,
            }
            outbox._condition_rule_failpoint("head")
        except (
            PriceAlertRuleVersionConflictError,
            PriceAlertRuleScopeError,
            PriceAlertRuleCapacityError,
        ) as rejection:
            status = PageControlStatus.FAILED
            error = "condition rule command rejected"
            result = {
                "rule_id": rule_id,
                "action": action,
                "code": "version_conflict"
                if isinstance(rejection, PriceAlertRuleVersionConflictError)
                else "scope_invalid"
                if isinstance(rejection, PriceAlertRuleScopeError)
                else "capacity_exceeded",
            }
        encoded = json.dumps(result, ensure_ascii=True)
        connection.execute(
            "INSERT INTO page_control_effect(command_id,command_hash,effect_k"
            "ind,status,owner_id,claim_token,started_at,completed_at,result_j"
            "son,error) VALUES(?,?,?,?,?,?,?,?,?,?)",
            (
                command.command_id,
                digest,
                command.kind,
                PageControlEffectStatus.SUCCEEDED.value
                if status is PageControlStatus.SUCCEEDED
                else PageControlEffectStatus.FAILED.value,
                claim.owner_id,
                claim.claim_token,
                completed_at,
                completed_at,
                encoded,
                error,
            ),
        )
        outbox._condition_rule_failpoint("effect")
        changed = connection.execute(
            "UPDATE page_control_command SET status=?,completed_at=?,result_j"
            "son=?,error=?,processing_owner=NULL,lease_expires_at=NULL,claim_"
            "token=NULL WHERE command_id=? AND status=? AND processing_owner="
            "? AND claim_token=? AND lease_expires_at>?",
            (
                status.value,
                completed_at,
                encoded,
                error,
                command.command_id,
                PageControlStatus.PROCESSING.value,
                claim.owner_id,
                claim.claim_token,
                completed_at,
            ),
        ).rowcount
        if changed != 1:
            raise RuntimeError("condition claim changed during completion")
        outbox._condition_rule_failpoint("receipt")
        if resolution is not None and not resolution.is_current():
            raise RuntimeError("condition scope changed before commit")
        outbox._condition_rule_failpoint("before_commit")
        completed = connection.execute(
            "SELECT * FROM page_control_command WHERE command_id=?", (command.command_id,)
        ).fetchone()
    return outbox._receipt(completed)


ConditionRuleCommand = SaveAlertRule | SetAlertRuleEnabled | DeleteAlertRule


class ConditionRuleTransport(Protocol):
    def lookup(
        self, command: ConditionRuleCommand, *, authenticated_owner_id: str
    ) -> PageControlReceipt | None: ...
    def submit(
        self, command: ConditionRuleCommand, *, authenticated_owner_id: str
    ) -> PageControlReceipt: ...
    def resume(
        self, command: ConditionRuleCommand, *, authenticated_owner_id: str
    ) -> PageControlReceipt: ...


def domain_condition_command(body: ConditionAlertRuleCommandRequest) -> ConditionRuleCommand:
    common = {
        "command_id": body.command_id,
        "requested_at": body.requested_at,
        "expected_version": body.expected_version,
    }
    if body.action == "save":
        if body.rule is None:
            raise ValueError("condition save requires a full definition")
        return SaveAlertRule(**common, rule=body.rule)
    if body.action == "set_enabled":
        return SetAlertRuleEnabled(**common, rule_id=body.rule_id, enabled=body.enabled)
    return DeleteAlertRule(**common, rule_id=body.rule_id)


def new_condition_preflight(
    request: Request, body: ConditionAlertRuleCommandRequest, owner: str
) -> None:
    from rquant.condition_alert_runtime_projection import (
        condition_consumer_ready,
        read_condition_rule_authority,
        resolve_condition_scope,
    )

    web = request.app.state.web
    web.tracker.refresh()
    now = normalize_aware_utc(web.clock())
    try:
        with web.tracker.borrow() as borrowed:
            if (
                borrowed is None
                or private_meta(request, borrowed, now).state is not ServingState.READY
            ):
                raise HTTPException(503, "规则暂不可用，请稍后重试。")
            if borrowed.manifest.generation_id != body.generation_id:
                raise HTTPException(409, "规则已更新，请刷新后重试。")
            authority = read_condition_rule_authority(borrowed, now=now)
            if authority is None:
                raise HTTPException(503, "规则操作暂未开放。")
            owned = tuple(row for row in authority.rows if row.owner_id == owner)
            head = next((row for row in owned if row.rule_id == body.rule_id), None)
            if (None if head is None else head.version) != body.expected_version:
                raise HTTPException(409, "规则已更新，请刷新后重试。")
            if body.action != "save" and (head is None or head.deleted):
                raise HTTPException(409, "规则已删除，请刷新后重试。")
            rule = (
                body.rule
                if body.action == "save"
                else None
                if body.action == "delete"
                else ConditionAlertRuleDefinition.model_validate(
                    {**head.rule.model_dump(mode="python"), "enabled": body.enabled}
                )
            )
            if rule is not None:
                if (head is None or head.deleted) and sum(not row.deleted for row in owned) >= 100:
                    raise HTTPException(409, "规则已满，请删除其他规则后重试。")
                if len(rule.model_dump_json().encode()) > 48 * 1024:
                    raise HTTPException(422, "规则过长，请减少内容。")
                if rule.enabled:
                    if not condition_consumer_ready(borrowed, now=now):
                        raise HTTPException(503, "提醒暂不可运行，可先保存为停用。")
                    resolve_condition_scope(borrowed, owner_id=owner, rule=rule, now=now)
            if not _same_pointer(request, borrowed):
                raise HTTPException(409, "规则已更新，请刷新后重试。")
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("Condition rule preflight unavailable")
        raise HTTPException(503, "范围或行情暂不可用，请刷新后重试。") from exc


def _condition_publication(
    request: Request,
    body: ConditionAlertRuleCommandRequest,
    owner: str,
    receipt: PageControlReceipt,
    version: int,
) -> str:
    from rquant.condition_alert_runtime_projection import read_condition_rule_authority

    web = request.app.state.web
    web.tracker.refresh()
    now = normalize_aware_utc(web.clock())
    try:
        with web.tracker.borrow() as borrowed:
            if (
                borrowed is None
                or private_meta(request, borrowed, now).state is not ServingState.READY
                or borrowed.manifest.generation_id == body.generation_id
                or borrowed.manifest.built_at <= receipt.completed_at
            ):
                return "saved_syncing"
            authority = read_condition_rule_authority(borrowed, now=now)
            if authority is None or not _same_pointer(request, borrowed):
                return "saved_syncing"
            head = next(
                (
                    row
                    for row in authority.rows
                    if row.owner_id == owner and row.rule_id == body.rule_id
                ),
                None,
            )
            if head is not None and head.version > version:
                return "superseded"
            if head is None or head.version != version:
                return "saved_syncing"
            if body.action == "delete":
                return "published" if head.deleted else "saved_syncing"
            if head.deleted or head.rule is None:
                return "saved_syncing"
            if body.action == "set_enabled":
                return "published" if head.rule.enabled is body.enabled else "saved_syncing"
            return "published" if head.rule == body.rule else "saved_syncing"
    except Exception:
        logger.exception("Condition publication cannot be confirmed")
        return "saved_syncing"


def condition_reply(
    body: ConditionAlertRuleCommandRequest, status: str, message: str, *, version: int | None = None
) -> ConditionAlertRuleCommandReceipt:
    return ConditionAlertRuleCommandReceipt.model_validate(
        {
            "command_id": body.command_id,
            "rule_id": body.rule_id,
            "action": body.action,
            "status": status,
            "version": version,
            "message": message,
        }
    )


def verified_condition_receipt(
    request: Request,
    body: ConditionAlertRuleCommandRequest,
    owner: str,
    receipt: PageControlReceipt,
) -> ConditionAlertRuleCommandReceipt:
    if receipt.command_id != body.command_id or receipt.enqueued_at != body.requested_at:
        raise ValueError("condition original receipt identity differs")
    if receipt.status not in (PageControlStatus.SUCCEEDED, PageControlStatus.FAILED):
        status = (
            "uncertain" if receipt.status is PageControlStatus.AMBIGUOUS else receipt.status.value
        )
        return condition_reply(
            body,
            status,
            {
                "pending": "等待处理。",
                "processing": "正在处理。",
                "uncertain": "状态待核对，请继续核对原操作。",
            }[status],
        )
    result = receipt.result
    if (
        receipt.completed_at is None
        or not isinstance(result, dict)
        or (result.get("rule_id"), result.get("action")) != (body.rule_id, body.action)
    ):
        raise ValueError("condition terminal receipt identity differs")
    if receipt.status is PageControlStatus.FAILED:
        status = {
            "version_conflict": "conflict",
            "scope_invalid": "scope_invalid",
            "capacity_exceeded": "capacity",
        }.get(result.get("code"), "failed")
        return condition_reply(
            body,
            status,
            {
                "conflict": "规则已更新，请刷新后重试。",
                "scope_invalid": "范围或行情暂不可用，请重新核对。",
                "capacity": "规则已满，请删除其他规则。",
                "failed": "操作未完成，请检查后重试。",
            }[status],
        )
    version = result.get("version")
    enabled = (
        None
        if body.action == "delete"
        else body.enabled
        if body.action == "set_enabled"
        else body.rule.enabled
    )
    if (
        type(version) is not int
        or version != (body.expected_version or 0) + 1
        or result.get("deleted") is not (body.action == "delete")
        or result.get("enabled") is not enabled
    ):
        raise ValueError("condition result differs from its exact intent")
    if body.action == "save" and result.get("rule_body_hash") != body.rule.rule_body_hash:
        raise ValueError("condition result definition differs")
    status = _condition_publication(request, body, owner, receipt, version)
    return condition_reply(
        body,
        status,
        {
            "published": "设置已保存。运行状态请查看规则。",
            "saved_syncing": "设置已写入，等待同步。",
            "superseded": "规则已更新，请查看当前设置。",
        }[status],
        version=version,
    )


def execute_condition_rule(
    request: Request, body: ConditionAlertRuleCommandRequest, owner: str, *, resume_only: bool
) -> tuple[ConditionAlertRuleCommandReceipt, int]:
    admission: ConditionRuleTransport | None = getattr(
        request.app.state.web, "price_alert_admission", None
    )
    if admission is None:
        return condition_reply(body, "rejected", "规则操作暂未开放。"), 503
    command = domain_condition_command(body)

    def uncertain() -> tuple[ConditionAlertRuleCommandReceipt, int]:
        return condition_reply(body, "uncertain", "状态待核对，请继续核对原操作。"), 503

    def existing(receipt: PageControlReceipt) -> tuple[ConditionAlertRuleCommandReceipt, int]:
        if receipt.status in (PageControlStatus.PENDING, PageControlStatus.PROCESSING):
            try:
                receipt = admission.resume(command, authenticated_owner_id=owner)
            except (PriceAlertAdmissionUnavailableError, OSError):
                receipt = admission.lookup(command, authenticated_owner_id=owner)
                if receipt is None:
                    return uncertain()
        try:
            reply = verified_condition_receipt(request, body, owner, receipt)
        except (ValueError, TypeError):
            return condition_reply(body, "uncertain", "状态待核对，请继续核对原操作。"), 502
        return reply, 409 if reply.status in {"conflict", "scope_invalid", "capacity"} else 200

    try:
        original = admission.lookup(command, authenticated_owner_id=owner)
        if original is not None:
            return existing(original)
        if resume_only:
            return condition_reply(body, "not_found", "暂未查到原操作，请保留记录并稍后核对。"), 404
        try:
            new_condition_preflight(request, body, owner)
        except HTTPException as exc:
            original = admission.lookup(command, authenticated_owner_id=owner)
            if original is not None:
                return existing(original)
            return condition_reply(
                body, "conflict" if exc.status_code == 409 else "rejected", str(exc.detail)
            ), exc.status_code
        try:
            receipt = admission.submit(command, authenticated_owner_id=owner)
        except (PriceAlertAdmissionUnavailableError, OSError):
            receipt = admission.lookup(command, authenticated_owner_id=owner)
            if receipt is None:
                return uncertain()
        return existing(receipt)
    except (PriceAlertAdmissionUnavailableError, OSError):
        return uncertain()
    except (PriceAlertAdmissionRejectedError, ValueError):
        return condition_reply(body, "conflict", "原操作内容无法核对，请保留记录并刷新。"), 409
