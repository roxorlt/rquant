"""Adapt ownerless Web requests to the original command journal and exact publication."""

from __future__ import annotations

from datetime import datetime, time
from typing import Protocol

from fastapi import HTTPException, Request
from loguru import logger

from rquant.alert_price_rule import PriceAlertRule
from rquant.page_control import (
    DeletePriceAlertRule,
    PageControlReceipt,
    PageControlStatus,
    SavePriceAlertRule,
    SetPriceAlertRuleEnabled,
)
from rquant.price_alert_admission import (
    PriceAlertAdmissionRejectedError,
    PriceAlertAdmissionUnavailableError,
)
from rquant.price_alert_rule_store import PriceAlertRuleEntry
from rquant.runtime_contracts import normalize_aware_utc
from rquant.serving_price_alert_rule_projection import PriceAlertRuleProjectionRow
from rquant.serving_publisher import ServingReader
from rquant.web.envelope import ServingMeta, ServingState
from rquant.web.models.price_alert_rules import (
    PriceAlertRuleCommandReceipt,
    PriceAlertRuleCommandRequest,
)
from rquant.web.price_alert_read import read_price_alert_rules
from rquant.web.serving import BorrowedGeneration, serving_meta

PriceRuleCommand = SavePriceAlertRule | SetPriceAlertRuleEnabled | DeletePriceAlertRule


class PriceRuleTransport(Protocol):
    def lookup(
        self, command: PriceRuleCommand, *, authenticated_owner_id: str
    ) -> PageControlReceipt | None: ...
    def submit(
        self, command: PriceRuleCommand, *, authenticated_owner_id: str
    ) -> PageControlReceipt: ...
    def resume(
        self, command: PriceRuleCommand, *, authenticated_owner_id: str
    ) -> PageControlReceipt: ...


def private_meta(
    request: Request, borrowed: BorrowedGeneration | None, now: datetime
) -> ServingMeta:
    web = request.app.state.web
    return serving_meta(
        borrowed, now=now, stale_after=web.settings.stale_after, failure=web.tracker.failure
    ).model_copy(update={"detail": ""})


def domain_command(body: PriceAlertRuleCommandRequest) -> PriceRuleCommand:
    common = {
        "command_id": body.command_id,
        "requested_at": body.requested_at,
        "expected_version": body.expected_version,
    }
    if body.action == "save":
        assert body.rule is not None
        return SavePriceAlertRule(
            **common,
            ts_code=body.ts_code,
            membership_version=body.membership_version,
            rule=body.rule.domain_rule(body.rule_id),
        )
    if body.action == "set_enabled":
        return SetPriceAlertRuleEnabled(**common, rule_id=body.rule_id, enabled=body.enabled)
    return DeletePriceAlertRule(**common, rule_id=body.rule_id)


def _same_pointer(request: Request, borrowed: BorrowedGeneration) -> bool:
    pointer = ServingReader(request.app.state.web.settings.serving_root).current_pointer()
    return (
        borrowed.pointer is not None
        and pointer.generation_id == borrowed.manifest.generation_id
        and pointer.manifest_sha256 == borrowed.pointer.manifest_sha256
    )


def _candidate(
    body: PriceAlertRuleCommandRequest,
    owner: str,
    now: datetime,
    head: PriceAlertRuleProjectionRow | None,
) -> PriceAlertRuleProjectionRow:
    if body.action == "delete":
        return PriceAlertRuleProjectionRow.from_entry(
            PriceAlertRuleEntry(
                owner_id=owner,
                rule_id=body.rule_id,
                version=body.expected_version + 1,
                deleted=True,
                ts_code=None,
                membership_version=None,
                rule=None,
                updated_at=now,
            )
        )
    if body.action == "save":
        assert body.rule is not None
        rule = body.rule.domain_rule(body.rule_id)
        code, membership = body.ts_code, body.membership_version
    else:
        assert head is not None and not head.deleted
        rule = PriceAlertRule(
            rule_id=head.rule_id,
            name=head.name,
            priority=head.priority,
            enabled=body.enabled,
            comparison=head.comparison,
            threshold=head.threshold,
            valid_from=time.fromisoformat(head.valid_from),
            valid_until=time.fromisoformat(head.valid_until),
        )
        code, membership = head.ts_code, head.membership_version
    # The full frozen row validator includes canonical Decimal reparse and text bounds.
    return PriceAlertRuleProjectionRow.from_entry(
        PriceAlertRuleEntry(
            owner_id=owner,
            rule_id=body.rule_id,
            version=(body.expected_version or 0) + 1,
            deleted=False,
            ts_code=code,
            membership_version=membership,
            rule=rule,
            updated_at=now,
        )
    )


def new_command_preflight(request: Request, body: PriceAlertRuleCommandRequest, owner: str) -> None:
    web = request.app.state.web
    web.tracker.refresh()
    now = normalize_aware_utc(web.clock())
    try:
        with web.tracker.borrow() as borrowed:
            meta = private_meta(request, borrowed, now)
            if borrowed is None or meta.state is not ServingState.READY:
                raise HTTPException(503, "规则暂不可用，请稍后重试。")
            if borrowed.manifest.generation_id != body.generation_id:
                raise HTTPException(409, "规则已更新，请刷新后重试。")
            view = read_price_alert_rules(borrowed, owner_id=owner, now=now)
            if view.availability != "ready" or not view.members_ready:
                raise HTTPException(503, "规则或盯盘名单暂不可用，请稍后重试。")
            head = next((row for row in view.rules if row.rule_id == body.rule_id), None)
            if (None if head is None else head.version) != body.expected_version:
                raise HTTPException(409, "规则已更新，请刷新后重试。")
            if body.action != "save" and (head is None or head.deleted):
                raise HTTPException(409, "规则已删除，请刷新后重试。")
            if (
                body.action == "save"
                and (head is None or head.deleted)
                and sum(not row.deleted for row in view.rules) >= 100
            ):
                raise HTTPException(409, "规则已满，请删除其他规则后重试。")
            try:
                candidate = _candidate(body, owner, now, head)
            except (ValueError, TypeError) as exc:
                raise HTTPException(422, "价格或时间超出可用范围，请检查后重试。") from exc
            needs_member = not candidate.deleted and (
                head is None
                or head.deleted
                or candidate.enabled
                or candidate.ts_code != head.ts_code
                or candidate.membership_version != head.membership_version
            )
            member = next((row for row in view.members if row.ts_code == candidate.ts_code), None)
            if needs_member and (
                member is None
                or member.deleted
                or member.version != candidate.membership_version
                or (member.expires_at is not None and member.expires_at <= now)
            ):
                raise HTTPException(409, "盯盘已更新，请重新选择股票。")
            if not _same_pointer(request, borrowed):
                raise HTTPException(409, "规则已更新，请刷新后重试。")
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("Price rule Web preflight failed")
        raise HTTPException(503, "规则暂不可用，请稍后重试。") from exc


def _publication(
    request: Request,
    body: PriceAlertRuleCommandRequest,
    owner: str,
    receipt: PageControlReceipt,
    version: int,
) -> str:
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
            view = read_price_alert_rules(borrowed, owner_id=owner, now=now)
            if (
                view.availability != "ready"
                or not view.members_ready
                or not _same_pointer(request, borrowed)
            ):
                return "saved_syncing"
            head = next((row for row in view.rules if row.rule_id == body.rule_id), None)
            if head is not None and head.version > version:
                return "superseded"
            if head is None or head.version != version:
                return "saved_syncing"
            if body.action == "delete":
                return "published" if head.deleted else "saved_syncing"
            if head.deleted:
                return "saved_syncing"
            if body.action == "set_enabled":
                return "published" if head.enabled is body.enabled else "saved_syncing"
            candidate = _candidate(body, owner, now, None)
            fields = (
                "ts_code",
                "membership_version",
                "name",
                "priority",
                "enabled",
                "comparison",
                "threshold",
                "valid_from",
                "valid_until",
            )
            return (
                "published"
                if all(getattr(head, name) == getattr(candidate, name) for name in fields)
                else "saved_syncing"
            )
    except Exception:
        logger.exception("Price rule publication cannot be confirmed")
        return "saved_syncing"


def reply(
    body: PriceAlertRuleCommandRequest, status: str, message: str, *, version: int | None = None
) -> PriceAlertRuleCommandReceipt:
    return PriceAlertRuleCommandReceipt.model_validate(
        {
            "command_id": body.command_id,
            "rule_id": body.rule_id,
            "action": body.action,
            "status": status,
            "message": message,
            "version": version,
        }
    )


def verified_receipt(
    request: Request, body: PriceAlertRuleCommandRequest, owner: str, receipt: PageControlReceipt
) -> PriceAlertRuleCommandReceipt:
    if receipt.command_id != body.command_id or receipt.enqueued_at != body.requested_at:
        raise ValueError("price rule receipt identity differs")
    if receipt.status in (PageControlStatus.SUCCEEDED, PageControlStatus.FAILED):
        result = receipt.result
        if (
            not isinstance(result, dict)
            or result.get("rule_id") != body.rule_id
            or result.get("action") != body.action
        ):
            raise ValueError("price rule result identity differs")
        if receipt.completed_at is None:
            raise ValueError("price rule terminal receipt lacks a valid completion time")
        if receipt.status is PageControlStatus.FAILED:
            code = result.get("code")
            status = {
                "version_conflict": "conflict",
                "scope_invalid": "scope_invalid",
                "capacity_exceeded": "capacity",
            }.get(code, "failed")
            return reply(
                body,
                status,
                {
                    "conflict": "规则已更新，请刷新后重试。",
                    "scope_invalid": "盯盘已更新，请重新选择股票。",
                    "capacity": "规则已满，请删除其他规则后重试。",
                    "failed": "操作未完成，请检查后重试。",
                }[status],
            )
        expected_enabled = (
            None
            if body.action == "delete"
            else body.enabled
            if body.action == "set_enabled"
            else body.rule.enabled
        )
        version = result.get("version")
        if (
            type(version) is not int
            or version != (body.expected_version or 0) + 1
            or result.get("deleted") is not (body.action == "delete")
            or result.get("enabled") is not expected_enabled
        ):
            raise ValueError("price rule result version or intent differs")
        status = _publication(request, body, owner, receipt, version)
        completed_message = (
            "已删除。"
            if body.action == "delete"
            else ("已启用。" if body.enabled else "已停用。")
            if body.action == "set_enabled"
            else "已保存。"
        )
        return reply(
            body,
            status,
            {
                "published": completed_message,
                "superseded": "规则已更新，请查看当前设置。",
                "saved_syncing": "设置已写入，等待同步。",
            }[status],
            version=version,
        )
    status = "uncertain" if receipt.status is PageControlStatus.AMBIGUOUS else receipt.status.value
    return reply(
        body,
        status,
        {
            "pending": "等待处理。",
            "processing": "正在处理。",
            "uncertain": "状态待核对，请继续核对原操作。",
        }[status],
    )


def execute_price_rule(
    request: Request, body: PriceAlertRuleCommandRequest, owner: str, *, resume_only: bool
) -> tuple[PriceAlertRuleCommandReceipt, int]:
    admission: PriceRuleTransport | None = getattr(
        request.app.state.web, "price_alert_admission", None
    )
    if admission is None:
        return reply(body, "rejected", "规则操作暂未开放。"), 503
    command = domain_command(body)

    def uncertain() -> tuple[PriceAlertRuleCommandReceipt, int]:
        return reply(body, "uncertain", "状态待核对，请继续核对原操作。"), 503

    def existing(original: PageControlReceipt) -> tuple[PriceAlertRuleCommandReceipt, int]:
        if original.status in (PageControlStatus.PENDING, PageControlStatus.PROCESSING):
            try:
                original = admission.resume(command, authenticated_owner_id=owner)
            except (PriceAlertAdmissionUnavailableError, OSError):
                original = admission.lookup(command, authenticated_owner_id=owner)
                if original is None:
                    return uncertain()
        try:
            response = verified_receipt(request, body, owner, original)
        except (ValueError, TypeError):
            logger.exception("Price rule Web receipt cannot be verified")
            return reply(body, "uncertain", "状态待核对，请继续核对原操作。"), 502
        return response, 409 if response.status in {
            "conflict",
            "capacity",
            "scope_invalid",
        } else 200

    try:
        original = admission.lookup(command, authenticated_owner_id=owner)
        if original is not None:
            return existing(original)
        if resume_only:
            return reply(body, "not_found", "暂未查到原操作，请保留记录并稍后核对。"), 404
        try:
            new_command_preflight(request, body, owner)
        except HTTPException as rejection:
            original = admission.lookup(command, authenticated_owner_id=owner)
            if original is not None:
                return existing(original)
            return reply(
                body,
                "conflict" if rejection.status_code == 409 else "rejected",
                str(rejection.detail),
            ), rejection.status_code
        try:
            receipt = admission.submit(command, authenticated_owner_id=owner)
        except (PriceAlertAdmissionUnavailableError, OSError):
            receipt = admission.lookup(command, authenticated_owner_id=owner)
            if receipt is None:
                return uncertain()
        return existing(receipt)
    except (PriceAlertAdmissionUnavailableError, OSError):
        return uncertain()
    except (PriceAlertAdmissionRejectedError, ValueError) as error:
        if str(error) in {"not_found", "rejected"}:
            return uncertain()
        return reply(body, "conflict", "原操作内容无法核对，请保留记录并刷新。"), 409
