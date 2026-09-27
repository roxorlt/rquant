"""Pure membership periods derived from verified daily pool results."""

from __future__ import annotations

import math
import re
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date
from typing import Literal, Self

from pydantic import Field, model_validator

from rquant.pool_result_receipt import ScreenRunReceipt, Sha256, member_set_digest
from rquant.runtime_contracts import RuntimeContractModel

MissingReceiptReason = Literal["not_run", "legacy_unproven"]
UnknownReason = Literal[
    "window_truncated",
    "not_run",
    "legacy_unproven",
    "calendar_incomplete",
    "definition_mismatch",
    "lineage_incomplete",
    "receipt_mismatch",
    "ambiguous_rerun",
    "entry_price_missing",
]
ProjectionStatus = Literal[
    "verified",
    "not_run",
    "legacy_unproven",
    "calendar_incomplete",
    "definition_mismatch",
    "lineage_incomplete",
    "receipt_mismatch",
    "ambiguous_rerun",
]


class PoolMemberClose(RuntimeContractModel):
    ts_code: str = Field(min_length=1)
    close: float | None = None


class PoolDayEvidence(RuntimeContractModel):
    """One run and its same-snapshot rows, or an explicit missing-proof state."""

    trade_date: date
    receipt: ScreenRunReceipt | None
    members: tuple[PoolMemberClose, ...] = ()
    missing_receipt_reason: MissingReceiptReason | None = None


class PoolMembershipMember(RuntimeContractModel):
    """Entry close is that day's screen_result.close, never an execution price."""

    ts_code: str = Field(min_length=1)
    entry_trade_date: date | None = None
    entry_close: float | None = Field(default=None, gt=0, allow_inf_nan=False)
    entry_result_version: Sha256 | None = None
    unknown_reason: UnknownReason | None = None

    @model_validator(mode="after")
    def require_complete_entry_evidence(self) -> Self:
        present = (
            self.entry_trade_date is not None,
            self.entry_close is not None,
            self.entry_result_version is not None,
        )
        if any(present) != all(present):
            raise ValueError("entry evidence must be complete or have an unknown reason")
        if all(present) and self.unknown_reason is not None:
            raise ValueError("entry evidence must be complete or have an unknown reason")
        if not any(present) and self.unknown_reason is None:
            raise ValueError("entry evidence must be complete or have an unknown reason")
        return self


class PoolMembershipProjection(RuntimeContractModel):
    """Current result-set status; each member separately records entry confidence."""

    pool_name: str
    trade_date: date | None
    result_version: Sha256 | None
    status: ProjectionStatus
    members: tuple[PoolMembershipMember, ...] = ()


@dataclass(frozen=True)
class _Entry:
    trade_date: date | None
    close: float | None
    result_version: str | None
    unknown_reason: UnknownReason | None


def _valid_price(close: float | None) -> bool:
    return close is not None and math.isfinite(close) and close > 0


def _select_day(evidence: Sequence[PoolDayEvidence]) -> tuple[PoolDayEvidence | None, bool]:
    receipts = [day for day in evidence if day.receipt is not None]
    if not receipts:
        legacy = any(
            day.missing_receipt_reason == "legacy_unproven" or day.members for day in evidence
        )
        reason: MissingReceiptReason = "legacy_unproven" if legacy else "not_run"
        return (
            PoolDayEvidence(
                trade_date=evidence[0].trade_date,
                receipt=None,
                missing_receipt_reason=reason,
            ),
            False,
        )
    latest_at = max(day.receipt.completed_at for day in receipts if day.receipt is not None)
    latest = [
        day for day in receipts if day.receipt is not None and day.receipt.completed_at == latest_at
    ]
    if len(latest) > 1 and any(day != latest[0] for day in latest[1:]):
        return None, True
    return latest[0], False


def _day_status(
    day: PoolDayEvidence,
    *,
    pool_name: str,
    published_definition_version: str,
) -> tuple[ProjectionStatus, dict[str, PoolMemberClose]]:
    receipt = day.receipt
    if receipt is None:
        return day.missing_receipt_reason or "not_run", {}
    if receipt.trade_date != day.trade_date or receipt.preset_name != pool_name:
        raise ValueError("pool receipt date or pool name differs from day evidence")
    codes = [member.ts_code for member in day.members]
    try:
        member_digest = member_set_digest(codes)
    except ValueError:
        return "receipt_mismatch", {}
    if receipt.hit_count != len(codes) or receipt.member_digest != member_digest:
        return "receipt_mismatch", {}
    members = {member.ts_code: member for member in day.members}
    if receipt.definition_version != published_definition_version:
        return "definition_mismatch", members
    if not receipt.lineage_complete:
        return "lineage_incomplete", members
    return "verified", members


def compute_pool_membership(
    *,
    pool_name: str,
    published_definition_version: str,
    trading_days: Sequence[date],
    days: Sequence[PoolDayEvidence],
    calendar_complete: bool,
) -> PoolMembershipProjection:
    """Derive entries only where a complete trusted calendar proves adjacent runs.

    ``calendar_complete`` is supplied by the calendar verifier; this function cannot
    discover a missing exchange day from a list that already omits it. Rows and receipts
    must come from one stable source generation. No returns or execution prices are inferred.
    """
    if not pool_name or not re.fullmatch(r"[0-9a-f]{64}", published_definition_version):
        raise ValueError("pool name and published definition version are required")
    calendar = tuple(trading_days)
    if any(left >= right for left, right in zip(calendar, calendar[1:], strict=False)):
        raise ValueError("trading days must be unique and sorted")
    if not calendar:
        return PoolMembershipProjection(
            pool_name=pool_name,
            trade_date=None,
            result_version=None,
            status="calendar_incomplete",
        )
    by_date: dict[date, list[PoolDayEvidence]] = {}
    for day in days:
        if day.receipt is not None and (
            day.receipt.trade_date != day.trade_date or day.receipt.preset_name != pool_name
        ):
            raise ValueError("pool receipt date or pool name differs from day evidence")
        if day.receipt is not None and day.missing_receipt_reason is not None:
            raise ValueError("a successful pool run cannot have a missing receipt reason")
        by_date.setdefault(day.trade_date, []).append(day)
    calendar_is_complete = calendar_complete and set(by_date).issubset(calendar)

    previous_status: ProjectionStatus | None = None
    previous_entries: dict[str, _Entry] = {}
    current_status: ProjectionStatus = "not_run"
    current_members: dict[str, PoolMemberClose] = {}
    current_entries: dict[str, _Entry] = {}
    current_receipt: ScreenRunReceipt | None = None
    for trade_date in calendar:
        evidence = by_date.get(trade_date)
        if evidence:
            selected, ambiguous = _select_day(evidence)
        else:
            selected, ambiguous = None, False
        if ambiguous:
            status: ProjectionStatus = "ambiguous_rerun"
            members: dict[str, PoolMemberClose] = {}
        elif selected is None:
            status = "not_run"
            members = {}
        else:
            status, members = _day_status(
                selected,
                pool_name=pool_name,
                published_definition_version=published_definition_version,
            )

        entries: dict[str, _Entry] = {}
        if status == "verified":
            assert selected is not None and selected.receipt is not None
            for code, member in members.items():
                if previous_status == "verified" and code in previous_entries:
                    entries[code] = previous_entries[code]
                elif previous_status == "verified":
                    entries[code] = (
                        _Entry(trade_date, member.close, selected.receipt.result_version, None)
                        if _valid_price(member.close)
                        else _Entry(None, None, None, "entry_price_missing")
                    )
                else:
                    entries[code] = _Entry(None, None, None, previous_status or "window_truncated")
        previous_status = status
        previous_entries = entries
        current_status = status
        current_members = members
        current_entries = entries
        current_receipt = selected.receipt if selected is not None else None

    result_version = current_receipt.result_version if current_receipt is not None else None
    if current_status in {"not_run", "legacy_unproven", "receipt_mismatch", "ambiguous_rerun"}:
        return PoolMembershipProjection(
            pool_name=pool_name,
            trade_date=calendar[-1],
            result_version=result_version,
            status=current_status,
        )
    if current_status in {"definition_mismatch", "lineage_incomplete"}:
        return PoolMembershipProjection(
            pool_name=pool_name,
            trade_date=calendar[-1],
            result_version=result_version,
            status=current_status,
        )

    output_status: ProjectionStatus = "verified" if calendar_is_complete else "calendar_incomplete"
    output_members: list[PoolMembershipMember] = []
    for code in sorted(current_members):
        entry = current_entries[code]
        if not calendar_is_complete:
            entry = _Entry(None, None, None, "calendar_incomplete")
        output_members.append(
            PoolMembershipMember(
                ts_code=code,
                entry_trade_date=entry.trade_date,
                entry_close=entry.close,
                entry_result_version=entry.result_version,
                unknown_reason=entry.unknown_reason,
            )
        )
    return PoolMembershipProjection(
        pool_name=pool_name,
        trade_date=calendar[-1],
        result_version=result_version,
        status=output_status,
        members=tuple(output_members),
    )
