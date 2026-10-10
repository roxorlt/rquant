"""Page a complete published transaction by its original attested sequence."""

from __future__ import annotations

import base64
import binascii
from typing import Literal

from pydantic import Field

from rquant.paper_portfolio_ledger import PaperPortfolioHistoryRecord, PaperPortfolioLedgerFrame
from rquant.paper_portfolio_models import PaperPortfolioConfiguration, Sha256
from rquant.runtime_contracts import RuntimeContractModel


class PaperHistoryCursor(RuntimeContractModel):
    contract: Literal["paper-history-cursor/v1"] = "paper-history-cursor/v1"
    account_id: str = Field(min_length=1, max_length=128)
    generation_id: Sha256
    frame_fingerprint: Sha256
    before_sequence: int = Field(strict=True, ge=2)


class PaperHistoryPage(RuntimeContractModel):
    account_id: str
    generation_id: Sha256
    ledger_revision: int
    total_orders: int
    coverage: Literal["complete"] = "complete"
    records: tuple[PaperPortfolioHistoryRecord, ...] = Field(max_length=200)
    next_cursor: str | None = None


def paper_history_page(frame: PaperPortfolioLedgerFrame, *, configuration: PaperPortfolioConfiguration,
                      authenticated_actor_id: str, generation_id: str, cursor: str | None = None,
                      limit: int = 200) -> PaperHistoryPage:
    if authenticated_actor_id != configuration.binding.owner_id:
        raise PermissionError("paper history belongs to a different user")
    if (frame.configuration_fingerprint != configuration.fingerprint
            or frame.account_id != configuration.binding.account_id
            or type(limit) is not int or not 1 <= limit <= 200):
        raise ValueError("paper history account, configuration or page budget differs")
    cutoff = frame.ledger_revision+1
    if cursor is not None:
        if not isinstance(cursor, str) or len(cursor) > 2048:
            raise ValueError("paper history cursor exceeds its budget")
        try:
            parsed = PaperHistoryCursor.model_validate_json(base64.b64decode(cursor.encode("ascii"), altchars=b"-_", validate=True))
        except (ValueError, UnicodeError, binascii.Error) as exc:
            raise ValueError("paper history cursor is invalid") from exc
        if (parsed.account_id != frame.account_id or parsed.generation_id != generation_id
                or parsed.frame_fingerprint != frame.fingerprint
                or parsed.before_sequence not in {item.sequence for item in frame.history}):
            raise ValueError("paper history cursor belongs to a different published account or generation")
        cutoff = parsed.before_sequence
    ordered = tuple(item for item in reversed(frame.history) if item.sequence < cutoff)
    records = ordered[:limit]
    next_cursor = None
    if len(ordered) > limit:
        value = PaperHistoryCursor(account_id=frame.account_id, generation_id=generation_id,
                                  frame_fingerprint=frame.fingerprint, before_sequence=records[-1].sequence)
        next_cursor = base64.urlsafe_b64encode(value.model_dump_json().encode()).decode("ascii")
    return PaperHistoryPage(account_id=frame.account_id, generation_id=generation_id,
                            ledger_revision=frame.ledger_revision, total_orders=len(frame.history), records=records, next_cursor=next_cursor)
