"""Ownerless private actions for importing an actual screening execution."""

from __future__ import annotations

from typing import Literal

from pydantic import Field

from rquant.runtime_contracts import RuntimeContractModel
from rquant.screen.alert_draft import ScreenAlertDraftRequest


class ScreenAlertDraftCreateAction(RuntimeContractModel):
    action: Literal["alert_draft_create"] = "alert_draft_create"
    request: ScreenAlertDraftRequest


class ScreenAlertDraftReadAction(RuntimeContractModel):
    action: Literal["alert_draft_read"] = "alert_draft_read"
    draft_id: str = Field(pattern=r"^[0-9a-f]{24}$")
