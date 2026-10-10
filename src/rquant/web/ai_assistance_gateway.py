"""Typed boundary to the original owner; Web never owns AI budgets or model keys."""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from rquant.web.models.ai_assistance import AIAction, AIReadData


@runtime_checkable
class AIAssistanceGateway(Protocol):
    def request(self, action: AIAction, *, authenticated_actor_id: str) -> AIReadData: ...


class AIAdmissionRejected(ValueError):
    pass


class AIAdmissionUnavailable(RuntimeError):
    pass
