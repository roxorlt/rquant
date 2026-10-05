"""The Web process can only use the typed private template admission."""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from rquant.strategy_authoring_admission import StrategyAuthoringAdmissionResult
from rquant.strategy_authoring_commands import StrategyAuthoringIdentity
from rquant.strategy_template_run_commands import (
    StrategyTemplateCommandValue as StrategyTemplateCommand,
)


@runtime_checkable
class StrategyAuthoringGateway(Protocol):
    def run_available(self, *, authenticated_actor_id: str) -> bool: ...

    def lookup(
        self, request: StrategyTemplateCommand, *, authenticated_actor_id: str
    ) -> StrategyAuthoringAdmissionResult | None: ...

    def resume(
        self, request: StrategyTemplateCommand, *, authenticated_actor_id: str
    ) -> StrategyAuthoringAdmissionResult: ...

    def submit(
        self,
        request: StrategyTemplateCommand,
        *,
        authenticated_actor_id: str,
        verified_metadata_identity: StrategyAuthoringIdentity,
    ) -> StrategyAuthoringAdmissionResult: ...
