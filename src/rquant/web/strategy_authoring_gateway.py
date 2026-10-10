"""The Web process can only use the typed private template admission."""

from __future__ import annotations

from typing import Protocol, runtime_checkable
from typing import Literal

from rquant.strategy_authoring_admission import StrategyAuthoringAdmissionResult, StrategyPromotionAdmissionResult
from rquant.strategy_authoring_commands import StrategyAuthoringIdentity, StrategyTemplateHead
from rquant.strategy_template_run_commands import (
    StrategyTemplateCommandValue as StrategyTemplateCommand,
)
from rquant.strategy_promotion_commands import StrategyPromotionCommand
from rquant.strategy_promotion_contracts import StrategyPromotionContext


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


@runtime_checkable
class StrategyPromotionGateway(Protocol):
    def promotion_context(self, *, authenticated_actor_id: str, source_kind: Literal["template", "builtin"], strategy_id: str, head: StrategyTemplateHead | None = None) -> StrategyPromotionContext: ...

    def promotion_lookup(self, request: StrategyPromotionCommand, *, authenticated_actor_id: str) -> StrategyPromotionAdmissionResult | None: ...

    def promotion_resume(self, request: StrategyPromotionCommand, *, authenticated_actor_id: str) -> StrategyPromotionAdmissionResult: ...

    def promotion_submit(self, request: StrategyPromotionCommand, *, authenticated_actor_id: str, verified_metadata_identity: StrategyAuthoringIdentity) -> StrategyPromotionAdmissionResult: ...
