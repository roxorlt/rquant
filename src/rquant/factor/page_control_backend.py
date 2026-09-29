"""Map trusted PageControl factor commands to the fenced definition registry."""

from __future__ import annotations

from pydantic import JsonValue

from rquant.factor.registry import (
    ArchiveFactorRequest,
    FactorDefinitionRegistry,
    FactorRegistryIdentity,
    SaveFactorDefinitionRequest,
)
from rquant.page_control import ArchiveFactor, FactorDefinitionRequestValue, SaveFactorDefinition


class FactorDefinitionPageControlBackend:
    def __init__(self, registry: FactorDefinitionRegistry) -> None:
        self.registry = registry

    def identity(self) -> FactorRegistryIdentity:
        return self.registry.identity()

    @staticmethod
    def _request(
        command: FactorDefinitionRequestValue,
    ) -> SaveFactorDefinitionRequest | ArchiveFactorRequest:
        if isinstance(command, SaveFactorDefinition):
            return SaveFactorDefinitionRequest(
                command_id=command.command_id,
                definition=command.definition,
                expected_head=command.expected_head,
            )
        if isinstance(command, ArchiveFactor):
            return ArchiveFactorRequest(
                command_id=command.command_id,
                factor_id=command.factor_id,
                expected_head=command.expected_head,
            )
        raise TypeError("factor backend requires a typed factor command")

    def submit(
        self,
        command: FactorDefinitionRequestValue,
        *,
        expected_identity: FactorRegistryIdentity,
    ) -> JsonValue:
        request = self._request(command)
        if isinstance(request, SaveFactorDefinitionRequest):
            receipt = self.registry.save(request, expected_identity=expected_identity)
        else:
            receipt = self.registry.archive(request, expected_identity=expected_identity)
        return receipt.model_dump(mode="json")

    def recover(
        self,
        command: FactorDefinitionRequestValue,
        *,
        expected_identity: FactorRegistryIdentity,
    ) -> JsonValue | None:
        receipt = self.registry.lookup_command(
            self._request(command), expected_identity=expected_identity
        )
        return None if receipt is None else receipt.model_dump(mode="json")
