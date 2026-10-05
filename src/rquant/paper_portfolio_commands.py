"""Concrete effects for the original PageControl journal and exact paper metadata."""

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime, timedelta
from uuid import uuid4

from rquant.paper_operator_commands import (
    OwnedPaperPortfolioCommand, OwnedSavePaperPortfolioConfiguration, OwnedSetPaperAccountPaused,
    PaperOperatorConfirmation, PaperPortfolioCommand, SavePaperPortfolioConfiguration, SetPaperAccountPaused,
)
from rquant.paper_portfolio_models import PaperPortfolioConfiguration, PaperPortfolioStateIdentity
from rquant.paper_portfolio_runtime import PaperPortfolioRuntime, PaperPortfolioRuntimeCatalog
from rquant.runtime_contracts import normalize_aware_utc
from rquant.paper_research_commands import OwnedRunPaperPortfolioResearch, RunPaperPortfolioResearch
from rquant.paper_research_submission import PaperResearchRunBackend
from rquant.web.paper_portfolio_reader import PaperPortfolioServingSource


class PaperPortfolioPageControlBackend:
    def __init__(self, catalog: PaperPortfolioRuntimeCatalog, *, clock: Callable[[], datetime],
                 editor_users: tuple[str, ...], enabled: bool = False,
                 research_backend: PaperResearchRunBackend | None = None,
                 admission_source: PaperPortfolioServingSource | None = None) -> None:
        if type(catalog) is not PaperPortfolioRuntimeCatalog:
            raise TypeError("paper commands require the finite concrete owner/account catalog")
        self.catalog = catalog
        self.clock = clock
        self.editor_users = editor_users
        self.enabled = enabled
        if research_backend is not None and type(research_backend) is not PaperResearchRunBackend:
            raise TypeError("paper research backend must be the original concrete installed service")
        self.research_backend = research_backend
        if admission_source is not None and type(admission_source) is not PaperPortfolioServingSource:
            raise TypeError("paper admission source must be the concrete original Serving reader")
        self.admission_source = admission_source

    def _require_fresh(self, request: PaperPortfolioCommand, actor_id: str, identity: PaperPortfolioStateIdentity) -> None:
        if self.admission_source is None:
            return
        fingerprint = request.expected_configuration_fingerprint if type(request) is SavePaperPortfolioConfiguration else request.configuration_fingerprint
        self.admission_source.require_fresh(generation_id=request.generation_id, account_id=request.account_id, actor_id=actor_id,
                                            configuration_fingerprint=fingerprint, metadata_identity=identity)

    def authorize(self, actor_id: str) -> None:
        if not self.enabled or actor_id not in self.editor_users:
            raise PermissionError("paper portfolio commands are not enabled for this owner")

    def _runtime(self, account_id: str, actor_id: str) -> PaperPortfolioRuntime:
        self.authorize(actor_id)
        return self.catalog.for_account(account_id, authenticated_actor_id=actor_id)

    def identity(self, account_id: str, *, authenticated_actor_id: str) -> PaperPortfolioStateIdentity:
        return self._runtime(account_id, authenticated_actor_id).state.identity()

    def _new_tables(self, runtime: PaperPortfolioRuntime) -> None:
        with runtime.state._connection(write=True) as connection:
            connection.execute("CREATE TABLE IF NOT EXISTS operator_confirmations(confirmation_id TEXT PRIMARY KEY,command_id TEXT UNIQUE NOT NULL,body TEXT NOT NULL)")
            connection.execute("CREATE TABLE IF NOT EXISTS configuration_command_refs(command_id TEXT PRIMARY KEY,owner_id TEXT NOT NULL,request_body TEXT NOT NULL,receipt_body TEXT NOT NULL)")

    def prepare_confirmation(self, request: SetPaperAccountPaused, *, authenticated_actor_id: str,
                             expected_identity: PaperPortfolioStateIdentity) -> PaperOperatorConfirmation:
        request = SetPaperAccountPaused.model_validate(request.model_dump(mode="python"))
        runtime = self._runtime(request.account_id, authenticated_actor_id)
        self._new_tables(runtime)
        if runtime.state.identity() != expected_identity:
            raise ValueError("paper confirmation metadata identity changed")
        with runtime.state._connection(write=True) as connection:
            old = connection.execute("SELECT body FROM operator_confirmations WHERE command_id=?", (request.command_id,)).fetchone()
            if old:
                result = PaperOperatorConfirmation.model_validate_json(old[0])
                if result.request != request or result.owner_id != authenticated_actor_id:
                    raise ValueError("original confirmation body differs")
                return result
            runtime.state.refresh_configuration()
            self._require_fresh(request, authenticated_actor_id, expected_identity)
            self._validate_predecessor(runtime, request)
            now = normalize_aware_utc(self.clock())
            result = PaperOperatorConfirmation(confirmation_id=str(uuid4()), owner_id=authenticated_actor_id,
                                               metadata_identity=expected_identity, request=request,
                                               issued_at=now, expires_at=now + timedelta(minutes=5))
            connection.execute("INSERT INTO operator_confirmations VALUES(?,?,?)", (result.confirmation_id, request.command_id, result.model_dump_json()))
        return result

    @staticmethod
    def _validate_predecessor(runtime: PaperPortfolioRuntime, request: SetPaperAccountPaused) -> None:
        if request.configuration_fingerprint != runtime.state.configuration.fingerprint:
            raise ValueError("paper configuration changed before confirmation")
        with runtime.state._connection() as connection:
            head = runtime.operator._head(connection)
        if (request.expected_sequence, request.expected_paused) != (head.sequence if head else 0, head.paused if head else True):
            raise ValueError("paper control predecessor changed before confirmation")

    def compile(self, request: PaperPortfolioCommand, *, authenticated_actor_id: str,
                expected_identity: PaperPortfolioStateIdentity, confirmation_id: str | None = None) -> OwnedPaperPortfolioCommand:
        if type(request) not in (SetPaperAccountPaused, SavePaperPortfolioConfiguration, RunPaperPortfolioResearch):
            raise TypeError("paper admission accepts an ownerless concrete request")
        runtime = self._runtime(request.account_id, authenticated_actor_id)
        self._new_tables(runtime)
        if runtime.state.identity() != expected_identity:
            raise ValueError("paper metadata identity changed before admission")
        if type(request) is RunPaperPortfolioResearch:
            if self.research_backend is None:
                raise PermissionError("paper research admission is not configured")
            original = self.research_backend.lookup(request, owner_id=authenticated_actor_id, expected_identity=expected_identity)
            if original is not None:
                return original
            self._require_fresh(request, authenticated_actor_id, expected_identity)
            return self.research_backend.compile(request, owner_id=authenticated_actor_id, expected_identity=expected_identity)
        self._require_fresh(request, authenticated_actor_id, expected_identity)
        accepted = normalize_aware_utc(self.clock())
        runtime.state.refresh_configuration()
        if type(request) is SetPaperAccountPaused:
            with runtime.state._connection() as connection:
                row = connection.execute("SELECT body FROM operator_confirmations WHERE confirmation_id=?", (confirmation_id,)).fetchone()
            if not row:
                raise ValueError("pause requires its original two-step confirmation")
            confirmation = PaperOperatorConfirmation.model_validate_json(row[0])
            self._validate_predecessor(runtime, request)
            return OwnedSetPaperAccountPaused(**request.model_dump(mode="python"), owner_id=authenticated_actor_id,
                                             metadata_identity=expected_identity, accepted_at=accepted, confirmation=confirmation)
        old = runtime.state.configuration
        if request.expected_configuration_fingerprint != old.fingerprint:
            raise ValueError("paper configuration head changed before admission")
        configuration = PaperPortfolioConfiguration(binding=old.binding, version=old.version + 1, configured_at=accepted,
                                                   weight_rule=request.weight_rule, drawdown_rule=request.drawdown_rule,
                                                   execution_cost_spec=old.execution_cost_spec)
        return OwnedSavePaperPortfolioConfiguration(**request.model_dump(mode="python"), owner_id=authenticated_actor_id,
                                                   metadata_identity=expected_identity, accepted_at=accepted, configuration=configuration)

    def validate(self, command: OwnedPaperPortfolioCommand) -> None:
        if type(command) not in (OwnedSetPaperAccountPaused, OwnedSavePaperPortfolioConfiguration, OwnedRunPaperPortfolioResearch):
            raise TypeError("paper effect requires the concrete owned command")
        if self.identity(command.account_id, authenticated_actor_id=command.owner_id) != command.metadata_identity:
            raise ValueError("original paper metadata identity was replaced")
        if type(command) is OwnedRunPaperPortfolioResearch:
            if self.research_backend is None:
                raise PermissionError("paper research admission is not configured")
            self.research_backend.validate(command)

    def submit(self, command: OwnedPaperPortfolioCommand) -> dict[str, object]:
        self.validate(command)
        runtime = self._runtime(command.account_id, command.owner_id)
        if type(command) is OwnedRunPaperPortfolioResearch:
            return self.research_backend.submit(command)
        if type(command) is OwnedSetPaperAccountPaused:
            control = runtime.operator.commit_confirmed_control(command.original(), authenticated_actor_id=command.owner_id,
                                                               original_command_id=command.command_id)
            runtime.operator.publish(control)
            return control.model_dump(mode="json")
        self._new_tables(runtime)
        with runtime.state._connection(write=True) as connection:
            row = connection.execute("SELECT * FROM configuration_command_refs WHERE command_id=?", (command.command_id,)).fetchone()
            if row:
                if row["owner_id"] != command.owner_id or SavePaperPortfolioConfiguration.model_validate_json(row["request_body"]) != command.original():
                    raise ValueError("original paper configuration request differs")
                configuration = PaperPortfolioConfiguration.model_validate_json(row["receipt_body"])
                if configuration != command.configuration:
                    raise ValueError("original saved configuration receipt differs")
                control = runtime.operator.configuration_control(command)
                runtime.operator.publish(control)
                return configuration.model_dump(mode="json")
            runtime.state.refresh_configuration()
            if runtime.state.configuration.fingerprint != command.expected_configuration_fingerprint:
                raise ValueError("paper configuration CAS predecessor changed")
            configuration = command.configuration
            if configuration.version != runtime.state.configuration.version + 1 or configuration.binding != runtime.state.configuration.binding:
                raise ValueError("paper immutable configuration continuation differs")
            control = runtime.operator._commit_configuration_control(connection, command)
            runtime.state._insert_configuration(connection, configuration)
            connection.execute("UPDATE metadata SET current_config=? WHERE singleton=1 AND current_config=?", (configuration.fingerprint, command.expected_configuration_fingerprint))
            connection.execute("INSERT INTO configuration_command_refs VALUES(?,?,?,?)", (command.command_id, command.owner_id,
                               command.original().model_dump_json(), command.configuration.model_dump_json()))
        runtime.state.configuration = command.configuration
        runtime.operator.publish(control)
        return command.configuration.model_dump(mode="json")

    def recover(self, command: OwnedPaperPortfolioCommand) -> dict[str, object]:
        return self.submit(command)

    def has_effect(self, command: OwnedPaperPortfolioCommand) -> bool:
        self.validate(command)
        runtime = self._runtime(command.account_id, command.owner_id)
        if type(command) is OwnedRunPaperPortfolioResearch:
            return self.research_backend.has_effect(command)
        if type(command) is OwnedSetPaperAccountPaused:
            return runtime.operator.lookup(command.original(), authenticated_actor_id=command.owner_id) is not None
        self._new_tables(runtime)
        with runtime.state._connection() as connection:
            row = connection.execute("SELECT owner_id,request_body FROM configuration_command_refs WHERE command_id=?", (command.command_id,)).fetchone()
        if row is not None and (row["owner_id"] != command.owner_id or SavePaperPortfolioConfiguration.model_validate_json(row["request_body"]) != command.original()):
            raise ValueError("original configuration effect reference differs")
        return row is not None
