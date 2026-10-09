"""Strict minute selectors and the original PageControl effect admission."""

from __future__ import annotations

from typing import TYPE_CHECKING, Literal, Protocol
from uuid import NAMESPACE_URL, UUID, uuid5

from pydantic import Field, JsonValue, SerializerFunctionWrapHandler, StrictInt, model_serializer

from rquant.lab_job_protocol import SubmitJobCommand
from rquant.minute_backtest_contracts import MinuteReplayModel, Sha256
from rquant.minute_backtest_formal import MinuteExperimentProtocol
from rquant.minute_backtest_formal_adapter import MinuteFormalReplayAdapter
from rquant.minute_backtest_parameters import MinuteParameterSet
from rquant.minute_backtest_parameter_study import MinuteParameterStudySettings
from rquant.runtime_contracts import AwareUtcDatetime, canonical_sha256
from rquant.strict_json import canonical_json_bytes

if TYPE_CHECKING:
    from rquant.minute_backtest_export import MinuteVerifiedReport, MinuteZipExportFacade
    from rquant.minute_backtest_installation import InstalledMinuteReplay
    from rquant.page_control import PageControlService


class MinuteRunConfig(MinuteReplayModel):
    source_key: str = Field(pattern=r"^[a-zA-Z0-9_.:-]{1,128}$")
    source_version: StrictInt = Field(ge=1)
    full_input_hash: Sha256
    native_id: Literal["n_shape", "auction_gap", "growth_board_surge"]
    native_version: StrictInt = Field(ge=1)
    protocol: MinuteExperimentProtocol
    random_seed: StrictInt = Field(default=0, ge=0, lt=2**63)
    deadline: AwareUtcDatetime


class MinuteParameterRunConfig(MinuteReplayModel):
    kind: Literal["minute_parameter_replay"] = "minute_parameter_replay"
    source_key: str = Field(pattern=r"^[a-zA-Z0-9_.:-]{1,128}$")
    source_version: StrictInt = Field(ge=1)
    full_input_hash: Sha256
    parameters: MinuteParameterSet
    protocol: MinuteExperimentProtocol
    random_seed: StrictInt = Field(default=0, ge=0, lt=2**63)
    deadline: AwareUtcDatetime
    study: MinuteParameterStudySettings | None = None

    @model_serializer(mode="wrap")
    def original_default_fields(self, handler: SerializerFunctionWrapHandler) -> dict[str, object]:
        value = handler(self)
        if self.study is None:
            value.pop("study", None)
        return value


class SubmitMinuteReplay(MinuteReplayModel):
    kind: Literal["submit_minute_replay"] = "submit_minute_replay"
    command_id: str = Field(pattern=r"^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$")
    requested_at: AwareUtcDatetime
    actor_id: str = Field(pattern=r"^[A-Za-z0-9._@-]{1,64}$")
    config: MinuteRunConfig | MinuteParameterRunConfig


class ExportMinuteReplayZip(MinuteReplayModel):
    kind: Literal["export_minute_replay_zip"] = "export_minute_replay_zip"
    command_id: str = Field(pattern=r"^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$")
    requested_at: AwareUtcDatetime
    actor_id: str = Field(pattern=r"^[A-Za-z0-9._@-]{1,64}$")
    job_id: UUID
    result_hash: Sha256


MinuteCommand = SubmitMinuteReplay | ExportMinuteReplayZip


class MinuteRunEffect(MinuteReplayModel):
    contract: Literal["minute-admission/v1"] = "minute-admission/v1"
    command_hash: Sha256
    config_hash: Sha256
    interaction_key: str
    command: SubmitJobCommand


class MinuteZipEffect(MinuteReplayModel):
    contract: Literal["minute-zip-admission/v1"] = "minute-zip-admission/v1"
    command_hash: Sha256
    request_id: UUID
    job_id: UUID
    result_hash: Sha256
    full_input_hash: Sha256
    core_input_hash: Sha256
    seed_hash: Sha256
    html_sha256: Sha256
    owner_binding_hash: Sha256


class MinutePageControlBackend(Protocol):
    def freeze(self, command: MinuteCommand) -> JsonValue: ...
    def submit(self, command: MinuteCommand, marker: JsonValue) -> JsonValue: ...
    def recover(self, command: MinuteCommand, marker: JsonValue) -> JsonValue | None: ...


def minute_interaction(command: SubmitMinuteReplay) -> str:
    return f"web.minute-runtime:{command.actor_id}:{command.command_id}"


def minute_job_id(actor_id: str, command_id: UUID | str) -> UUID:
    return uuid5(NAMESPACE_URL, f"rquant.minute-runtime-job:{actor_id}:{command_id}")


def minute_zip_request_id(command: ExportMinuteReplayZip) -> UUID:
    return uuid5(NAMESPACE_URL, f"rquant.minute-runtime-zip:{command.actor_id}:{command.command_id}")


def _minute_command_hash(command: MinuteCommand) -> str:
    if isinstance(command, SubmitMinuteReplay) and isinstance(command.config, MinuteParameterRunConfig):
        from rquant.page_control import _command_hash

        return _command_hash(command)
    return canonical_sha256(command)


def _minute_config_hash(config: MinuteRunConfig | MinuteParameterRunConfig) -> str:
    return canonical_sha256(config.model_dump(mode="json") if isinstance(config, MinuteParameterRunConfig) else config)


def build_minute_zip_effect(command: ExportMinuteReplayZip, report: MinuteVerifiedReport) -> MinuteZipEffect:
    return MinuteZipEffect(
        command_hash=canonical_sha256(command), request_id=minute_zip_request_id(command),
        job_id=command.job_id, result_hash=command.result_hash, full_input_hash=report.sealed.full_input_hash,
        core_input_hash=report.sealed.core_input_hash, seed_hash=report.sealed.seed_hash,
        html_sha256=report.report.html_sha256, owner_binding_hash=report.owner.content_sha256,
    )


def parse_minute_zip_effect(command: ExportMinuteReplayZip, marker: JsonValue) -> MinuteZipEffect:
    checked = MinuteZipEffect.model_validate_json(canonical_json_bytes(marker))
    if (checked.command_hash, checked.request_id, checked.job_id, checked.result_hash) != (
        canonical_sha256(command), minute_zip_request_id(command), command.job_id, command.result_hash,
    ):
        raise PermissionError("minute export original command and frozen effect differ")
    return checked


def require_minute_zip_report(effect: MinuteZipEffect, report: MinuteVerifiedReport) -> None:
    if (
        effect.full_input_hash, effect.core_input_hash, effect.seed_hash, effect.html_sha256, effect.owner_binding_hash,
    ) != (
        report.sealed.full_input_hash, report.sealed.core_input_hash, report.sealed.seed_hash,
        report.report.html_sha256, report.owner.content_sha256,
    ):
        raise PermissionError("minute export complete source, report or original owner changed")


class MinuteCommandWriter:
    def __init__(self, installation: InstalledMinuteReplay) -> None:
        self.installation = installation
        self.owner_authority: PageControlService | None = None
        self.exports: MinuteZipExportFacade | None = None

    def bind_owner_authority(self, authority: PageControlService) -> None:
        from rquant.page_control import PageControlService
        if type(authority) is not PageControlService or authority.consumer.minute_backend is not self:
            raise TypeError("minute exports require their original PageControl owner authority")
        if self.owner_authority is not None and self.owner_authority is not authority:
            raise PermissionError("minute original owner authority cannot be rebound")
        self.owner_authority = authority

    def _exports(self, command: ExportMinuteReplayZip) -> MinuteZipExportFacade:
        authority = self.owner_authority
        if authority is None or authority.collaboration.mode != "enforced":
            raise PermissionError("minute export requires original enforced roles")
        from rquant.page_control import _command_hash
        actor = authority.outbox.trusted_command_actor(command.command_id, _command_hash(command))
        if actor != command.actor_id:
            raise PermissionError("minute export requires the original authenticated command")
        authority.collaboration.require_command(actor, command.kind)
        self.installation.verify_current()
        if self.exports is None:
            from rquant.lab_artifact_export import LabJobZipExportFacade
            from rquant.lab_artifacts import LabJobArtifactStore
            from rquant.minute_backtest_export import MinuteReportReader, MinuteZipExportFacade
            artifact_store = LabJobArtifactStore(self.installation.profile.final_artifact_root)
            try:
                original = LabJobZipExportFacade(reader=self.installation.reader, artifact_store=artifact_store,
                    export_root=self.installation.profile.runtime_root / "exports" / "minute-base")
                self.exports = MinuteZipExportFacade(reader=self.installation.reader, artifact_store=artifact_store,
                    report_reader=MinuteReportReader(self.installation, owner_authority=authority), original_exports=original,
                    export_root=self.installation.profile.runtime_root / "exports" / "minute-reports")
            except BaseException:
                artifact_store.close()
                raise
        return self.exports

    def freeze(self, command: MinuteCommand) -> JsonValue:
        if type(command) is ExportMinuteReplayZip:
            result = self._exports(command).report_reader.read(command.job_id, owner_id=command.actor_id,
                expected_result_hash=command.result_hash)
            return build_minute_zip_effect(command, result).model_dump(mode="json")
        prepared = self.installation.prepare(command.config, owner_id=command.actor_id,
            **({"operation_key": _minute_command_hash(command)} if isinstance(command.config, MinuteParameterRunConfig) else {}))
        marker = MinuteRunEffect(command_hash=_minute_command_hash(command), config_hash=_minute_config_hash(command.config),
            interaction_key=minute_interaction(command), command=prepared.submission(
                job_id=minute_job_id(command.actor_id, command.command_id)).command)
        return marker.model_dump(mode="json")

    def _marker(self, command: MinuteCommand, marker: JsonValue) -> MinuteRunEffect | MinuteZipEffect:
        if type(command) is ExportMinuteReplayZip:
            checked = parse_minute_zip_effect(command, marker)
            result = self._exports(command).report_reader.read(command.job_id, owner_id=command.actor_id,
                expected_result_hash=command.result_hash)
            require_minute_zip_report(checked, result)
            return checked
        checked = MinuteRunEffect.model_validate_json(canonical_json_bytes(marker))
        if (checked.command_hash, checked.config_hash, checked.interaction_key, checked.command.job_id) != (
            _minute_command_hash(command), _minute_config_hash(command.config), minute_interaction(command),
            minute_job_id(command.actor_id, command.command_id)):
            raise PermissionError("minute original command and frozen effect differ")
        config, spec = command.config, checked.command.spec
        if isinstance(config, MinuteParameterRunConfig):
            from rquant.minute_backtest_parameter_adapter import MinuteParameterFormalReplayAdapter
            from rquant.minute_backtest_parameter_producer import MinuteParameterPreparedPublication

            self.installation.verify_current()
            catalog = self.installation.profile.parameter_catalog
            if catalog is None:
                raise PermissionError("parameter frozen task lacks its complete installed facts")
            adapter = MinuteParameterFormalReplayAdapter(catalog)
            parameters = adapter.parameters(spec)
            if parameters.prepared_publication_json is None:
                raise PermissionError("parameter frozen task lacks its original complete prepared publication")
            carrier = MinuteParameterPreparedPublication.model_validate_json(parameters.prepared_publication_json)
            expected = adapter.expected(parameters)
            experiment = spec.experiment
            if experiment is None or (parameters.owner_id, parameters.native_strategy_id, parameters.native_strategy_version,
                parameters.parameter_set_json, parameters.source_frequency, spec.deadline, spec.random_seed,
                experiment.spec.train_range, experiment.spec.validation_range, experiment.spec.frozen_outer_test_range) != (
                command.actor_id, config.parameters.definition_id, config.parameters.definition_version,
                config.parameters.model_dump_json(), config.parameters.parameters.freq, config.deadline, config.random_seed,
                config.protocol.train_range, config.protocol.validation_range, config.protocol.frozen_outer_test_range):
                raise PermissionError("parameter frozen task differs from its full recipe/protocol/deadline")
            if (carrier.baseline.source_key, carrier.baseline.source_version, carrier.baseline.owner_id,
                carrier.baseline.full_input_hash) != (config.source_key, config.source_version, command.actor_id, config.full_input_hash):
                raise PermissionError("parameter frozen task differs from its complete selected baseline")
            if expected.frozen.runtime.parameters != config.parameters:
                raise PermissionError("parameter original complete recipe changed")
            from rquant.minute_backtest_parameter_study import verify_minute_parameter_study_request

            verify_minute_parameter_study_request(expected.frozen.runtime.study_binding, config)
            self.installation.verify_current()
            return checked
        published = self.installation.publication(source_key=config.source_key, source_version=config.source_version,
            owner_id=command.actor_id, full_input_hash=config.full_input_hash)
        parameters = MinuteFormalReplayAdapter(self.installation.profile.catalog).parameters(spec)
        experiment = spec.experiment
        if experiment is None or (parameters.owner_id, parameters.native_strategy_id, parameters.native_strategy_version,
            spec.deadline, spec.random_seed, experiment.spec.train_range, experiment.spec.validation_range,
            experiment.spec.frozen_outer_test_range) != (
            command.actor_id, config.native_id, config.native_version, config.deadline, config.random_seed,
            config.protocol.train_range, config.protocol.validation_range, config.protocol.frozen_outer_test_range):
            raise PermissionError("minute frozen task differs from original native/protocol/deadline")
        if parameters.full_input_hash != published.receipt.frozen.full_input_hash:
            raise PermissionError("minute original complete source changed")
        return checked

    def submit(self, command: MinuteCommand, marker: JsonValue) -> JsonValue:
        checked = self._marker(command, marker)
        if type(command) is ExportMinuteReplayZip:
            assert type(checked) is MinuteZipEffect
            return self._exports(command).export_minute(checked.job_id, owner_id=command.actor_id,
                request_id=checked.request_id, expected_result_hash=checked.result_hash).model_dump(mode="json")
        assert type(checked) is MinuteRunEffect
        return self.installation.commands.submit_create(checked.command,
            interaction_key=checked.interaction_key).model_dump(mode="json")

    def recover(self, command: MinuteCommand, marker: JsonValue) -> JsonValue | None:
        if type(command) is ExportMinuteReplayZip:
            checked = self._marker(command, marker)
            assert type(checked) is MinuteZipEffect
            receipt = self._exports(command).recover_minute(checked.job_id, owner_id=command.actor_id,
                request_id=checked.request_id, expected_result_hash=checked.result_hash)
            return None if receipt is None else receipt.model_dump(mode="json")
        return self.submit(command, marker)

    def close(self) -> None:
        if self.exports is not None:
            self.exports.artifact_store.close()
