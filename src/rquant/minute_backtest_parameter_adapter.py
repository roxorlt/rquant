"""Complete parameter bindings on the existing formal minute execution gate."""

from __future__ import annotations

from pathlib import Path
from typing import Literal, Self

import pandas as pd
from pydantic import Field, model_validator

from rquant.minute_backtest_contracts import MAX_INPUT_BYTES, Sha256
from rquant.minute_backtest_formal_adapter import (
    MinuteFormalParameters,
    MinuteFormalReplayAdapter,
    MinuteFormalReplayResult,
    MinuteFormalRunInput,
)
from rquant.minute_backtest_parameter_contracts import (
    FrozenMinuteParameterResearchInput,
    MinuteParameterRuntimeReceipt,
)
from rquant.minute_backtest_parameter_producer import (
    MinuteParameterPreparedPublication,
    MinuteParameterPublicationReceipt,
    MinuteParameterReplayCatalog,
    MinuteParameterResolvedReadUnit,
)
from rquant.minute_backtest_parameter_runner import (
    MinuteParameterReplayResult,
    minute_parameter_result_tables,
    run_minute_parameter_replay,
)
from rquant.minute_backtest_parameter_source import read_minute_parameter_input_table
from rquant.minute_backtest_parameters import MinuteFrequency, MinuteParameterSet
from rquant.research_run_spec import ResearchJobType
from rquant.research_snapshot import ResearchExecutionSession
from rquant.strict_json import strict_model_validate_json

_MAX_PREPARED_PUBLICATION_BYTES = 1_048_576


def _decode_prepared(payload: str) -> MinuteParameterPreparedPublication:
    if len(payload.encode("utf-8")) > _MAX_PREPARED_PUBLICATION_BYTES:
        raise ValueError("prepared publication exceeds the original control byte budget")
    prepared = strict_model_validate_json(MinuteParameterPreparedPublication, payload)
    if payload != prepared.model_dump_json(exclude_computed_fields=True):
        raise ValueError("prepared publication must preserve the complete canonical JSON")
    return prepared


class MinuteParameterFormalParameters(MinuteFormalParameters):
    native_strategy_id: str = Field(pattern=r"^(?:np|ap|gp)\.[a-z2-7]{52}$")
    parameter_set_json: str = Field(min_length=1, max_length=MAX_INPUT_BYTES)
    parameter_hash: Sha256
    source_frequency: MinuteFrequency
    prepared_publication_json: str | None = Field(
        default=None, max_length=_MAX_PREPARED_PUBLICATION_BYTES
    )

    @model_validator(mode="after")
    def complete_parameter_binding(self) -> Self:
        if len(self.parameter_set_json.encode("utf-8")) > MAX_INPUT_BYTES:
            raise ValueError("complete parameter JSON exceeds the original input byte budget")
        recipe = MinuteParameterSet.model_validate_json(self.parameter_set_json)
        if self.parameter_set_json != recipe.model_dump_json():
            raise ValueError("parameter recipe JSON must preserve every canonical field")
        if (self.parameter_hash, self.native_strategy_id, self.source_frequency) != (
            recipe.fingerprint,
            recipe.definition_id,
            recipe.parameters.freq,
        ):
            raise ValueError("parameter hash/definition/frequency differs from the complete recipe")
        if self.prepared_publication_json is not None:
            prepared = _decode_prepared(self.prepared_publication_json)
            if (
                self.source_key,
                self.source_version,
                self.owner_id,
                self.full_input_hash,
                self.core_input_hash,
                self.seed_hash,
                self.parameter_hash,
                self.work_units,
            ) != (
                prepared.source_key,
                prepared.source_version,
                prepared.owner_id,
                prepared.full_input_hash,
                prepared.core_input_hash,
                prepared.seed_hash,
                prepared.parameter_hash,
                prepared.work_units,
            ):
                raise ValueError(
                    "parameter identity/work differs from its complete prepared material"
                )
        return self

    @classmethod
    def from_frozen(
        cls, value: FrozenMinuteParameterResearchInput
    ) -> MinuteParameterFormalParameters:
        runtime = value.runtime
        return cls(
            source_key=runtime.source_key,
            source_version=runtime.source_version,
            owner_id=runtime.owner_id,
            full_input_hash=value.full_input_hash,
            core_input_hash=value.core_input_hash,
            seed_hash=value.source_content_seed.seed_hash,
            profile_hash=runtime.execution_profile.profile_hash,
            native_strategy_id=runtime.strategy.strategy_id,
            native_strategy_version=runtime.strategy.strategy_version,
            native_registration_hash=value.native_registration.record_hash,
            wrapper_registration_hash=value.wrapper_registration.record_hash,
            work_units=value.formal_work.work_units,
            parameter_set_json=runtime.parameters.model_dump_json(),
            parameter_hash=runtime.parameters.fingerprint,
            source_frequency=runtime.source_frequency,
        )

    @classmethod
    def from_prepared(
        cls,
        value: FrozenMinuteParameterResearchInput,
        prepared: MinuteParameterPreparedPublication,
    ) -> MinuteParameterFormalParameters:
        original = cls.from_frozen(value)
        return cls.model_validate(
            original.model_dump(mode="python")
            | {
                "prepared_publication_json": prepared.model_dump_json(exclude_computed_fields=True),
                "work_units": prepared.work_units,
            }
        )


class MinuteParameterFormalRunInput(MinuteFormalRunInput):
    kind: Literal["minute_parameter_replay"] = "minute_parameter_replay"
    parameters: MinuteParameterFormalParameters

    @classmethod
    def from_frozen(
        cls, value: FrozenMinuteParameterResearchInput
    ) -> MinuteParameterFormalRunInput:
        return cls(
            start_date=value.runtime.start_date,
            end_date=value.runtime.end_date,
            parameters=MinuteParameterFormalParameters.from_frozen(value),
        )


class MinuteParameterFormalReplayResult(MinuteFormalReplayResult):
    publication: MinuteParameterPublicationReceipt
    replay: MinuteParameterReplayResult

    @model_validator(mode="after")
    def complete_parameter_result_binding(self) -> Self:
        original = self.publication.frozen.runtime
        if (self.replay.parameters, self.replay.parameter_work, self.replay.study_binding) != (
            original.parameters,
            original.parameter_work,
            original.study_binding,
        ):
            raise ValueError("parameter result recipe/work/study differs from the complete publication")
        return self


class MinuteParameterFormalReplayAdapter(MinuteFormalReplayAdapter):
    adapter_id = "minute-parameter-replay"
    adapter_version = "1"
    strategy_name = "minute_parameter_replay"
    snapshot_strategy_name = "minute_parameter_replay"
    job_type = ResearchJobType.STRATEGY_REPLAY
    catalog: MinuteParameterReplayCatalog

    def __init__(self, catalog: MinuteParameterReplayCatalog, *,
        resolved_read_unit: MinuteParameterResolvedReadUnit | None = None) -> None:
        if resolved_read_unit is not None and type(resolved_read_unit) is not MinuteParameterResolvedReadUnit:
            raise TypeError("parameter replay needs its exact lexical resolved source unit")
        super().__init__(catalog)
        self.resolved_read_unit = resolved_read_unit

    def expected(
        self, parameters: MinuteParameterFormalParameters
    ) -> MinuteParameterPublicationReceipt:
        if parameters.prepared_publication_json is None:
            original = super().expected(parameters)
            if not isinstance(original, MinuteParameterPublicationReceipt):
                raise PermissionError(
                    "parameter static source has an incompatible receipt contract"
                )
            return original
        prepared = _decode_prepared(parameters.prepared_publication_json)
        expected = (self.catalog.resolve_prepared(prepared) if self.resolved_read_unit is None
            else self.resolved_read_unit.resolve(self.catalog, prepared))
        if parameters != MinuteParameterFormalParameters.from_prepared(expected.frozen, prepared):
            raise PermissionError(
                "parameter fields differ from complete independent prepared receipts"
            )
        return expected

    def _catalog_model(self) -> type[MinuteParameterReplayCatalog]:
        return MinuteParameterReplayCatalog

    def _parameter_model(self) -> type[MinuteParameterFormalParameters]:
        return MinuteParameterFormalParameters

    def _read_source(self, store: ResearchExecutionSession) -> FrozenMinuteParameterResearchInput:
        return read_minute_parameter_input_table(store._conn)

    def _replay_runtime(
        self, value: FrozenMinuteParameterResearchInput, root: Path
    ) -> MinuteParameterReplayResult:
        return run_minute_parameter_replay(
            value.runtime,
            expected=MinuteParameterRuntimeReceipt(frozen=value.runtime),
            research_root=root / "replay",
        )

    def _formal_result_model(self) -> type[MinuteParameterFormalReplayResult]:
        return MinuteParameterFormalReplayResult

    def _result_tables(self, replay: MinuteParameterReplayResult) -> dict[str, pd.DataFrame]:
        return minute_parameter_result_tables(replay)
