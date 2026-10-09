"""Three-part execution binding without changing the original two-part study."""

from __future__ import annotations

from datetime import date
from typing import TYPE_CHECKING, Literal, Self

from pydantic import Field, field_validator, model_validator

from rquant.experiment_registry import DateRange
from rquant.minute_backtest_contracts import MAX_CODES
from rquant.minute_backtest_study_protocols import MinuteStudyProtocol, StudyHash
from rquant.runtime_contracts import RuntimeContractModel, canonical_sha256

if TYPE_CHECKING:
    from rquant.minute_backtest_commands import MinuteParameterRunConfig
    from rquant.minute_backtest_formal import MinuteExperimentProtocol

MinuteStudyExecutionPartition = Literal["training", "validation", "out_of_sample"]


class MinuteParameterStudySettings(RuntimeContractModel):
    score_profile: str = Field(min_length=1, max_length=128)
    top_n: int = Field(strict=True, ge=1, le=MAX_CODES)
    min_trades: int = Field(strict=True, ge=1)

    @field_validator("score_profile")
    @classmethod
    def original_profile(cls, value: str) -> str:
        from rquant.topn_selection import resolve_score_profiles

        resolve_score_profiles([value])
        return value


class MinuteParameterStudyBinding(RuntimeContractModel):
    schema_version: Literal[1] = 1
    protocol: MinuteStudyProtocol
    train_range: DateRange
    validation_range: DateRange
    frozen_outer_test_range: DateRange
    request_hash: StudyHash

    @model_validator(mode="after")
    def exact_primary_split(self) -> Self:
        if not (
            self.train_range.end_date < self.validation_range.start_date
            <= self.validation_range.end_date < self.frozen_outer_test_range.start_date
        ):
            raise ValueError("study execution ranges must preserve the original chronological separation")
        split = self.protocol.split
        if (
            split.train_start, split.train_end, split.test_start, split.test_end
        ) != (
            self.train_range.start_date, self.train_range.end_date,
            self.frozen_outer_test_range.start_date, self.frozen_outer_test_range.end_date,
        ):
            raise ValueError("primary study training/test must bind formal training/out-of-sample exactly")
        return self

    @classmethod
    def from_formal_protocol(
        cls, *, protocol: MinuteStudyProtocol, formal_protocol: MinuteExperimentProtocol,
        request_hash: str,
    ) -> MinuteParameterStudyBinding:
        # The original formal owner remains the constructor gate; keeping its import
        # local avoids a source-contract/adapter/runner import cycle.
        from rquant.minute_backtest_formal import MinuteExperimentProtocol

        formal = MinuteExperimentProtocol.model_validate(formal_protocol)
        return cls(protocol=protocol, train_range=formal.train_range,
            validation_range=formal.validation_range,
            frozen_outer_test_range=formal.frozen_outer_test_range, request_hash=request_hash)

    @property
    def study_id(self) -> str:
        return self.protocol.study_id

    @property
    def binding_hash(self) -> str:
        return canonical_sha256(self.model_dump(mode="json"))

    @property
    def settings(self) -> MinuteParameterStudySettings:
        return MinuteParameterStudySettings(score_profile=self.protocol.score_profile,
            top_n=self.protocol.top_n, min_trades=self.protocol.min_trades)

    def partition(self, trade_date: date) -> MinuteStudyExecutionPartition:
        for name, window in (
            ("training", self.train_range), ("validation", self.validation_range),
            ("out_of_sample", self.frozen_outer_test_range),
        ):
            if window.start_date <= trade_date <= window.end_date:
                return name
        raise ValueError("date is outside the declared three-part study execution windows")

    def verify_request(self, config: MinuteParameterRunConfig) -> None:
        if (config.study != self.settings or self.request_hash != canonical_sha256(config.model_dump(mode="json"))
                or (self.protocol.parameters, self.protocol.source.source_key, self.protocol.source.source_version,
                    self.protocol.source.full_input_hash, self.protocol.random_seed,
                    self.train_range, self.validation_range, self.frozen_outer_test_range) != (
                    config.parameters, config.source_key, config.source_version, config.full_input_hash,
                    config.random_seed, config.protocol.train_range, config.protocol.validation_range,
                    config.protocol.frozen_outer_test_range)):
            raise PermissionError("complete study differs from its original source/recipe/selection/request")


def verify_minute_parameter_study_request(
    binding: MinuteParameterStudyBinding | None, config: MinuteParameterRunConfig,
) -> None:
    if binding is None:
        if config.study is not None:
            raise PermissionError("study request lacks its complete runtime selection binding")
    else:
        binding.verify_request(config)
