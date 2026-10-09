"""Closed study inputs for the original minute replay and score owners."""

from __future__ import annotations

from datetime import date
from typing import Annotated, Literal, Self
from zoneinfo import ZoneInfo

from pydantic import Field, StringConstraints, field_validator, model_validator

from rquant import topn_selection
from rquant.minute_backtest_contracts import MAX_CODES, MAX_DATE_SPAN
from rquant.minute_backtest_parameters import MinuteAuctionGapParameters, MinuteParameterSet
from rquant.runtime_contracts import AwareUtcDatetime, RuntimeContractModel, canonical_sha256

StudyHash = Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{64}$")]
StudyCommit = Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{40}$")]
StudyIdentity = Annotated[str, StringConstraints(min_length=1, max_length=256)]
StudyDefinitionId = Annotated[str, StringConstraints(pattern=r"^[a-zA-Z0-9_.-]{1,64}$")]
StudyFrequency = Literal["1min", "5min", "15min", "30min", "60min"]
StudyPartition = Literal["train", "test"]
SHANGHAI = ZoneInfo("Asia/Shanghai")


def study_feature_names() -> frozenset[str]:
    profiles = topn_selection.default_score_profiles()
    return frozenset(
        [term.name for profile in profiles for term in profile.terms]
        + [profile.env_gate.feature for profile in profiles if profile.env_gate is not None]
    )


class MinuteStudySource(RuntimeContractModel):
    """Verified archive identity; published_at is not an event's PIT clock."""

    source_key: StudyIdentity
    source_version: int = Field(strict=True, ge=1)
    owner_id: StudyIdentity
    full_input_hash: StudyHash
    dataset_snapshot_id: StudyIdentity
    frequency: StudyFrequency
    start_date: date
    end_date: date
    published_at: AwareUtcDatetime

    @model_validator(mode="after")
    def validate_window(self) -> Self:
        if not 1 <= (self.end_date - self.start_date).days + 1 <= MAX_DATE_SPAN:
            raise ValueError("source window is reversed or exceeds the minute input budget")
        return self


class MinuteStudyHead(RuntimeContractModel):
    definition_id: StudyDefinitionId
    definition_version: int = Field(strict=True, ge=1)
    evaluator_semantic_version: Annotated[str, StringConstraints(min_length=1, max_length=64)]
    parameter_fingerprint: StudyHash
    registration_fingerprint: StudyHash
    spec_fingerprint: StudyHash
    executable_fingerprint: StudyHash
    producer_commit: StudyCommit


class MinuteStudySplit(RuntimeContractModel):
    train_start: date
    train_end: date
    test_start: date
    test_end: date

    @model_validator(mode="after")
    def validate_split(self) -> Self:
        if not self.train_start <= self.train_end < self.test_start <= self.test_end:
            raise ValueError("training and test windows must be ordered and disjoint")
        if (self.test_end - self.train_start).days + 1 > MAX_DATE_SPAN:
            raise ValueError("study window exceeds the original minute input budget")
        return self

    def partition(self, trade_date: date) -> StudyPartition:
        if self.train_start <= trade_date <= self.train_end:
            return "train"
        if self.test_start <= trade_date <= self.test_end:
            return "test"
        raise ValueError("date is outside the declared training and test windows")


class MinuteStudyProtocol(RuntimeContractModel):
    source: MinuteStudySource
    head: MinuteStudyHead
    parameters: MinuteParameterSet
    split: MinuteStudySplit
    score_profile: Annotated[str, StringConstraints(min_length=1, max_length=128)]
    top_n: int = Field(strict=True, ge=1, le=MAX_CODES)
    min_trades: int = Field(strict=True, ge=1)
    random_seed: int = Field(strict=True, ge=0, lt=2**63)
    requested_at: AwareUtcDatetime

    @field_validator("score_profile")
    @classmethod
    def known_profile(cls, value: str) -> str:
        topn_selection.resolve_score_profiles([value])
        return value

    @model_validator(mode="after")
    def validate_bindings(self) -> Self:
        if self.source.frequency != self.parameters.parameters.freq:
            raise ValueError("source and complete parameter frequency differ")
        if (
            self.head.parameter_fingerprint != self.parameters.fingerprint
            or self.head.definition_id != self.parameters.definition_id
            or self.head.definition_version != self.parameters.definition_version
            or self.head.evaluator_semantic_version != self.parameters.evaluator_semantic_version
        ):
            raise ValueError("head does not bind the complete parameter recipe")
        if (
            not self.source.start_date
            <= self.split.train_start
            <= self.split.test_end
            <= self.source.end_date
        ):
            raise ValueError("source does not cover the full declared split")
        if self.source.published_at > self.requested_at:
            raise ValueError("source was not published when the study was requested")
        if isinstance(self.parameters.parameters, MinuteAuctionGapParameters) and (
            date.fromisoformat(self.parameters.parameters.start_date) > self.split.train_start
            or date.fromisoformat(self.parameters.parameters.end_date) < self.split.test_end
        ):
            raise ValueError("auction recipe dates must cover the declared study window")
        return self

    @property
    def profile_fingerprint(self) -> str:
        return canonical_sha256(topn_selection.resolve_score_profiles([self.score_profile])[0])

    @property
    def study_id(self) -> str:
        profile = topn_selection.resolve_score_profiles([self.score_profile])[0]
        return canonical_sha256(
            {
                "protocol": self.model_dump(mode="json"),
                "score_profile": profile.model_dump(mode="json"),
            }
        )

    @property
    def worker_seed(self) -> int:
        return self.random_seed


class MinuteStudyFeature(RuntimeContractModel):
    name: StudyIdentity
    value: float | None = Field(strict=True, allow_inf_nan=False)
    available_at: AwareUtcDatetime

    @field_validator("name")
    @classmethod
    def known_feature(cls, value: str) -> str:
        if value not in study_feature_names():
            raise ValueError("feature is outside the original score profiles")
        return value


class MinuteStudyCandidate(RuntimeContractModel):
    """Facts copied by a verified source adapter, never a claim of authority."""

    source: MinuteStudySource
    head: MinuteStudyHead
    parameter_fingerprint: StudyHash
    candidate_id: StudyDefinitionId
    ts_code: Annotated[str, StringConstraints(pattern=r"^[0-9]{6}\.(SH|SZ|BJ)$")]
    trade_date: date
    event_time: AwareUtcDatetime
    available_at: AwareUtcDatetime
    features: tuple[MinuteStudyFeature, ...]

    @model_validator(mode="after")
    def validate_event(self) -> Self:
        if self.event_time.astimezone(SHANGHAI).date() != self.trade_date:
            raise ValueError("event time and trade date differ")
        if self.available_at < self.event_time:
            raise ValueError("candidate cannot be available before its event")
        if len({item.name for item in self.features}) != len(self.features):
            raise ValueError("duplicate score feature")
        if self.parameter_fingerprint != self.head.parameter_fingerprint:
            raise ValueError("candidate parameter identity differs from its head")
        return self
