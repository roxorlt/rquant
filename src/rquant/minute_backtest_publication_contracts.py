"""Complete source provenance and publication identities for formal minute research."""

from __future__ import annotations

import base64
import hashlib
from datetime import date
from pathlib import PurePosixPath
from typing import TYPE_CHECKING, Literal, Self

from pydantic import Field, model_validator

from rquant.definition_registry import StrategySpecRegistration
from rquant.intraday_feature_engine import IntradayFeatureConfig
from rquant.minute_backtest_contracts import (
    MAX_INPUT_BYTES, MAX_WORK_UNITS, CommitSha, FrozenMinuteRuntimeInput,
    MinuteReplayMaterial, MinuteReplayModel, MinuteReplayResultBudget, MinuteReplayWork,
    MinuteReplayExecutionProfile, Sha256,
)
from rquant.runtime_contracts import AwareUtcDatetime, canonical_sha256
from rquant.runtime_definition_bootstrap import BuiltinDefinitionStrategyBinding
from rquant.runtime_market_session import MarketCalendarAuthority

if TYPE_CHECKING:
    from rquant.minute_backtest_parameter_contracts import MinuteParameterSourceSeed, FrozenMinuteParameterResearchInput


MINUTE_FORMAL_CONTRACT = "minute-runtime-replay-input/v2"
MINUTE_FORMAL_TABLE = "minute_runtime_replay_input"
MINUTE_AUDIT_RULE = "minute-formal-source/v2"
MAX_MINUTE_CONTROL_BYTES = 1_048_576


class MinuteRuntimeContent(MinuteReplayModel):
    """Every core input field except the two identities created by publication."""

    contract: Literal["minute-runtime-replay-input/v1"] = "minute-runtime-replay-input/v1"
    source_key: str = Field(pattern=r"^[a-zA-Z0-9_.:-]{1,128}$")
    source_version: int = Field(ge=1)
    owner_id: str = Field(min_length=1, max_length=128)
    producer_commit: CommitSha
    available_at: AwareUtcDatetime
    start_date: date
    end_date: date
    complete_through: AwareUtcDatetime
    warmup_available_at: AwareUtcDatetime
    warmup_complete: Literal[True]
    holding_tail_complete: Literal[True]
    strategy: BuiltinDefinitionStrategyBinding
    feature_config: IntradayFeatureConfig
    market_calendar: MarketCalendarAuthority
    execution_profile: MinuteReplayExecutionProfile
    work: MinuteReplayWork
    result_budget: MinuteReplayResultBudget
    tick_times: tuple[AwareUtcDatetime, ...] = Field(min_length=1, max_length=MAX_WORK_UNITS)
    materials: tuple[MinuteReplayMaterial, ...] = Field(min_length=1, max_length=MAX_WORK_UNITS * 4 + 128)

    @classmethod
    def from_runtime(cls, value: FrozenMinuteRuntimeInput) -> MinuteRuntimeContent:
        return cls.model_validate(value.model_dump(mode="python", exclude={"audit_run_id", "dataset_snapshot_id"}))

    def freeze(self, *, audit_run_id: str, dataset_snapshot_id: str) -> FrozenMinuteRuntimeInput:
        return FrozenMinuteRuntimeInput.model_validate(self.model_dump(mode="python") | {
            "audit_run_id": audit_run_id, "dataset_snapshot_id": dataset_snapshot_id})


class MinuteOriginMaterial(MinuteReplayModel):
    object_key: str = Field(pattern=r"^[a-zA-Z0-9][a-zA-Z0-9_.:-]{0,199}$")
    content_base64: str = Field(max_length=((MAX_INPUT_BYTES + 2) // 3) * 4)
    content_sha256: Sha256
    format: Literal["parquet", "json", "sqlite", "bytes"]

    def payload(self) -> bytes:
        try:
            data = base64.b64decode(self.content_base64, validate=True)
        except (ValueError, UnicodeEncodeError) as exc:
            raise ValueError("minute original material is not canonical base64") from exc
        if len(data) > MAX_INPUT_BYTES or base64.b64encode(data).decode("ascii") != self.content_base64:
            raise ValueError("minute original material exceeds bytes or is not canonical base64")
        return data

    @model_validator(mode="after")
    def exact_bytes(self) -> Self:
        if hashlib.sha256(self.payload()).hexdigest() != self.content_sha256:
            raise ValueError("minute original bytes differ from their hash")
        return self


class MinuteCaptureLineage(MinuteReplayModel):
    object_key: str
    content_sha256: Sha256
    acquisition_commit: CommitSha | None
    captured_at: AwareUtcDatetime | None
    timing_evidence_object_key: str | None
    collector_id: str = Field(default="unrecorded", min_length=1, max_length=128)


class MinuteCodeFile(MinuteReplayModel):
    logical_name: str = Field(min_length=1, max_length=256)
    content_sha256: Sha256

    @model_validator(mode="after")
    def logical_only(self) -> Self:
        path = PurePosixPath(self.logical_name)
        if path.is_absolute() or path.as_posix() != self.logical_name or any(x in {".", ".."} for x in path.parts):
            raise ValueError("minute code names must be logical names, not authority paths")
        return self


class MinuteVisibilityPolicy(MinuteReplayModel):
    policy_id: str = Field(pattern=r"^[a-zA-Z0-9_.:-]{1,128}$")
    version: int = Field(ge=1)
    timestamp_semantics: Literal["bar_end", "provider_snapshot"]
    market_event_basis: str = Field(min_length=1, max_length=2048)
    market_visibility_basis: str = Field(min_length=1, max_length=2048)
    candidate_visibility_basis: str = Field(min_length=1, max_length=2048)
    constraint_visibility_basis: str = Field(min_length=1, max_length=2048)
    native_definition_basis: str = Field(min_length=1, max_length=2048)
    limitations: str = Field(min_length=1, max_length=4096)

    @property
    def fingerprint(self) -> str:
        return canonical_sha256(self.model_dump(mode="json"))


class MinutePublicationEvidence(MinuteReplayModel):
    kind: Literal["market", "candidate", "constraint"]
    sequence: int = Field(ge=0)
    material_path: str = Field(min_length=1, max_length=256)
    origin_object_key: str
    pointer_object_key: str | None = None
    completion_receipt_object_key: str | None = None
    published_at: AwareUtcDatetime
    time_basis: Literal["actual", "modeled"]


class MinuteDerivation(MinuteReplayModel):
    material_path: str = Field(min_length=1, max_length=256)
    origin_object_keys: tuple[str, ...] = Field(min_length=1, max_length=MAX_WORK_UNITS)
    method: Literal["identity", "retained_research_archive", "research_derivative"]
    transformation_fingerprint: Sha256
    time_basis: Literal["actual", "modeled"]


class MinuteProvenance(MinuteReplayModel):
    source_kind: Literal["captured", "reconstructed"]
    capture_lineage: tuple[MinuteCaptureLineage, ...] = Field(min_length=1, max_length=MAX_WORK_UNITS)
    publication_evidence: tuple[MinutePublicationEvidence, ...] = Field(min_length=1, max_length=MAX_WORK_UNITS)
    extracted_at: AwareUtcDatetime
    published_at: AwareUtcDatetime
    extractor_code_commit: CommitSha
    extractor_fingerprint: Sha256
    research_code_commit: CommitSha
    source_index_sha256: Sha256
    code_files: tuple[MinuteCodeFile, ...] = Field(min_length=1, max_length=MAX_WORK_UNITS)
    replay_start: AwareUtcDatetime
    replay_end: AwareUtcDatetime
    native_definition_replay_available_at: AwareUtcDatetime
    visibility_policy: MinuteVisibilityPolicy | None

    @model_validator(mode="after")
    def honest_time_basis(self) -> Self:
        if self.extracted_at > self.published_at or self.replay_start > self.replay_end:
            raise ValueError("minute extract/publication or replay times are unordered")
        if self.native_definition_replay_available_at > self.replay_start:
            raise ValueError("minute native replay bootstrap is from the future")
        if len({x.logical_name for x in self.code_files}) != len(self.code_files):
            raise ValueError("minute research source bundle has duplicate logical files")
        if self.source_kind == "captured":
            if self.visibility_policy is not None or any(x.time_basis != "actual" for x in self.publication_evidence):
                raise ValueError("captured minute source cannot use modeled publication times")
            if any(x.captured_at is None or x.acquisition_commit is None or x.timing_evidence_object_key is None
                   or x.collector_id == "unrecorded" for x in self.capture_lineage):
                raise ValueError("captured minute source lacks real capture lineage")
            if any(x.kind == "market" and (x.pointer_object_key is None or x.completion_receipt_object_key is None)
                   for x in self.publication_evidence):
                raise ValueError("captured minute source lacks original per-publication completion proof")
        elif self.visibility_policy is None:
            raise ValueError("reconstructed minute source requires an explicit modeled visibility policy")
        return self


class MinuteFormalWork(MinuteReplayModel):
    runtime_work: MinuteReplayWork
    origin_physical_rows: int = Field(ge=0, le=MAX_WORK_UNITS)
    provenance_record_count: int = Field(ge=1, le=MAX_WORK_UNITS)

    @property
    def work_units(self) -> int:
        return self.runtime_work.work_units + self.origin_physical_rows + self.provenance_record_count

    @model_validator(mode="after")
    def bounded(self) -> Self:
        if self.work_units > MAX_WORK_UNITS:
            raise ValueError("minute formal work exceeds the original 20000-unit budget")
        return self


class _MinuteSourceBody(MinuteReplayModel):
    contract: Literal["minute-runtime-replay-input/v2"] = MINUTE_FORMAL_CONTRACT
    native_registration: StrategySpecRegistration
    wrapper_registration: StrategySpecRegistration
    provenance: MinuteProvenance
    origin_materials: tuple[MinuteOriginMaterial, ...] = Field(min_length=1, max_length=MAX_WORK_UNITS)
    derivations: tuple[MinuteDerivation, ...] = Field(min_length=1, max_length=MAX_WORK_UNITS)
    formal_work: MinuteFormalWork
    result_budget: MinuteReplayResultBudget

    @model_validator(mode="after")
    def complete_source(self) -> Self:
        _validate_minute_source_body(self, research_key="minute_runtime_replay")
        return self


class MinuteSourceContentSeed(_MinuteSourceBody):
    runtime: MinuteRuntimeContent

    @property
    def seed_hash(self) -> str:
        return canonical_sha256(self.model_dump(mode="json"))

    def freeze(self, *, audit_run_id: str, dataset_snapshot_id: str) -> FrozenMinuteResearchInput:
        data = self.model_dump(mode="python")
        data["runtime"] = self.runtime.freeze(audit_run_id=audit_run_id, dataset_snapshot_id=dataset_snapshot_id)
        return FrozenMinuteResearchInput.model_validate(data)


class FrozenMinuteResearchInput(_MinuteSourceBody):
    runtime: FrozenMinuteRuntimeInput

    @property
    def source_content_seed(self) -> MinuteSourceContentSeed:
        data = self.model_dump(mode="python")
        data["runtime"] = MinuteRuntimeContent.from_runtime(self.runtime)
        return MinuteSourceContentSeed.model_validate(data)

    @property
    def full_input_hash(self) -> str:
        return canonical_sha256(self.model_dump(mode="json"))

    @property
    def core_input_hash(self) -> str:
        return self.runtime.input_hash


def _validate_minute_source_body(
    value: _MinuteSourceBody | MinuteParameterSourceSeed | FrozenMinuteParameterResearchInput, *,
    research_key: str,
) -> None:
    """Full original lineage/proof gate shared by separately typed contracts."""
    runtime = value.runtime
    keys = tuple(x.object_key for x in value.origin_materials)
    if len(set(keys)) != len(keys):
        raise ValueError("minute original object keys repeat")
    origins = {x.object_key: x for x in value.origin_materials}
    lineage = {x.object_key: x for x in value.provenance.capture_lineage}
    if len(lineage) != len(value.provenance.capture_lineage) or set(lineage) != set(origins):
        raise ValueError("minute capture lineage does not bind every full original")
    for key, item in lineage.items():
        if item.content_sha256 != origins[key].content_sha256 or (item.timing_evidence_object_key is not None
                and item.timing_evidence_object_key not in origins):
            raise ValueError("minute capture lineage original bytes or evidence differ")
        if item.captured_at is not None and item.captured_at > value.provenance.extracted_at:
            raise ValueError("minute capture occurs after actual extraction")
    material = {x.relative_path: x for x in runtime.materials}
    derived = {x.material_path: x for x in value.derivations}
    if len(derived) != len(value.derivations) or set(derived) != set(material):
        raise ValueError("minute derivations do not cover the full execution archive")
    for key, item in derived.items():
        if len(set(item.origin_object_keys)) != len(item.origin_object_keys) or not set(item.origin_object_keys) <= set(origins):
            raise ValueError("minute derivation parents are missing or duplicate")
        if item.method in {"identity", "retained_research_archive"} and (len(item.origin_object_keys) != 1
                or origins[item.origin_object_keys[0]].payload() != material[key].payload()):
            raise ValueError("minute identity derivation changed original bytes")
        if value.provenance.source_kind == "captured" and (item.method != "identity" or item.time_basis != "actual"):
            raise ValueError("captured minute execution must retain actual original bytes and times")
    proofs = value.provenance.publication_evidence
    if len({(x.kind, x.sequence) for x in proofs}) != len(proofs):
        raise ValueError("minute publication evidence repeats")
    if any(x.material_path not in material or x.origin_object_key not in origins
           or (x.pointer_object_key is not None and x.pointer_object_key not in origins)
           or (x.completion_receipt_object_key is not None and x.completion_receipt_object_key not in origins) for x in proofs):
        raise ValueError("minute publication evidence lacks full archive parents")
    if (value.wrapper_registration.logical_id, value.wrapper_registration.version,
        value.wrapper_registration.producer_commit) != (research_key, 1, runtime.producer_commit):
        raise ValueError("minute wrapper registration differs from current research definition")
    binding = runtime.strategy
    native = value.native_registration
    if (native.logical_id, native.version, native.spec.spec_fingerprint, native.fingerprint,
        native.executable_fingerprint, native.candidate_schema_fingerprint, native.producer_commit) != (
        binding.strategy_id, binding.strategy_version, binding.strategy_spec_fingerprint,
        binding.registration_fingerprint, binding.executable_fingerprint,
        binding.candidate_schema_fingerprint, runtime.producer_commit):
        raise ValueError("minute full native registration differs from original runtime binding")
    if native.available_at > value.provenance.published_at:
        raise ValueError("minute native registration is not visible at actual publication")
    if value.wrapper_registration.available_at > value.provenance.published_at:
        raise ValueError("minute wrapper is not visible at actual publication")
    if (value.provenance.research_code_commit, value.provenance.replay_start, value.provenance.replay_end,
        value.provenance.native_definition_replay_available_at) != (runtime.producer_commit, runtime.tick_times[0],
            runtime.tick_times[-1], runtime.available_at):
        raise ValueError("minute code or modeled replay asof differs from native input")
    if value.provenance.visibility_policy is not None and value.provenance.visibility_policy.timestamp_semantics != runtime.execution_profile.timestamp_semantics:
        raise ValueError("minute modeled visibility policy uses different timestamp semantics")
    if value.formal_work.runtime_work != runtime.work or value.result_budget != runtime.result_budget:
        raise ValueError("minute formal work or budgets differ from native runtime")
    if sum(len(x.payload()) for x in (*value.origin_materials, *runtime.materials)) > MAX_INPUT_BYTES:
        raise ValueError("minute original plus execution material exceeds 16 MiB")
    if len(value.model_dump_json().encode()) > MAX_INPUT_BYTES:
        raise ValueError("minute complete formal payload exceeds 16 MiB")
