"""Complete parameter replay input, separate from the closed native contracts."""

from __future__ import annotations

import hashlib
import inspect
import sys
from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager
from contextvars import ContextVar
from copy import deepcopy
from dataclasses import dataclass, field
from datetime import date
from types import CodeType, FunctionType, MappingProxyType, MethodType
from typing import Literal, Self, TypeVar

from pydantic import (
    BaseModel, Field, SerializerFunctionWrapHandler, model_serializer, model_validator,
)
from pydantic.fields import FieldInfo

from rquant.definition_registry import (
    FeatureContractRegistration, StrategySpecRegistration, _canonical_feature_contract, _canonical_strategy_spec,
)
from rquant.minute_backtest_contracts import (
    MAX_INPUT_BYTES, MAX_WORK_UNITS, CommitSha, MinuteReplayExecutionProfile,
    MinuteReplayMaterial, MinuteReplayModel, MinuteReplayResultBudget,
    MinuteReplayWork, Sha256, _validate_minute_replay_archive,
)
from rquant.minute_backtest_parameters import MinuteFrequency, MinuteParameterSet
from rquant.minute_backtest_parameter_features import MinuteParameterSessionFacts
from rquant.minute_backtest_parameter_study import MinuteParameterStudyBinding
from rquant.minute_backtest_publication_contracts import (
    MinuteFormalWork, _MinuteSourceBody, _validate_minute_source_body,
)
from rquant.runtime_contracts import AwareUtcDatetime, canonical_sha256
from rquant.runtime_market_session import MarketCalendarAuthority
from rquant.executable_dependencies import (
    ExecutableBinding, ExecutableDependencyError, ExecutableDependencyGuard,
    _referenced_global_paths,
    capture_executable_dependency_guard,
)

PARAMETER_RUNTIME_CONTRACT = "minute-parameter-runtime-input/v1"
PARAMETER_SOURCE_CONTRACT = "minute-parameter-replay-input/v1"
PARAMETER_SOURCE_TABLE = "minute_parameter_replay_input"
PARAMETER_RESEARCH_KEY = "minute_parameter_replay"


class MinuteParameterStrategyBinding(MinuteReplayModel):
    strategy_id: str = Field(pattern=r"^(?:np|ap|gp)\.[a-z2-7]{52}$")
    strategy_version: Literal[1] = 1
    registration_fingerprint: Sha256
    candidate_schema_fingerprint: Sha256
    strategy_spec_fingerprint: Sha256
    executable_fingerprint: Sha256

    @classmethod
    def from_registration(cls, registration: StrategySpecRegistration, *,
        parameters: MinuteParameterSet, producer_commit: str,
    ) -> MinuteParameterStrategyBinding:
        from rquant.minute_backtest_parameter_definition import (
            build_minute_parameter_definition, minute_parameter_validation_plan,
        )

        record = StrategySpecRegistration.model_validate(registration.model_dump(mode="python"))
        plan = minute_parameter_validation_plan(parameters, producer_commit=producer_commit)
        if plan is None:
            definition = build_minute_parameter_definition(parameters, producer_commit=producer_commit)
            expected = (_canonical_strategy_spec(definition.spec), definition.executable_fingerprint,
                definition.candidate_schema_fingerprint)
        else:
            expected = (plan.native_spec, plan.native_executable_fingerprint, plan.candidate_schema_fingerprint)
        if (record.logical_id, record.version, record.spec, record.executable_fingerprint,
            record.candidate_schema_fingerprint, record.producer_commit) != (
            parameters.definition_id, parameters.definition_version, *expected, producer_commit):
            raise ValueError("registration differs from the complete parameter definition")
        return cls(strategy_id=record.logical_id, strategy_version=1,
            registration_fingerprint=record.fingerprint,
            candidate_schema_fingerprint=record.candidate_schema_fingerprint,
            strategy_spec_fingerprint=record.spec.spec_fingerprint,
            executable_fingerprint=record.executable_fingerprint)


class MinuteParameterWork(MinuteReplayModel):
    runtime_work: MinuteReplayWork
    prefix_rows: int = Field(ge=0, le=MAX_WORK_UNITS)
    history_rows: int = Field(ge=0, le=MAX_WORK_UNITS)
    derived_rows: int = Field(ge=0, le=MAX_WORK_UNITS)
    lifecycle_rows: int = Field(ge=0, le=MAX_WORK_UNITS)
    session_fact_rows: int = Field(default=0, ge=0, le=MAX_WORK_UNITS)
    study_prefix_rows: int = Field(default=0, ge=0, le=MAX_WORK_UNITS)
    study_history_rows: int = Field(default=0, ge=0, le=MAX_WORK_UNITS)
    study_feature_rows: int = Field(default=0, ge=0, le=MAX_WORK_UNITS)
    study_selection_rows: int = Field(default=0, ge=0, le=MAX_WORK_UNITS)

    @model_serializer(mode="wrap")
    def original_default_fields(self, handler: SerializerFunctionWrapHandler) -> dict[str, object]:
        value = handler(self)
        for name in ("study_prefix_rows", "study_history_rows", "study_feature_rows", "study_selection_rows"):
            if getattr(self, name) == 0:
                value.pop(name, None)
        return value

    @property
    def work_units(self) -> int:
        return (self.runtime_work.work_units + self.prefix_rows + self.history_rows
            + self.derived_rows + self.lifecycle_rows + self.session_fact_rows
            + self.study_prefix_rows + self.study_history_rows + self.study_feature_rows
            + self.study_selection_rows)

    @model_validator(mode="after")
    def bounded(self) -> Self:
        if self.work_units > MAX_WORK_UNITS:
            raise ValueError("parameter replay exceeds the original 20000-unit total budget")
        return self


class MinuteParameterRuntimeContent(MinuteReplayModel):
    """Only AuditID and SnapshotID are absent before original publication."""

    contract: Literal["minute-parameter-runtime-input/v1"] = PARAMETER_RUNTIME_CONTRACT
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
    parameters: MinuteParameterSet
    study_binding: MinuteParameterStudyBinding | None = None
    source_frequency: MinuteFrequency
    strategy: MinuteParameterStrategyBinding
    market_calendar: MarketCalendarAuthority
    execution_profile: MinuteReplayExecutionProfile
    parameter_work: MinuteParameterWork
    session_facts: tuple[MinuteParameterSessionFacts, ...] = Field(default=(), max_length=MAX_WORK_UNITS)
    result_budget: MinuteReplayResultBudget = MinuteReplayResultBudget()
    tick_times: tuple[AwareUtcDatetime, ...] = Field(min_length=1, max_length=MAX_WORK_UNITS)
    materials: tuple[MinuteReplayMaterial, ...] = Field(min_length=1, max_length=MAX_WORK_UNITS * 4 + 128)

    @model_serializer(mode="wrap")
    def original_default_fields(self, handler: SerializerFunctionWrapHandler) -> dict[str, object]:
        value = handler(self)
        if self.study_binding is None:
            value.pop("study_binding", None)
        return value

    @model_validator(mode="after")
    def complete_parameter_input(self) -> Self:
        from rquant.minute_backtest_parameter_definition import (
            build_minute_parameter_definition, minute_parameter_validation_plan,
        )

        _validate_minute_replay_archive(self)
        if self.source_frequency != self.parameters.parameters.freq:
            raise ValueError("complete parameter frequency differs from the physical source declaration")
        if self.study_binding is not None:
            protocol = self.study_binding.protocol
            head = protocol.head
            if (protocol.parameters, protocol.source.owner_id, protocol.source.frequency,
                    protocol.source.start_date, protocol.source.end_date, head.definition_id,
                    head.definition_version, head.registration_fingerprint, head.spec_fingerprint,
                    head.executable_fingerprint, head.producer_commit) != (
                    self.parameters, self.owner_id, self.source_frequency, self.start_date, self.end_date,
                    self.strategy.strategy_id, self.strategy.strategy_version,
                    self.strategy.registration_fingerprint, self.strategy.strategy_spec_fingerprint,
                    self.strategy.executable_fingerprint, self.producer_commit):
                raise ValueError("complete study binding differs from the real runtime source/head/recipe")
        keys = tuple((item.trade_date, item.ts_code) for item in self.session_facts)
        if keys != tuple(sorted(set(keys))) or any(not self.start_date <= item.trade_date <= self.end_date
                for item in self.session_facts):
            raise ValueError("parameter session facts must be unique, ordered and inside the frozen range")
        if self.parameter_work.session_fact_rows != len(self.session_facts):
            raise ValueError("parameter session facts differ from the independently bound work")
        plan = minute_parameter_validation_plan(self.parameters, producer_commit=self.producer_commit)
        if plan is None:
            definition = build_minute_parameter_definition(self.parameters, producer_commit=self.producer_commit)
            expected = (definition.strategy_id, definition.strategy_version, definition.spec.spec_fingerprint,
                definition.executable_fingerprint, definition.candidate_schema_fingerprint)
        else:
            expected = (plan.native_spec.strategy_id, plan.native_spec.version, plan.native_spec.spec_fingerprint,
                plan.native_executable_fingerprint, plan.candidate_schema_fingerprint)
        if (self.strategy.strategy_id, self.strategy.strategy_version,
            self.strategy.strategy_spec_fingerprint, self.strategy.executable_fingerprint,
            self.strategy.candidate_schema_fingerprint) != expected:
            raise ValueError("parameter runtime binding differs from the full current executable")
        if len(self.model_dump_json().encode("utf-8")) > MAX_INPUT_BYTES:
            raise ValueError("complete parameter runtime exceeds the original input byte budget")
        return self

    @property
    def work(self) -> MinuteReplayWork:
        return self.parameter_work.runtime_work

    @property
    def daily_trade_dates(self) -> tuple[date, ...]:
        return tuple(day for day in self.market_calendar.open_dates if self.start_date <= day <= self.end_date)

    def freeze(self, *, audit_run_id: str, dataset_snapshot_id: str) -> FrozenMinuteParameterInput:
        return FrozenMinuteParameterInput.model_validate(self.model_dump(mode="python") | {
            "audit_run_id": audit_run_id, "dataset_snapshot_id": dataset_snapshot_id})


class FrozenMinuteParameterInput(MinuteParameterRuntimeContent):
    audit_run_id: str = Field(min_length=1, max_length=128)
    dataset_snapshot_id: Sha256

    @property
    def input_hash(self) -> str:
        return canonical_sha256(self.model_dump(mode="json"))

    @property
    def content(self) -> MinuteParameterRuntimeContent:
        return _minute_parameter_pure_content(
            self, kind="content", derive=_derive_parameter_runtime_content,
        )


class MinuteParameterRuntimeReceipt(MinuteReplayModel):
    frozen: FrozenMinuteParameterInput

    def verify(self, value: FrozenMinuteParameterInput) -> None:
        if self.frozen != value:
            raise PermissionError("parameter runtime differs from the complete independent receipt")


class MinuteParameterFormalWork(MinuteFormalWork):
    parameter_work: MinuteParameterWork

    @property
    def work_units(self) -> int:
        return self.parameter_work.work_units + self.origin_physical_rows + self.provenance_record_count

    @model_validator(mode="after")
    def complete_work(self) -> Self:
        if self.runtime_work != self.parameter_work.runtime_work:
            raise ValueError("parameter source work differs from the complete runtime bound")
        return self


class _MinuteParameterSourceBody(_MinuteSourceBody):
    contract: Literal["minute-parameter-replay-input/v1"] = PARAMETER_SOURCE_CONTRACT
    feature_registration: FeatureContractRegistration
    formal_work: MinuteParameterFormalWork

    @model_validator(mode="after")
    def complete_source(self) -> Self:
        from rquant.minute_backtest_parameter_definition import (
            build_minute_parameter_definition, build_minute_parameter_research_definition,
            minute_parameter_executable_registry,
            minute_parameter_feature_contract,
            minute_parameter_validation_plan,
        )

        _validate_minute_source_body(self, research_key=PARAMETER_RESEARCH_KEY)
        runtime = self.runtime
        if self.formal_work.parameter_work != runtime.parameter_work:
            raise ValueError("complete source omits parameter projection work")
        binding = MinuteParameterStrategyBinding.from_registration(self.native_registration,
            parameters=runtime.parameters, producer_commit=runtime.producer_commit)
        if binding != runtime.strategy:
            raise ValueError("complete parameter source native registration differs")
        plan = minute_parameter_validation_plan(runtime.parameters, producer_commit=runtime.producer_commit)
        if plan is None:
            definition = build_minute_parameter_definition(runtime.parameters, producer_commit=runtime.producer_commit)
            contract = _canonical_feature_contract(minute_parameter_feature_contract(definition))
            trusted = minute_parameter_executable_registry(definition)
            feature_bindings = trusted.feature_bindings(contract)
            wrapper = build_minute_parameter_research_definition(runtime.producer_commit)
            wrapper_expected = (_canonical_strategy_spec(wrapper.spec), wrapper.executable_fingerprint,
                wrapper.candidate_schema_fingerprint)
        else:
            contract, feature_bindings = plan.feature_contract, plan.feature_bindings
            wrapper_expected = (plan.wrapper_spec, plan.wrapper_executable_fingerprint,
                plan.wrapper_candidate_schema_fingerprint)
        feature = self.feature_registration
        if (feature.contract, feature.execution_bindings, feature.fingerprint, feature.producer_commit) != (
            contract, feature_bindings, self.native_registration.feature_contract_fingerprint,
            runtime.producer_commit):
            raise ValueError("parameter source feature registration differs from its real trusted definition")
        if feature.available_at > self.provenance.published_at:
            raise ValueError("parameter feature registration is after actual publication")
        original_hashes = {item.content_sha256: item.payload() for item in self.origin_materials}
        for fact in runtime.session_facts:
            payload = fact.source_payload()
            if hashlib.sha256(payload).hexdigest() != fact.source_snapshot_id or original_hashes.get(fact.source_snapshot_id) != payload:
                raise ValueError("parameter session fact original bytes differ from complete source evidence")
        if (self.wrapper_registration.spec, self.wrapper_registration.executable_fingerprint,
            self.wrapper_registration.candidate_schema_fingerprint,
            self.wrapper_registration.feature_contract_fingerprint) != (
            *wrapper_expected, feature.fingerprint):
            raise ValueError("parameter wrapper is not the complete original research-only definition")
        return self


class MinuteParameterSourceSeed(_MinuteParameterSourceBody):
    runtime: MinuteParameterRuntimeContent

    @property
    def seed_hash(self) -> str:
        return canonical_sha256(self.model_dump(mode="json"))

    def freeze(self, *, audit_run_id: str, dataset_snapshot_id: str) -> FrozenMinuteParameterResearchInput:
        data = self.model_dump(mode="python")
        data["runtime"] = self.runtime.freeze(audit_run_id=audit_run_id, dataset_snapshot_id=dataset_snapshot_id)
        return FrozenMinuteParameterResearchInput.model_validate(data)


class FrozenMinuteParameterResearchInput(_MinuteParameterSourceBody):
    runtime: FrozenMinuteParameterInput

    @property
    def source_content_seed(self) -> MinuteParameterSourceSeed:
        return _minute_parameter_pure_content(
            self, kind="seed", derive=_derive_parameter_source_seed,
        )

    @property
    def full_input_hash(self) -> str:
        return canonical_sha256(self.model_dump(mode="json"))

    @property
    def core_input_hash(self) -> str:
        return self.runtime.input_hash



def _derive_parameter_runtime_content(
    value: FrozenMinuteParameterInput,
) -> MinuteParameterRuntimeContent:
    return MinuteParameterRuntimeContent.model_validate(
        value.model_dump(mode="python", exclude={"audit_run_id", "dataset_snapshot_id"})
    )


def _derive_parameter_source_seed(
    value: FrozenMinuteParameterResearchInput,
) -> MinuteParameterSourceSeed:
    data = value.model_dump(mode="python")
    data["runtime"] = value.runtime.content
    return MinuteParameterSourceSeed.model_validate(data)


_ParameterGlobalPaths = tuple[tuple[str, tuple[tuple[str, ...], ...]], ...]


@dataclass(frozen=True, slots=True)
class _ParameterContentGuard:
    guards: tuple[ExecutableDependencyGuard, ...]
    models: tuple[type[BaseModel], ...]
    functions: tuple[Callable[..., object], ...]
    schema_sha256: str
    code_paths: Mapping[CodeType, _ParameterGlobalPaths] | None
    code_paths_bytes: int
    model_policy_probe: _ParameterModelPolicyProbe | None

    def assert_unchanged(self) -> None:
        for guard in self.guards:
            guard.assert_unchanged()
        if self.model_policy_probe is not None:
            if self.model_policy_probe.matches(code_paths=self.code_paths):
                return
            raise ExecutableDependencyError("minute parameter pure content schema changed")
        if _parameter_model_policy(
            self.models, self.functions, code_paths=self.code_paths
        ) != self.schema_sha256:
            raise ExecutableDependencyError("minute parameter pure content schema changed")


@dataclass(frozen=True, slots=True)
class _ParameterContentEntry:
    value: MinuteReplayModel
    payload_sha256: str
    retained_bytes: int


@dataclass(slots=True)
class _ParameterContentState:
    entries: dict[tuple[str, str], _ParameterContentEntry] = field(default_factory=dict)
    guards: dict[str, _ParameterContentGuard] = field(default_factory=dict)
    retained_bytes: int = 0
    policy_bytes: int = 0
    validation_plan_bytes: int = 0
    resolved_read_unit_bytes: int = 0
    resolved_read_unit_count: int = 0


_PARAMETER_CONTENT_STATE: ContextVar[_ParameterContentState | None] = ContextVar(
    "minute_parameter_request_pure_content", default=None
)
_MAX_PARAMETER_CONTENT_ENTRIES = 8
_MAX_PARAMETER_CONTENT_BYTES = MAX_INPUT_BYTES
_ContentInput = TypeVar("_ContentInput", bound=MinuteReplayModel)
_ContentOutput = TypeVar("_ContentOutput", bound=MinuteReplayModel)


@contextmanager
def _minute_parameter_content_scope() -> Iterator[None]:
    if _PARAMETER_CONTENT_STATE.get() is not None:
        yield
        return
    state = _ParameterContentState()
    token = _PARAMETER_CONTENT_STATE.set(state)
    try:
        yield
    finally:
        state.entries.clear()
        state.guards.clear()
        state.retained_bytes = 0
        state.policy_bytes = 0
        state.validation_plan_bytes = 0
        state.resolved_read_unit_bytes = 0
        state.resolved_read_unit_count = 0
        _PARAMETER_CONTENT_STATE.reset(token)


def _parameter_read_unit_capacity() -> int | None:
    state = _PARAMETER_CONTENT_STATE.get()
    if state is None or (
        len(state.entries) + state.resolved_read_unit_count >= _MAX_PARAMETER_CONTENT_ENTRIES
    ):
        return None
    return _MAX_PARAMETER_CONTENT_BYTES - state.retained_bytes


@dataclass(slots=True)
class _ParameterReadUnitContentEntries:
    state: _ParameterContentState
    before: dict[tuple[str, str], _ParameterContentEntry]
    closed: bool = False

    def release_new_content(self) -> None:
        if self.closed or _PARAMETER_CONTENT_STATE.get() is not self.state:
            return
        for key, entry in tuple(self.state.entries.items()):
            if key not in self.before and self.state.entries.get(key) is entry:
                del self.state.entries[key]
                self.state.retained_bytes -= entry.retained_bytes


@contextmanager
def _parameter_read_unit_content_entries(*, control_bytes: int = 0) -> Iterator[_ParameterReadUnitContentEntries | None]:
    state = _PARAMETER_CONTENT_STATE.get()
    if state is None or type(control_bytes) is not int or control_bytes < 0:
        yield None
        return
    lease = _ParameterReadUnitContentEntries(state=state, before=dict(state.entries))
    fee = control_bytes + sys.getsizeof(lease) + sys.getsizeof(lease.before)
    fee += sum(sys.getsizeof(key) + sum(sys.getsizeof(part) for part in key) for key in lease.before)
    if fee > _MAX_PARAMETER_CONTENT_BYTES - state.retained_bytes:
        yield None
        return
    state.retained_bytes += fee
    state.resolved_read_unit_bytes += fee
    try:
        yield lease
    finally:
        lease.release_new_content()
        lease.closed = True
        lease.before.clear()
        if _PARAMETER_CONTENT_STATE.get() is state:
            state.retained_bytes -= fee
            state.resolved_read_unit_bytes -= fee


@contextmanager
def _parameter_read_unit_retention(*, retained_bytes: int) -> Iterator[bool]:
    state = _PARAMETER_CONTENT_STATE.get()
    capacity = _parameter_read_unit_capacity()
    if state is None or capacity is None or retained_bytes < 0 or retained_bytes > capacity:
        yield False
        return
    state.retained_bytes += retained_bytes
    state.resolved_read_unit_bytes += retained_bytes
    state.resolved_read_unit_count += 1
    try:
        yield True
    finally:
        if _PARAMETER_CONTENT_STATE.get() is state:
            state.retained_bytes -= retained_bytes
            state.resolved_read_unit_bytes -= retained_bytes
            state.resolved_read_unit_count -= 1


def _release_parameter_validation_guard_bytes(retained_bytes: int) -> None:
    if not retained_bytes:
        return
    state = _PARAMETER_CONTENT_STATE.get()
    if state is not None:
        state.retained_bytes -= retained_bytes
        state.policy_bytes -= retained_bytes
        state.validation_plan_bytes -= retained_bytes


def _adopt_parameter_validation_guards(
    guards: tuple[ExecutableDependencyGuard, ...], *, retain: bool,
) -> tuple[tuple[ExecutableDependencyGuard, ...], int]:
    state = _PARAMETER_CONTENT_STATE.get()
    if state is None or not retain:
        return guards, 0
    adopted = []
    reserved = 0
    try:
        for guard in guards:
            current = guard.with_compiled_code_plan(
                max_retained_bytes=_MAX_PARAMETER_CONTENT_BYTES - state.retained_bytes,
            )
            fee = current.code_plan_retained_bytes
            state.retained_bytes += fee
            state.policy_bytes += fee
            state.validation_plan_bytes += fee
            reserved += fee
            adopted.append(current)
    except BaseException:
        _release_parameter_validation_guard_bytes(reserved)
        raise
    return tuple(adopted), reserved


def _parameter_parsed_global_paths(code: CodeType) -> _ParameterGlobalPaths:
    return tuple(
        (name, tuple(sorted(paths)))
        for name, paths in sorted(_referenced_global_paths(code).items())
    )


def _parameter_function_policy(
    member: FunctionType,
    *,
    code_paths: Mapping[CodeType, _ParameterGlobalPaths] | None = None,
) -> tuple[object, ...]:
    paths_used = code_paths.get(member.__code__) if code_paths is not None else None
    if paths_used is None:
        paths_used = _parameter_parsed_global_paths(member.__code__)
    globals_used = []
    for name, paths in paths_used:
        if name not in member.__globals__:
            continue
        for path in paths:
            value = member.__globals__[name]
            for attribute in path:
                value = inspect.getattr_static(value, attribute, None)
                if isinstance(value, (classmethod, staticmethod)):
                    value = value.__func__
            if isinstance(value, FunctionType):
                value = (
                    id(value), id(value.__code__),
                    repr(value.__defaults__), repr(value.__kwdefaults__),
                    _parameter_closure_policy(value),
                )
            globals_used.append((name, path, repr(value)))
    return (
        id(member),
        id(member.__code__),
        repr(member.__defaults__),
        repr(member.__kwdefaults__),
        _parameter_closure_policy(member),
        tuple(globals_used),
    )


def _parameter_closure_policy(member: FunctionType) -> tuple[object, ...]:
    seen: dict[int, int] = {}

    def content(value: object) -> object:
        if isinstance(value, (dict, tuple, list, set, frozenset, FunctionType)):
            if id(value) in seen:
                return ("ref", seen[id(value)])
            seen[id(value)] = len(seen)
        if isinstance(value, FunctionType):
            return (
                "function", id(value), id(value.__code__),
                content(value.__defaults__), content(value.__kwdefaults__), cells(value),
            )
        if type(value) is dict:
            return ("dict", tuple((repr(key), content(item)) for key, item in value.items()))
        if type(value) in (tuple, list):
            return (type(value).__name__, tuple(content(item) for item in value))
        if type(value) in (set, frozenset):
            return (type(value).__name__, tuple(sorted(
                (content(item) for item in value), key=repr
            )))
        return (type(value).__module__, type(value).__qualname__, repr(value))

    def cells(function: FunctionType) -> tuple[object, ...]:
        result = []
        for cell in function.__closure__ or ():
            try:
                result.append((id(cell), content(cell.cell_contents)))
            except ValueError as error:
                raise ExecutableDependencyError(
                    "minute parameter pure content closure unavailable"
                ) from error
        return tuple(result)

    return cells(member)


@dataclass(frozen=True, slots=True)
class _ParameterPolicyValue:
    kind: str
    value: object
    children: tuple[tuple[object, _ParameterPolicyValue], ...] = ()

    def matches(self, current: object, references: dict[int, int]) -> bool:
        if self.kind == "repr":
            return repr(current) == self.value
        if self.kind == "function":
            if current is not self.value:
                return False
            if not self.children:
                return True
            try:
                closure = tuple(cell.cell_contents for cell in current.__closure__ or ())
            except ValueError as error:
                raise ExecutableDependencyError(
                    "minute parameter pure content closure unavailable"
                ) from error
            return (
                self.children[0][1].matches(current.__defaults__, references)
                and self.children[1][1].matches(current.__kwdefaults__, references)
                and self.children[2][1].matches(closure, references)
            )
        if self.kind == "method":
            return (
                type(current) is MethodType
                and current.__func__ is self.value
                and self.children[0][1].matches(current.__self__, references)
            )
        if self.kind == "ref":
            return references.get(id(current)) == self.value
        if self.kind == "field":
            if type(current) is not FieldInfo:
                return False
            parts = tuple(current.__repr_args__())
            if tuple(name for name, _ in parts) != self.value:
                return False
            return all(
                probe.matches(value, references)
                for (_, probe), (_, value) in zip(self.children[:-1], parts, strict=True)
            ) and self.children[-1][1].matches(current.default_factory, references)
        pending: list[tuple[_ParameterPolicyValue, object, object]] = []
        probe = self
        while True:
            expected_type, keys, scalars, reference = probe.value
            if type(current) is not expected_type:
                return False
            if reference is not None:
                if id(current) in references:
                    return False
                references[id(current)] = reference
            if len(current) != len(keys):
                return False
            if probe.kind == "mapping":
                for actual, expected in zip(current, keys, strict=True):
                    if type(actual) is not type(expected) or actual != expected:
                        return False
            for key, expected in scalars:
                value = current[key]
                if type(value) is not type(expected) or value != expected:
                    return False
            children = probe.children
            if children:
                # Read a later child's value only after earlier checks finish.
                for index in range(len(children) - 1, 0, -1):
                    key, child = children[index]
                    pending.append((child, current, key))
                key, probe = children[0]
                current = current[key]
            elif pending:
                probe, parent, key = pending.pop()
                current = parent[key]
            else:
                return True
            while probe.kind not in ("mapping", "sequence"):
                if not probe.matches(current, references):
                    return False
                if not pending:
                    return True
                probe, parent, key = pending.pop()
                current = parent[key]


_ParameterModelPolicyRow = tuple[
    type[BaseModel], object, object, _ParameterPolicyValue, _ParameterPolicyValue,
    _ParameterPolicyValue, tuple[tuple[str, FunctionType], ...],
]


@dataclass(frozen=True, slots=True)
class _ParameterModelPolicyProbe:
    models: tuple[_ParameterModelPolicyRow, ...]
    functions: tuple[tuple[FunctionType, tuple[object, ...]], ...]
    representation_functions: tuple[FunctionType, ...]

    def matches(self, *, code_paths: Mapping[CodeType, _ParameterGlobalPaths] | None) -> bool:
        if _parameter_policy_representation_functions() != self.representation_functions:
            return False
        references: dict[int, int] = {}
        for model, validator, serializer, schema, fields, config, methods in self.models:
            current = sys.modules.get(model.__module__)
            for name in model.__qualname__.split("."):
                current = inspect.getattr_static(current, name, None)
            if (
                current is not model
                or model.__pydantic_validator__ is not validator
                or model.__pydantic_serializer__ is not serializer
                or _parameter_model_methods(model) != methods
                or not schema.matches(model.__pydantic_core_schema__, references)
                or not fields.matches(model.model_fields, {})
                or not config.matches(model.model_config, {})
            ):
                return False
        return all(
            _parameter_function_policy(function, code_paths=code_paths) == expected
            for function, expected in self.functions
        )


def _parameter_policy_representation_functions() -> tuple[FunctionType, ...]:
    return tuple(inspect.getattr_static(FieldInfo, name) for name in (
        "__repr__", "__repr_str__", "__repr_args__", "is_required",
    ))


def _parameter_policy_structure(
    models: tuple[type[BaseModel], ...],
    functions: tuple[Callable[..., object], ...],
) -> tuple[tuple[_ParameterModelPolicyRow, ...], tuple[FunctionType, ...], bool]:
    members = dict.fromkeys(functions)
    references: dict[int, int] = {}
    supported = True

    def capture(
        value: object, *, schema: bool = False, active: frozenset[int] = frozenset(),
    ) -> _ParameterPolicyValue:
        nonlocal supported
        if isinstance(value, FunctionType):
            members[value] = None
            if id(value) in active:
                return _ParameterPolicyValue("function", value)
            try:
                closure = tuple(cell.cell_contents for cell in value.__closure__ or ())
            except ValueError as error:
                raise ExecutableDependencyError(
                    "minute parameter pure content closure unavailable"
                ) from error
            return _ParameterPolicyValue("function", value, (
                ("defaults", capture(value.__defaults__, active=active | {id(value)})),
                ("keyword_defaults", capture(value.__kwdefaults__, active=active | {id(value)})),
                ("closure", capture(closure, active=active | {id(value)})),
            ))
        if type(value) is MethodType:
            members[value.__func__] = None
            return _ParameterPolicyValue("method", value.__func__, (
                ("owner", capture(value.__self__, active=active)),
            ))
        if type(value) is FieldInfo:
            parts = tuple(value.__repr_args__())
            return _ParameterPolicyValue("field", tuple(name for name, _ in parts), (
                *((name, capture(item, active=active)) for name, item in parts),
                ("default_factory", capture(value.default_factory, active=active)),
            ))
        if (
            schema and isinstance(value, (dict, tuple, list))
            and type(value) not in (dict, tuple, list)
        ):
            supported = False
        if schema and id(value) in references:
            return _ParameterPolicyValue("ref", references[id(value)])
        if type(value) not in (dict, tuple, list) or id(value) in active:
            return _ParameterPolicyValue("repr", repr(value))
        if type(value) is dict and any(type(key) not in (str, int, bool, bytes) for key in value):
            if schema:
                supported = False
            return _ParameterPolicyValue("repr", repr(value))
        reference = len(references) if schema else None
        if schema:
            references[id(value)] = reference
        keys = tuple(value) if type(value) is dict else tuple(range(len(value)))
        scalars = []
        children = []
        for key in keys:
            item = value[key]
            if type(item) in (str, int, bool, bytes, type(None)):
                scalars.append((key, item))
            else:
                children.append((key, capture(
                    item, schema=schema, active=active | {id(value)},
                )))
        return _ParameterPolicyValue(
            "mapping" if type(value) is dict else "sequence",
            (type(value), keys, tuple(scalars), reference), tuple(children),
        )

    rows = []
    for model in models:
        methods = _parameter_model_methods(model)
        members.update(dict.fromkeys(member for _, member in methods))
        rows.append((
            model, model.__pydantic_validator__, model.__pydantic_serializer__,
            capture(model.__pydantic_core_schema__, schema=True),
            capture(model.model_fields), capture(model.model_config), methods,
        ))
    return tuple(rows), tuple(members), supported


def _parameter_model_methods(model: type[BaseModel]) -> tuple[tuple[str, FunctionType], ...]:
    members = list(vars(model).items())
    members.extend(
        (name, inspect.getattr_static(model, name))
        for name in ("model_validate", "model_dump_json", "model_copy")
    )
    members.extend(
        (f"{name}.default_factory", value.default_factory)
        for name, value in model.model_fields.items()
    )
    methods = []
    for name, member in members:
        if isinstance(member, (staticmethod, classmethod)):
            member = member.__func__
        elif isinstance(member, property):
            member = member.fget
        if isinstance(member, FunctionType):
            methods.append((name, member))
    return tuple(methods)


def _parameter_code_paths(
    models: tuple[type[BaseModel], ...],
    functions: tuple[Callable[..., object], ...],
) -> Mapping[CodeType, _ParameterGlobalPaths]:
    members = list(functions)
    for model in models:
        members.extend(member for _, member in _parameter_model_methods(model))
    paths = {}
    for member in members:
        if member.__code__ not in paths:
            paths[member.__code__] = _parameter_parsed_global_paths(member.__code__)
    return MappingProxyType(paths)


def _parameter_code_paths_retained_bytes(
    paths: Mapping[CodeType, _ParameterGlobalPaths],
    policy_probe: _ParameterModelPolicyProbe | None = None,
) -> int:
    visited: set[int] = set()

    def retained(item: object) -> int:
        if id(item) in visited:
            return 0
        visited.add(id(item))
        size = sys.getsizeof(item)
        if isinstance(item, CodeType):
            size += sum(retained(value) for value in (
                item.co_code, item.co_consts, item.co_names, item.co_varnames,
                item.co_freevars, item.co_cellvars, item.co_filename, item.co_name,
                item.co_qualname, item.co_linetable, item.co_exceptiontable,
            ))
        elif isinstance(item, Mapping):
            if isinstance(item, MappingProxyType):
                size += sys.getsizeof(dict(item))
            size += sum(retained(key) + retained(value) for key, value in item.items())
        elif isinstance(item, (tuple, frozenset)):
            size += sum(retained(value) for value in item)
        elif isinstance(item, (_ParameterPolicyValue, _ParameterModelPolicyProbe)):
            size += sum(retained(getattr(item, name)) for name in item.__slots__)
        return size

    return retained(paths) + retained(policy_probe)


def _parameter_model_policy(
    models: tuple[type[BaseModel], ...],
    functions: tuple[Callable[..., object], ...],
    *,
    code_paths: Mapping[CodeType, _ParameterGlobalPaths] | None = None,
) -> str:
    digest = hashlib.sha256()
    seen: dict[int, int] = {}

    def schema(item: object) -> None:
        if isinstance(item, (dict, tuple, list)):
            if id(item) in seen:
                digest.update(f"ref:{seen[id(item)]}".encode("ascii"))
                return
            seen[id(item)] = len(seen)
            digest.update(type(item).__name__.encode("ascii"))
            if isinstance(item, dict):
                for key, value in item.items():
                    digest.update(repr(key).encode("utf-8"))
                    schema(value)
            else:
                for value in item:
                    schema(value)
        else:
            digest.update(repr(item).encode("utf-8"))

    for model in models:
        module = sys.modules.get(model.__module__)
        current = module
        for name in model.__qualname__.split("."):
            current = inspect.getattr_static(current, name, None)
        if current is not model:
            raise ExecutableDependencyError("minute parameter pure content model binding changed")
        methods = [
            (name, _parameter_function_policy(member, code_paths=code_paths))
            for name, member in _parameter_model_methods(model)
        ]
        schema(model.__pydantic_core_schema__)
        policy = (
            id(model.__pydantic_validator__),
            id(model.__pydantic_serializer__),
            model.model_fields,
            model.model_config,
            methods,
        )
        digest.update(repr(policy).encode("utf-8"))
    for function in functions:
        digest.update(repr(
            _parameter_function_policy(function, code_paths=code_paths)
        ).encode("utf-8"))
    return digest.hexdigest()


def _parameter_content_guard(
    value: MinuteReplayModel,
    *,
    output_model: type[BaseModel],
    derive: Callable[..., MinuteReplayModel],
    remaining_bytes: int,
) -> _ParameterContentGuard:
    models: set[type[BaseModel]] = {type(value), output_model}
    functions: set[FunctionType] = set()
    visited: set[int] = set()

    def visit(item: object) -> None:
        if id(item) in visited:
            return
        visited.add(id(item))
        if isinstance(item, dict):
            for nested in item.values():
                visit(nested)
        elif isinstance(item, (tuple, list)):
            for nested in item:
                visit(nested)
        elif isinstance(item, type) and issubclass(item, BaseModel):
            models.add(item)
            visit(item.__pydantic_core_schema__)
        else:
            function = item.__func__ if inspect.ismethod(item) else item
            if isinstance(function, FunctionType) and function.__module__.startswith("rquant."):
                functions.add(function)

    visit(type(value).__pydantic_core_schema__)
    visit(output_model.__pydantic_core_schema__)
    # Property bodies are checked in the model policy. Their actual callable
    # globals are also covered by the original dependency guard.
    for model in models:
        for member in vars(model).values():
            if isinstance(member, property) and member.fget is not None:
                for name in member.fget.__code__.co_names:
                    dependency = member.fget.__globals__.get(name)
                    if isinstance(dependency, FunctionType) and dependency.__module__.startswith(
                        "rquant."
                    ):
                        functions.add(dependency)
    roots = (
        derive,
        _copy_parameter_content,
        _parameter_content_retained_bytes,
        _minute_parameter_pure_content,
        _parameter_model_policy,
        _parameter_function_policy,
        _parameter_closure_policy,
        _ParameterPolicyValue.matches,
        _ParameterModelPolicyProbe.matches,
        _parameter_policy_representation_functions,
        _parameter_policy_structure,
        _parameter_model_methods,
        _parameter_parsed_global_paths,
        _parameter_code_paths,
        _parameter_code_paths_retained_bytes,
        _parameter_content_guard,
        _adopt_parameter_validation_guards,
        _release_parameter_validation_guard_bytes,
        _validate_minute_replay_archive,
        _validate_minute_source_body,
        *_parameter_policy_representation_functions(),
        *functions,
    )
    bindings = []
    for root in roots:
        if "<" in root.__qualname__:
            continue
        # Bind each actual dependency below; unrelated first-time imports must
        # not turn the interpreter's entire module directory into schema input.
        module = sys.modules.get(root.__module__)
        if module is None:
            raise ExecutableDependencyError("minute parameter pure content module unavailable")
        member = module
        for name in root.__qualname__.split("."):
            member = inspect.getattr_static(member, name, None)
        if isinstance(member, property):
            continue
        bindings.append(ExecutableBinding.from_callable(root))
    # Pydantic compiled schema/default-factory state is checked separately:
    # its internal opaque sentinels are not Python executable roots.
    code_guard = capture_executable_dependency_guard(
        tuple(bindings),
        contract="minute-parameter-pure-content-code/v1",
        include_global_dependencies=False,
    )
    dependency_guard = capture_executable_dependency_guard(
        tuple(
            ExecutableBinding.from_callable(root)
            for root in (
                _validate_minute_replay_archive,
                _validate_minute_source_body,
                canonical_sha256,
            )
        ),
        contract="minute-parameter-pure-content-dependencies/v1",
    )
    ordered_models = tuple(sorted(models, key=lambda model: (model.__module__, model.__qualname__)))
    ordered_functions = tuple(
        sorted(roots, key=lambda function: (function.__module__, function.__qualname__))
    )
    # The plan holds immutable comparison data. Every check reads the current
    # schema, FieldInfo representation, functions and mutable closure contents.
    model_rows, policy_functions, supported = _parameter_policy_structure(
        ordered_models, ordered_functions
    )
    code_paths = _parameter_code_paths(ordered_models, policy_functions)
    model_policy_probe = _ParameterModelPolicyProbe(
        models=model_rows,
        functions=tuple((function, _parameter_function_policy(
            function, code_paths=code_paths,
        )) for function in policy_functions),
        representation_functions=_parameter_policy_representation_functions(),
    )
    if not supported:
        model_policy_probe = None
    policy_bytes = _parameter_code_paths_retained_bytes(code_paths, model_policy_probe)
    if policy_bytes > remaining_bytes:
        code_paths = None
        model_policy_probe = None
        policy_bytes = 0
    code_guard = code_guard.with_compiled_code_plan(
        max_retained_bytes=remaining_bytes - policy_bytes,
    )
    policy_bytes += code_guard.code_plan_retained_bytes
    dependency_guard = dependency_guard.with_compiled_code_plan(
        max_retained_bytes=remaining_bytes - policy_bytes,
    )
    policy_bytes += dependency_guard.code_plan_retained_bytes
    return _ParameterContentGuard(
        guards=(code_guard, dependency_guard),
        models=ordered_models,
        functions=ordered_functions,
        schema_sha256=_parameter_model_policy(
            ordered_models, ordered_functions, code_paths=code_paths
        ),
        code_paths=code_paths,
        code_paths_bytes=policy_bytes,
        model_policy_probe=model_policy_probe,
    )


def _minute_parameter_pure_content(
    value: _ContentInput,
    *,
    kind: Literal["content", "seed"],
    derive: Callable[[_ContentInput], _ContentOutput],
) -> _ContentOutput:
    state = _PARAMETER_CONTENT_STATE.get()
    input_model = (
        FrozenMinuteParameterInput if kind == "content" else FrozenMinuteParameterResearchInput
    )
    if state is None or type(value) is not input_model:
        return derive(value)
    output_model = MinuteParameterRuntimeContent if kind == "content" else MinuteParameterSourceSeed
    guard = state.guards.get(kind)
    if guard is None:
        guard = _parameter_content_guard(
            value, output_model=output_model, derive=derive,
            remaining_bytes=_MAX_PARAMETER_CONTENT_BYTES - state.retained_bytes,
        )
        state.guards[kind] = guard
        state.policy_bytes += guard.code_paths_bytes
        state.retained_bytes += guard.code_paths_bytes
    guard.assert_unchanged()
    # Include every actual frozen field and archive payload, not a claimed hash,
    # source identity, object identity or physical path.
    input_digest = hashlib.sha256()
    input_digest.update(guard.schema_sha256.encode("ascii"))
    for dependency in guard.guards:
        input_digest.update(dependency.fingerprint.encode("ascii"))
    input_digest.update(value.model_dump_json(exclude_computed_fields=True).encode("utf-8"))
    key = (kind, input_digest.hexdigest())
    entry = state.entries.get(key)
    if entry is None:
        result = derive(value)
        guard.assert_unchanged()
        payload = result.model_dump_json(exclude_computed_fields=True).encode("utf-8")
        # Include actual retained model data and indexing/digest overhead. Shared
        # objects may be charged twice across entries; never undercount them.
        retained_bytes = _parameter_content_retained_bytes(
            result, key=key, payload_bytes=len(payload)
        )
        entry = _ParameterContentEntry(
            value=result,
            payload_sha256=hashlib.sha256(payload).hexdigest(),
            retained_bytes=retained_bytes,
        )
        if (
            len(state.entries) + state.resolved_read_unit_count < _MAX_PARAMETER_CONTENT_ENTRIES
            and state.retained_bytes + retained_bytes <= _MAX_PARAMETER_CONTENT_BYTES
        ):
            state.entries[key] = entry
            state.retained_bytes += retained_bytes
    else:
        from rquant.minute_backtest_parameter_definition import minute_parameter_validation_plan

        runtime = value.runtime if isinstance(value, FrozenMinuteParameterResearchInput) else value
        minute_parameter_validation_plan(
            runtime.parameters, producer_commit=runtime.producer_commit
        )
    if (
        hashlib.sha256(
            entry.value.model_dump_json(exclude_computed_fields=True).encode("utf-8")
        ).hexdigest()
        != entry.payload_sha256
    ):
        raise ExecutableDependencyError("minute parameter pure content payload changed")
    guard.assert_unchanged()
    if type(entry.value) is not output_model:
        raise ExecutableDependencyError("minute parameter pure content model changed")
    result = _copy_parameter_content(entry.value)
    guard.assert_unchanged()
    if (
        hashlib.sha256(
            result.model_dump_json(exclude_computed_fields=True).encode("utf-8")
        ).hexdigest()
        != entry.payload_sha256
    ):
        raise ExecutableDependencyError("minute parameter pure content copy changed")
    return result


def _copy_parameter_content(value: _ContentOutput) -> _ContentOutput:
    # Original spec/feature maps use mappingproxy. Give each returned model its
    # own read-only maps without re-running archive validation or sharing state.
    memo: dict[int, object] = {}
    proxies: list[tuple[Mapping[str, object], dict[str, object]]] = []
    visited: set[int] = set()

    def visit(item: object) -> None:
        if id(item) in visited:
            return
        visited.add(id(item))
        if isinstance(item, MappingProxyType):
            copied: dict[str, object] = {}
            memo[id(item)] = MappingProxyType(copied)
            proxies.append((item, copied))
        if isinstance(item, BaseModel):
            visit(item.__dict__)
        elif isinstance(item, Mapping):
            for nested in item.values():
                visit(nested)
        elif isinstance(item, (tuple, list, set, frozenset)):
            for nested in item:
                visit(nested)

    visit(value)
    for original, copied in proxies:
        copied.update({key: deepcopy(item, memo) for key, item in original.items()})
    return deepcopy(value, memo)


def _parameter_content_retained_bytes(
    value: MinuteReplayModel,
    *,
    key: tuple[str, str],
    payload_bytes: int,
) -> int:
    visited: set[int] = set()

    def retained(item: object) -> int:
        if id(item) in visited:
            return 0
        visited.add(id(item))
        size = sys.getsizeof(item)
        if isinstance(item, BaseModel):
            size += sum(
                retained(nested)
                for nested in (
                    item.__dict__,
                    item.__pydantic_fields_set__,
                    item.__pydantic_extra__,
                    item.__pydantic_private__,
                )
            )
        elif isinstance(item, Mapping):
            if isinstance(item, MappingProxyType):
                size += sys.getsizeof(dict(item))
            size += sum(retained(k) + retained(v) for k, v in item.items())
        elif isinstance(item, (tuple, list, set, frozenset)):
            size += sum(retained(nested) for nested in item)
        return size

    data_bytes = max(payload_bytes, retained(value))
    overhead = (
        sys.getsizeof(key)
        + sum(sys.getsizeof(part) for part in key)
        + sys.getsizeof("0" * 64)
        + sys.getsizeof({key: None})
        + sys.getsizeof(
            _ParameterContentEntry(value=value, payload_sha256="0" * 64, retained_bytes=0)
        )
    )
    return data_bytes + overhead
