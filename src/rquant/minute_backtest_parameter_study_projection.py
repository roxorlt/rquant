"""Authenticated, bounded derivations of original indexed minute study results."""

from __future__ import annotations

import fcntl
import ast
import hashlib
import hmac
import inspect
import os
import operator
import sqlite3
import stat
import sys
from dataclasses import fields, is_dataclass
from enum import Enum
from types import CodeType, FunctionType, GenericAlias, MappingProxyType, MethodType, ModuleType
from collections.abc import Iterator
from contextlib import contextmanager, closing
from contextvars import ContextVar
from threading import get_ident
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Literal, Self
from uuid import UUID, uuid4

from pydantic import BaseModel, Field, SerializationInfo, SerializerFunctionWrapHandler, model_serializer, model_validator
from pydantic.fields import FieldInfo
from pydantic_core import PydanticUndefined, SchemaSerializer, SchemaValidator

from rquant.executable_dependencies import ExecutableDependencyError, fingerprint_dependency_value
from rquant.lab_artifact_preview import ArtifactCompleteByteEvidence, ArtifactCompleteTableBudget, ArtifactPreviewReader
from rquant.lab_jobs import LabArtifactPreviewAuthority
from rquant.minute_backtest_parameter_definition import minute_parameter_validation_request
from rquant.minute_backtest_contracts import (
    MAX_RESULT_TOTAL_BYTES, MAX_RESULT_WIRE_BYTES, MinuteReplayModel, Sha256,
)
from rquant.minute_backtest_parameter_study_execution import MinuteParameterStudyWindowObservation
from rquant.minute_backtest_parameter_adapter import MinuteParameterFormalParameters
from rquant.minute_backtest_parameter_producer import (
    MinuteParameterPreparedPublication, MinuteParameterFactSourceReference,
    MinuteParameterPublicationReceipt, MinuteParameterReplayCatalog,
    _MinuteStudyInputPath, _MinuteStudyInputFileEvidence,
)
from rquant.minute_backtest_contracts import MinuteReplayResultBudget, MAX_WORK_UNITS
from rquant.definition_registry import FeatureContractRegistration, StrategySpecRegistration
from rquant.research_snapshot import DatasetSnapshotBinding
from rquant.minute_backtest_producer import MinutePrivateFileReference, _secure_private_bytes
from rquant.minute_backtest_publication_contracts import MAX_MINUTE_CONTROL_BYTES
from rquant.runtime_contracts import AwareUtcDatetime, canonical_sha256
from rquant.strict_json import canonical_json_bytes, strict_model_validate_json

if TYPE_CHECKING:
    from rquant.minute_backtest_installation import InstalledMinuteReplay
    from rquant.minute_backtest_parameter_study_execution import MinuteParameterPreparedStudyTrial, MinuteParameterStudyTrialResult

_DOMAIN = b"minute-study-projection/v1\0"
_ENTRY_LIMIT = 16_384
_ACTIVE_MINUTE_INPUT_READ: ContextVar[_MinuteStudyVerifiedInputRead | None] = ContextVar(
    "minute-study-authenticated-input-read", default=None)


class _MinuteStudyAcceptedShard(MinuteReplayModel):
    shard_id: UUID
    shard_index: int = Field(ge=0)
    plan_hash: Sha256
    payload_hash: Sha256
    payload_sha256: Sha256
    adapter_id: str = Field(min_length=1)
    adapter_version: str = Field(min_length=1)
    work_units: int = Field(ge=1, le=MAX_WORK_UNITS)


class _MinuteStudyInputVerification(MinuteReplayModel):
    contract: Literal["minute-study-input-verification/v1"] = "minute-study-input-verification/v1"
    prepared: MinuteParameterPreparedPublication
    baseline: MinuteParameterFactSourceReference
    prepared_binding: DatasetSnapshotBinding
    baseline_binding: DatasetSnapshotBinding
    catalog_sha256: Sha256
    files: tuple[_MinuteStudyInputFileEvidence, ...] = Field(min_length=1)
    parameters: MinuteParameterFormalParameters
    native_registration: StrategySpecRegistration
    parameter_registration: StrategySpecRegistration
    feature_registrations: tuple[FeatureContractRegistration, ...] = Field(min_length=1)
    expected_publication_hash: Sha256
    result_budget: MinuteReplayResultBudget
    accepted_shard: _MinuteStudyAcceptedShard
    input_semantic_fingerprint: Sha256

    @model_serializer(mode="wrap")
    def original_binding_fields(self, handler: SerializerFunctionWrapHandler,
        info: SerializationInfo) -> dict[str, object]:
        values = handler(self)
        for name in ("prepared_binding", "baseline_binding"):
            values[name] = getattr(self, name).model_dump(mode=info.mode, exclude_computed_fields=True)
        return values


class MinuteStudyProjectionBinding(MinuteReplayModel):
    owner_id: str = Field(min_length=1, max_length=128)
    job_id: UUID
    shard_id: UUID
    spec_hash: Sha256
    plan_hash: Sha256
    payload_hash: Sha256
    manifest_hash: Sha256
    complete_result_hash: Sha256
    result_hash: Sha256
    full_input_hash: Sha256
    core_input_hash: Sha256
    seed_hash: Sha256
    profile_hash: Sha256
    parameter_hash: Sha256
    study_binding_hash: Sha256
    publication_hash: Sha256
    formal_plan_id: Sha256
    completed_at: AwareUtcDatetime


class MinuteStudyProjectionCertificate(MinuteReplayModel):
    contract: Literal["minute-study-projection-certificate/v1"] = "minute-study-projection-certificate/v1"
    binding: MinuteStudyProjectionBinding
    training: MinuteParameterStudyWindowObservation
    validation: MinuteParameterStudyWindowObservation
    independent_test: MinuteParameterStudyWindowObservation
    original_table_bytes: int = Field(ge=0, le=MAX_RESULT_TOTAL_BYTES)
    complete_wire_bytes: int = Field(ge=0, le=MAX_RESULT_WIRE_BYTES)
    algorithm_fingerprint: Sha256
    original_byte_fingerprint: Sha256 | None = None
    complete_semantic_fingerprint: Sha256 | None = None
    input_verification: _MinuteStudyInputVerification | None = None

    @model_validator(mode="after")
    def original_window_identity(self) -> Self:
        for window in (self.training, self.validation, self.independent_test):
            if (window.full_input_hash, window.parameter_hash, window.profile_hash) != (
                self.binding.full_input_hash, self.binding.parameter_hash, self.binding.profile_hash):
                raise ValueError("derived window differs from its complete original identity")
        return self


class MinuteStudyProjectionDirectory(MinuteReplayModel):
    path: Path
    device: int = Field(ge=0)
    inode: int = Field(ge=1)
    owner_uid: int = Field(ge=0)


class MinuteStudyProjectionAuthority(MinuteReplayModel):
    contract: Literal["minute-study-projection-authority/v1"] = "minute-study-projection-authority/v1"
    installation_reference: MinutePrivateFileReference
    code_sha: str = Field(pattern=r"^[0-9a-f]{40}$")
    deployment_profile_id: Sha256
    deployment_generation_hash: Sha256
    authority_manifest_hash: Sha256
    state_directory: MinuteStudyProjectionDirectory
    payload_directory: MinuteStudyProjectionDirectory
    index_device: int = Field(ge=0)
    index_inode: int = Field(ge=1)
    key_reference: MinutePrivateFileReference
    algorithm_fingerprint: Sha256


class MinuteStudyProjectionPayloadIdentity(MinuteReplayModel):
    device: int
    inode: int
    size: int = Field(ge=1, le=MAX_MINUTE_CONTROL_BYTES)
    mtime_ns: int
    ctime_ns: int
    owner_uid: int
    mode: Literal[256] = 0o400
    link_count: Literal[1] = 1


class MinuteStudyProjectionEntry(MinuteReplayModel):
    contract: Literal["minute-study-projection-entry/v1"] = "minute-study-projection-entry/v1"
    authority_hash: Sha256
    binding: MinuteStudyProjectionBinding
    algorithm_fingerprint: Sha256
    payload_sha256: Sha256
    payload_identity: MinuteStudyProjectionPayloadIdentity
    revision: Literal[1] = 1


class MinuteStudyProjectionReconcileResult(MinuteReplayModel):
    sequence: int = Field(ge=0)
    candidate_job_id: UUID | None
    published: bool
    pending_jobs: int = Field(ge=0, le=8)


def _portable_projection_schema(schema: object) -> object:
    labels: dict[str, str] = {}
    seen: set[int] = set()
    nodes = 0

    def collect(value: object, depth: int = 0) -> None:
        nonlocal nodes
        nodes += 1
        if nodes > 8192 or depth > 64:
            raise ExecutableDependencyError("minute projection schema node/depth limit")
        if type(value) not in (dict, list, tuple) or id(value) in seen:
            return
        seen.add(id(value))
        if type(value) is dict:
            owner = value
            for _ in range(65):
                if type(owner) is not dict or owner.get("type") not in ("function-after", "function-before", "function-wrap"):
                    break
                owner = owner.get("schema")
            else:
                raise ExecutableDependencyError("minute projection schema wrapper depth limit")
            model = owner.get("cls") if type(owner) is dict else None
            if type(owner) is dict and owner.get("type") == "model" and isinstance(model, type) and issubclass(model, BaseModel):
                current: object = sys.modules.get(model.__module__)
                for name in model.__qualname__.split("."):
                    current = inspect.getattr_static(current, name, None)
                if current is not model:
                    raise ExecutableDependencyError("minute projection schema model binding changed")
                reference = value.get("ref")
                if isinstance(reference, str) and reference.rpartition(":")[2] == str(id(model)):
                    labels[reference] = reference.rpartition(":")[0]
            for key, item in value.items():
                collect(key, depth + 1)
                collect(item, depth + 1)
        else:
            for item in value:
                collect(item, depth + 1)

    collect(schema)
    memo: dict[int, object] = {}
    active_tuples: set[int] = set()

    def clone(value: object) -> object:
        if type(value) not in (dict, list, tuple):
            return value
        identity = id(value)
        if identity in memo:
            return memo[identity]
        if type(value) is dict:
            result: dict[object, object] = {}
            memo[identity] = result
            for key, item in value.items():
                if key in ("ref", "schema_ref") and isinstance(item, str):
                    item = labels.get(item, item)
                result[key] = clone(item)
            return result
        if type(value) is list:
            items: list[object] = []
            memo[identity] = items
            items.extend(clone(item) for item in value)
            return items
        if identity in active_tuples:
            raise ExecutableDependencyError("minute projection schema tuple cycle")
        active_tuples.add(identity)
        try:
            result_tuple = tuple(clone(item) for item in value)
            memo[identity] = result_tuple
            return result_tuple
        finally:
            active_tuples.remove(identity)

    return clone(schema)


def projection_algorithm_fingerprint() -> str:
    """Read actual schema contents; this value is never retained as a live guard."""
    model = MinuteParameterStudyWindowObservation
    module = sys.modules.get(model.__module__)
    current: object = module
    for name in model.__qualname__.split("."):
        current = inspect.getattr_static(current, name, None)
    if current is not model:
        raise PermissionError("minute derived window model binding changed")
    try:
        return fingerprint_dependency_value((_portable_projection_schema(model.__pydantic_core_schema__), model.model_config),
            contract="minute-study-projection-semantics/v1")
    except ExecutableDependencyError as exc:
        raise PermissionError("minute projection live semantics cannot be proven") from exc


class _ProjectionSemanticSnapshot:
    """Ephemeral content traversal, with the original pure-guard limits and aliases."""

    def __init__(self) -> None:
        self.references: dict[int, int] = {}
        self.reference_objects: list[object] = []
        self.nodes = 0
        self.models: set[type[BaseModel]] = set()
        self.functions: dict[FunctionType, None] = {}
        self.schemas: dict[int, tuple[dict[str, object], int]] = {}

    def part(self, value: object, *, globals: bool = True) -> object:
        # Each original pure guard has its own finite traversal budget. Keep the
        # alias ledger across these explicit roots, never split an oversized root.
        self.nodes = 0
        return self.value(value, globals=globals)

    @staticmethod
    def digest(value: object) -> str:
        encoded = canonical_json_bytes(value)
        if len(encoded) > MAX_MINUTE_CONTROL_BYTES:
            raise ExecutableDependencyError("minute projection live semantic byte limit")
        return hashlib.sha256(encoded).hexdigest()

    def value(self, value: object, depth: int = 0, *, globals: bool = True) -> object:
        self.nodes += 1
        if self.nodes > 8192 or depth > 64:
            raise ExecutableDependencyError("minute projection live semantic node/depth limit")
        if value is PydanticUndefined:
            return {"undefined": True}
        if value is Ellipsis or value is NotImplemented:
            return {"singleton": type(value).__name__}
        if value is None or type(value) in (bool, int, str):
            return value
        if type(value) in (float, bytes):
            return {type(value).__name__: fingerprint_dependency_value(value, contract="minute-study-policy-value/v1")}
        if isinstance(value, (staticmethod, classmethod)):
            return {type(value).__name__: self.value(value.__func__, depth + 1, globals=globals)}
        if isinstance(value, MethodType):
            return {"bound_method": (self.value(value.__self__, depth + 1), self.value(value.__func__, depth + 1, globals=globals))}
        if isinstance(value, Enum):
            return {"enum": (type(value).__module__, type(value).__qualname__, value.name, self.value(value.value, depth + 1))}
        if isinstance(value, ModuleType):
            if sys.modules.get(value.__name__) is not value:
                raise ExecutableDependencyError("minute projection semantic module binding changed")
            return {"module": value.__name__}
        if isinstance(value, type):
            current: object = sys.modules.get(value.__module__)
            for part in value.__qualname__.split("."):
                current = inspect.getattr_static(current, part, None)
            if current is not value and value not in (MappingProxyType, FunctionType, MethodType, ModuleType,
                CodeType, type(None), type(Ellipsis), type(NotImplemented)):
                raise ExecutableDependencyError(f"minute projection semantic type binding changed: {value.__module__}.{value.__qualname__}")
            if issubclass(value, BaseModel) and value is not BaseModel:
                self.models.add(value)
            return {"type": (value.__module__, value.__qualname__)}
        if type(value).__module__ == "typing" or type(value) is GenericAlias or type(value).__module__ == "types" and type(value).__name__ == "UnionType":
            from typing import get_args, get_origin

            origin, arguments = get_origin(value), get_args(value)
            captured_origin = self.value(origin, depth + 1, globals=globals)
            # Typing interns its annotation/argument wrappers according to import
            # order. The original policy reads their representation, not that
            # incidental sharing. Keep every live argument and the tuple budget;
            # nested mutable objects still use the ordinary alias ledger below.
            self.nodes += 1
            if self.nodes > 8192 or depth + 1 > 64:
                raise ExecutableDependencyError("minute projection live semantic node/depth limit")
            return {"annotation": (repr(value) if origin is None else None, captured_origin,
                {"tuple": [self.value(item, depth + 2, globals=globals) for item in arguments]})}
        if isinstance(value, FunctionType) and globals:
            self.functions[value] = None
        identity = id(value)
        if identity in self.references:
            return {"reference": self.references[identity]}
        self.references[identity] = len(self.references)
        self.reference_objects.append(value)
        if type(value) is object:
            # Exact object sentinels have no mutable content; their sharing with
            # defaults/global bindings remains in the ordinary alias ledger.
            return {"object_sentinel": True}
        if isinstance(value, CodeType):
            return {"code": self.value(tuple(getattr(value, name) for name in (
                "co_argcount", "co_posonlyargcount", "co_kwonlyargcount", "co_nlocals", "co_stacksize", "co_flags",
                "co_code", "co_consts", "co_names", "co_varnames", "co_freevars", "co_cellvars", "co_filename",
                "co_name", "co_qualname", "co_firstlineno", "co_linetable", "co_exceptiontable")), depth + 1)}
        if isinstance(value, FunctionType):
            closure = tuple(cell.cell_contents for cell in value.__closure__ or ())
            code = canonical_sha256(self.value(value.__code__, depth + 1, globals=False))
            code_identity = id(value.__code__)
            if code_identity not in self.references:
                self.references[code_identity] = len(self.references)
            return {"function": (value.__module__, value.__qualname__, code, self.references[code_identity],
                self.value(value.__defaults__, depth + 1, globals=globals), self.value(value.__kwdefaults__, depth + 1, globals=globals),
                self.value(closure, depth + 1, globals=globals), self.value(vars(value), depth + 1, globals=globals))}
        if type(value) is dict:
            if value.get("type") == "model" and isinstance(value.get("cls"), type) and issubclass(value["cls"], BaseModel):
                # A model schema is a distinct original validation unit. Its
                # embedded bytes are checked, even if its class schema differs.
                self.schemas[identity] = value, depth
                return {"model_schema": self.references[identity]}
            return self.mapping(value, depth, globals=globals)
        if type(value) in (list, tuple):
            return {type(value).__name__: [self.value(item, depth + 1, globals=globals) for item in value]}
        if isinstance(value, FieldInfo):
            return {"field": self.value(value.asdict(), depth + 1, globals=globals)}
        if isinstance(value, property):
            return {"property": (self.value(value.fget, depth + 1), self.value(value.fset, depth + 1), self.value(value.fdel, depth + 1))}
        from pydantic._internal._fields import _general_metadata_cls
        from pydantic._internal._utils import deprecated_instance_property
        if type(value) is _general_metadata_cls() or type(value) is deprecated_instance_property:
            return {"pydantic_metadata": (type(value).__module__, type(value).__qualname__,
                self.value(vars(value), depth + 1, globals=globals),
                tuple((name, self.value(member, depth + 1, globals=globals))
                    for name, member in vars(type(value)).items() if isinstance(member, FunctionType)))}
        if type(value) in (operator.itemgetter, operator.attrgetter, operator.methodcaller):
            return {"operator": (type(value).__qualname__, self.value(value.__reduce__()[1], depth + 1, globals=globals))}
        if is_dataclass(value):
            return {"dataclass": (type(value).__module__, type(value).__qualname__,
                tuple((field.name, self.value(getattr(value, field.name), depth + 1)) for field in fields(value)))}
        try:
            return {"pure_value": fingerprint_dependency_value(value, contract="minute-study-policy-value/v1")}
        except ExecutableDependencyError as exc:
            raise ExecutableDependencyError(f"minute projection opaque value {type(value).__module__}.{type(value).__qualname__}: {exc}") from exc

    def mapping(self, value: dict[object, object], depth: int, *, globals: bool = True) -> object:
            entries = []
            for key, item in value.items():
                # Pydantic embeds process addresses in schema reference labels;
                # actual model bindings and alias structure are checked separately.
                if key in ("ref", "schema_ref") and isinstance(item, str) and item.rpartition(":")[2].isdigit():
                    item = item.rpartition(":")[0]
                entries.append((self.value(key, depth + 1, globals=globals), self.value(item, depth + 1, globals=globals)))
            return {"dict": entries}

    def model(self, model: type[BaseModel]) -> object:
        from rquant.minute_backtest_parameter_contracts import _parameter_model_methods
        self.part(model)
        validator, serializer = model.__pydantic_validator__, model.__pydantic_serializer__
        if type(validator) is not SchemaValidator or type(serializer) is not SchemaSerializer:
            raise ExecutableDependencyError(f"minute projection opaque validation/serialization engine: {model.__module__}.{model.__qualname__}")
        return (model.__module__, model.__qualname__, self.digest(self.part(model.__pydantic_core_schema__)),
            self.digest(self.part(model.model_config)), self.digest(self.part(model.model_fields)),
            self.engine(validator, model), self.engine(serializer, model),
            tuple((name, self.digest(self.part(function))) for name, function in _parameter_model_methods(model)))

    def engine(self, engine: SchemaValidator | SchemaSerializer, model: type[BaseModel]) -> str:
        args = engine.__reduce__()[1]
        if len(args) != 3 or args[0] is not model.__pydantic_core_schema__ or type(args[1]) is not dict or any(type(key) is not str for key in args[1]):
            raise ExecutableDependencyError("minute projection native engine schema/config is opaque")
        self.nodes = 0
        # Rust exports its option map in unspecified hash-map order. Preserve
        # every option/value; the actual Python model/schema maps keep their order.
        return self.digest((self.value(args[0]), tuple((key, self.value(args[1][key])) for key in sorted(args[1])), self.value(args[2])))

    def function_bindings(self, function: FunctionType) -> object:
        from rquant.minute_backtest_parameter_contracts import _parameter_parsed_global_paths

        dependencies = []
        for name, paths in _parameter_parsed_global_paths(function.__code__):
            if name not in function.__globals__:
                continue
            for path in paths:
                endpoint = function.__globals__[name]
                for part in path:
                    endpoint = inspect.getattr_static(endpoint, part)
                    if isinstance(endpoint, (staticmethod, classmethod)):
                        endpoint = endpoint.__func__
                recursive = isinstance(endpoint, FunctionType) and endpoint.__module__ in (
                    'rquant.minute_backtest_performance', 'rquant.perf.core', 'rquant.perf.trades')
                dependencies.append((name, path, self.value(endpoint, globals=recursive)))
        return self.references[id(function)], dependencies


def _projection_semantic_components(*, detailed: bool = False) -> tuple[tuple[str, str], ...]:
    from rquant import minute_backtest_parameter_artifact as artifact
    from rquant import minute_backtest_parameter_runner as runner
    from rquant import minute_backtest_parameter_study_execution as execution
    from rquant import minute_backtest_performance as performance
    from rquant import strategy_compare
    from rquant.minute_backtest_parameter_adapter import MinuteParameterFormalReplayResult

    snapshot = _ProjectionSemanticSnapshot()
    parts = []
    for item in (
        artifact.MinuteParameterSealedReplayReader._complete_result,
        artifact.MinuteParameterSealedReplayReader._formal_result_model,
        artifact.MinuteParameterSealedReplayReader._sealed_result_model,
        artifact.MinuteParameterSealedReplayReader._result_tables,
        runner.minute_parameter_result_tables,
        execution.project_minute_parameter_study_sealed_windows,
        execution.project_minute_parameter_study_window,
        execution._closed_sell_bindings, execution._original_exit_reason, execution._window_contains,
        performance.build_minute_performance, strategy_compare._summary_row,
    ):
        parts.append(('root:' + item.__module__ + '.' + item.__qualname__, snapshot.digest(snapshot.part(item))))
    snapshot.models.update((MinuteParameterFormalReplayResult, runner.MinuteParameterReplayResult,
        artifact.MinuteParameterSealedReplayResult, execution.MinuteParameterStudyWindowObservation))
    processed: set[type[BaseModel]] = set()
    processed_schemas: set[int] = set()
    processed_functions: set[FunctionType] = set()
    for_models = lambda: snapshot.models - processed
    while for_models() or snapshot.schemas.keys() - processed_schemas or snapshot.functions.keys() - processed_functions:
        for model in sorted(for_models(), key=lambda item: (item.__module__, item.__qualname__)):
            label = 'model:' + model.__module__ + '.' + model.__qualname__
            captured = snapshot.model(model)
            parts.append((label, snapshot.digest(captured)))
            if detailed:
                parts.extend((label + '.' + name, value) for name, value in zip(
                    ('schema', 'config', 'fields', 'validator', 'serializer'), captured[2:7]))
                parts.extend((label + '.method:' + name, value) for name, value in captured[7])
            processed.add(model)
        for identity in tuple(key for key in snapshot.schemas if key not in processed_schemas):
            schema, depth = snapshot.schemas[identity]
            snapshot.nodes = 0
            label = 'schema:' + schema['cls'].__module__ + '.' + schema['cls'].__qualname__ + ':' + str(snapshot.references[identity])
            parts.append((label, snapshot.digest(snapshot.mapping(schema, depth))))
            processed_schemas.add(identity)
        for function in tuple(function for function in snapshot.functions if function not in processed_functions):
            snapshot.nodes = 0
            label = 'bindings:' + function.__module__ + '.' + function.__qualname__ + ':' + str(snapshot.references[id(function)])
            parts.append((label, snapshot.digest(snapshot.function_bindings(function))))
            processed_functions.add(function)
    return tuple(parts)


def projection_complete_semantic_fingerprint() -> str | None:
    """No retained plan/owner: unsupported live objects keep the original full path."""
    try:
        return _ProjectionSemanticSnapshot.digest(_projection_semantic_components())
    except (ExecutableDependencyError, AttributeError, ValueError, TypeError):
        return None


class _MinuteStudyInputSemanticSnapshot(_ProjectionSemanticSnapshot):
    def value(self, value: object, depth: int = 0, *, globals: bool = True) -> object:
        from types import (
            BuiltinFunctionType, GetSetDescriptorType, MemberDescriptorType,
            MethodDescriptorType, MethodWrapperType, WrapperDescriptorType,
        )

        if isinstance(value, type) and any(value is item for item in (BuiltinFunctionType, GetSetDescriptorType,
                MemberDescriptorType, MethodDescriptorType, MethodWrapperType, WrapperDescriptorType)):
            self.nodes += 1
            if self.nodes > 8192 or depth > 64:
                raise ExecutableDependencyError("minute projection live semantic node/depth limit")
            # These immutable runtime types have canonical aliases in types,
            # while their reported builtins qualname is not an exported binding.
            return {"runtime_type": (value.__module__, value.__qualname__)}
        if type(value) is ContextVar:
            self.nodes += 1
            if self.nodes > 8192 or depth > 64:
                raise ExecutableDependencyError("minute projection live semantic node/depth limit")
            identity = id(value)
            if identity in self.references:
                return {"reference": self.references[identity]}
            self.references[identity] = len(self.references)
            self.reference_objects.append(value)
            # The handle/name are immutable. Its value is lexical state, read
            # freshly by the actual budget/authentication operations, not a
            # retained permission or fingerprint decision.
            return {"context_variable": value.name}
        return super().value(value, depth, globals=globals)

    def function_bindings(self, function: FunctionType) -> object:
        from rquant.minute_backtest_parameter_contracts import _parameter_parsed_global_paths

        dependencies = []
        for name, paths in _parameter_parsed_global_paths(function.__code__):
            if name not in function.__globals__:
                continue
            for path in paths:
                endpoint = function.__globals__[name]
                for component in path:
                    endpoint = (endpoint.__func__ if component == "__func__" and isinstance(endpoint, (staticmethod, classmethod))
                        else inspect.getattr_static(endpoint, component))
                target = endpoint.__func__ if isinstance(endpoint, (staticmethod, classmethod)) else endpoint
                dependencies.append((name, path, self.value(endpoint,
                    globals=isinstance(target, FunctionType) and target.__module__.startswith("rquant."))))
        return self.references[id(function)], dependencies


def _minute_study_input_semantic_components(parameters: object, *, producer_commit: str) -> tuple[object, ...]:
    """Complete live input/read binding evidence; no stored authorization decision."""
    import textwrap
    from rquant import definition_registry as definitions
    from rquant import executable_dependencies as executable
    from rquant.lab_job_center import LabCommandSubmissionFacade
    from rquant.minute_backtest_formal_adapter import MinuteFormalParameters, MinuteFormalReplayAdapter
    from rquant.minute_backtest_parameter_adapter import MinuteParameterFormalParameters, MinuteParameterFormalReplayAdapter
    from rquant.minute_backtest_parameter_contracts import FrozenMinuteParameterResearchInput, MinuteParameterSourceSeed
    from rquant.minute_backtest_parameter_contracts import _ParameterReadUnitContentEntries
    from rquant.minute_backtest_parameter_definition import build_minute_parameter_definition, build_minute_parameter_research_definition
    from rquant.minute_backtest_parameter_producer import (
        MinuteParameterPreparedPublication, MinuteParameterPublicationReceipt, MinuteParameterReplayCatalog,
        _resolved_read_functions,
    )
    from rquant.minute_backtest_parameters import MinuteParameterSet
    from rquant.strategy_job_adapters import StrategyJobAdapterRegistry
    from rquant.minute_backtest_installation import InstalledMinuteReplay
    from rquant.web.minute_backtest_service import _InstalledStudyReplayReader

    if type(parameters) is not MinuteParameterSet:
        raise TypeError("minute input proof requires its actual parameter model")
    snapshot = _MinuteStudyInputSemanticSnapshot()
    parts = []
    native = build_minute_parameter_definition(parameters, producer_commit=producer_commit)
    wrapper = build_minute_parameter_research_definition(producer_commit)
    for value in (parameters, native, wrapper):
        parts.append(snapshot.digest(snapshot.part(value)))
    roots = (*_resolved_read_functions(),
        MinuteFormalParameters.from_frozen.__func__, MinuteParameterFormalParameters.from_prepared.__func__,
        MinuteFormalReplayAdapter.parameters, MinuteFormalReplayAdapter.expected,
        MinuteFormalReplayAdapter.build_shard_inputs, MinuteFormalReplayAdapter.build_work_plan,
        MinuteParameterFormalReplayAdapter.parameters, MinuteParameterFormalReplayAdapter.expected,
        StrategyJobAdapterRegistry.plan,
        LabCommandSubmissionFacade.parameter_definitions_for_spec,
        LabCommandSubmissionFacade._validate_formal_submission_authorities,
        LabCommandSubmissionFacade._validate_private_publication,
        LabCommandSubmissionFacade.validate_prepared_experiment_submission,
        definitions.ImmutableDefinitionRegistry.read_feature_contract,
        definitions.ImmutableDefinitionRegistry.read_strategy_spec,
        definitions.ImmutableDefinitionRegistry._validate_stored_feature_execution,
        definitions.ImmutableDefinitionRegistry._validate_stored_strategy_execution,
        definitions.TrustedExecutableRegistry.__init__,
        definitions.TrustedExecutableRegistry.feature_bindings,
        definitions.TrustedExecutableRegistry.strategy_binding,
        executable.fingerprint_callable, executable.fingerprint_executable_bindings,
        InstalledMinuteReplay.verify_current, InstalledMinuteReplay.parameter_submission_facade,
        _InstalledStudyReplayReader.__init__, _InstalledStudyReplayReader.read,
        _minute_study_input_paths, _minute_study_accepted_shard, _capture_minute_study_input_verification,
        _verify_minute_study_input_boundaries,
        InstalledMinuteStudyProjection._verified_input_read.__wrapped__,
        InstalledMinuteStudyProjection._read_verified_input_trial,
        InstalledMinuteStudyProjection._materialize_indexed,
        _MinuteStudyVerifiedInputRead.__init__, _MinuteStudyVerifiedInputRead._assert_active,
        _MinuteStudyVerifiedInputRead._definition_registry,
        definitions._MinuteStudyInputDefinitionView.__init__, definitions._MinuteStudyInputDefinitionView._input_proof,
        definitions._MinuteStudyInputDefinitionView._validate_stored_feature_execution,
        definitions._MinuteStudyInputDefinitionView._validate_stored_strategy_execution,
        definitions._MinuteStudyInputDefinitionView._publish)
    for function in roots:
        parts.append(snapshot.digest(snapshot.part(function)))
    source_roots = set(roots)
    source_roots.update(item for owner in (native, wrapper)
        for item in (owner.entry_evaluator, owner.exit_evaluator, owner.evaluator) if isinstance(item, FunctionType))
    snapshot.models.update((MinuteParameterSet, type(parameters.parameters), MinuteFormalParameters,
        MinuteParameterFormalParameters, MinuteParameterPreparedPublication, MinuteParameterPublicationReceipt,
        MinuteParameterReplayCatalog, FrozenMinuteParameterResearchInput, MinuteParameterSourceSeed,
        definitions.FeatureContractRegistration, definitions.StrategySpecRegistration,
        _MinuteStudyInputVerification, _MinuteStudyAcceptedShard,
        _MinuteStudyInputFileEvidence, _MinuteStudyInputPath))
    processed_models: set[type[BaseModel]] = set()
    processed_schemas: set[int] = set()
    processed_functions: set[FunctionType] = set()
    while (snapshot.models - processed_models or snapshot.schemas.keys() - processed_schemas
            or snapshot.functions.keys() - processed_functions):
        for model in sorted(snapshot.models - processed_models, key=lambda item: (item.__module__, item.__qualname__)):
            parts.append(snapshot.digest(snapshot.model(model)))
            processed_models.add(model)
        for identity in tuple(key for key in snapshot.schemas if key not in processed_schemas):
            schema, depth = snapshot.schemas[identity]
            snapshot.nodes = 0
            parts.append(snapshot.digest(snapshot.mapping(schema, depth)))
            processed_schemas.add(identity)
        for function in tuple(item for item in snapshot.functions if item not in processed_functions):
            # Inspect the current root source as well as loaded code/bindings.
            # A file edit which leaves an imported object alive is still live.
            try:
                try:
                    source_tree = ast.parse(textwrap.dedent(inspect.getsource(function)))
                except SyntaxError:
                    if function.__code__.co_name != "<lambda>":
                        raise
                    try:
                        source_lines, _ = inspect.findsource(function)
                    except OSError as exc:
                        raise ExecutableDependencyError("minute input lambda source unavailable") from exc
                    # An argument-line fragment is not a standalone statement.
                    # Locate its expression in the same actual file and line.
                    lambdas = tuple(node for node in ast.walk(ast.parse("".join(source_lines)))
                        if isinstance(node, ast.Lambda) and node.lineno == function.__code__.co_firstlineno)
                    if len(lambdas) != 1:
                        raise ExecutableDependencyError("minute input lambda source position is ambiguous")
                    source_tree = lambdas[0]
                source = ast.dump(source_tree,
                    annotate_fields=True, include_attributes=False)
            except OSError as exc:
                if function is _ParameterReadUnitContentEntries.__init__:
                    from dataclasses import MISSING

                    source = ast.dump(ast.parse(textwrap.dedent(inspect.getsource(_ParameterReadUnitContentEntries))),
                        annotate_fields=True, include_attributes=False)
                    field_content = tuple((field.name, field.type,
                        {"dataclass_missing": True} if field.default is MISSING else field.default,
                        {"dataclass_missing": True} if field.default_factory is MISSING else field.default_factory,
                        field.init, field.repr, field.hash, field.compare, field.metadata, field.kw_only)
                        for field in fields(_ParameterReadUnitContentEntries))
                    parts.append(snapshot.digest(snapshot.part(field_content)))
                elif function in source_roots:
                    raise ExecutableDependencyError(
                        f"minute input source unavailable: {function.__module__}.{function.__qualname__}") from exc
                else:
                    # The original canonical graph permits generated dependency
                    # functions without source; their complete code remains live.
                    source = None
            parts.append(snapshot.digest((function.__module__, function.__qualname__, source)))
            snapshot.nodes = 0
            parts.append(snapshot.digest(snapshot.function_bindings(function)))
            processed_functions.add(function)
    return tuple(parts)


def _minute_study_input_semantic_fingerprint(parameters: object, *, producer_commit: str) -> str | None:
    try:
        return _ProjectionSemanticSnapshot.digest(_minute_study_input_semantic_components(
            parameters, producer_commit=producer_commit))
    except (ExecutableDependencyError, AttributeError, ValueError, TypeError, OSError, SyntaxError):
        return None


def _minute_study_input_paths(catalog: MinuteParameterReplayCatalog,
    prepared: MinuteParameterPreparedPublication, baseline: MinuteParameterFactSourceReference,
    prepared_binding: DatasetSnapshotBinding, baseline_binding: DatasetSnapshotBinding,
) -> tuple[_MinuteStudyInputPath, ...]:
    if catalog.research_lake_root is None or tuple(item for item in catalog.fact_sources
            if item.fact_identity == prepared.baseline) != (baseline,):
        raise PermissionError("minute input proof has no unique current installed baseline")
    paths = []
    for prefix, reference, binding in (("prepared", prepared, prepared_binding),
            ("baseline", baseline, baseline_binding)):
        paths.extend((
            _MinuteStudyInputPath(role=prefix + "_source", path=reference.source.path),
            _MinuteStudyInputPath(role=prefix + "_receipt", path=reference.receipt.path),
            _MinuteStudyInputPath(role=prefix + "_metadata", path=reference.metadata_identity.source_path),
            _MinuteStudyInputPath(role=prefix + "_manifest",
                path=catalog.research_lake_root / binding.manifest_relative_path)))
        paths.extend(_MinuteStudyInputPath(role=prefix + "_artifact",
            path=catalog.research_lake_root / item.relative_path) for item in binding.manifest.artifacts)
    return tuple(paths)


def _minute_study_accepted_shard(value: object, *, work_units: int) -> _MinuteStudyAcceptedShard:
    from rquant.lab_shard_protocol import LabShardDefinition
    from rquant.lab_jobs import LabShardRecord

    if type(value) is LabShardDefinition:
        if value.work_plan is None or value.work_plan.work_units != work_units:
            raise PermissionError("minute input proof differs from the accepted work plan")
    elif type(value) is LabShardRecord:
        if value.work_units != work_units:
            raise PermissionError("minute input proof differs from the accepted shard work")
    else:
        raise TypeError("minute input proof needs the original typed shard")
    return _MinuteStudyAcceptedShard(shard_id=value.shard_id, shard_index=value.shard_index,
        plan_hash=value.plan_hash, payload_hash=value.payload_hash,
        payload_sha256=hashlib.sha256(value.payload_json.encode("utf-8")).hexdigest(),
        adapter_id=value.adapter_id, adapter_version=value.adapter_version, work_units=work_units)


def _verify_minute_study_input_boundaries(catalog: MinuteParameterReplayCatalog,
    proof: _MinuteStudyInputVerification) -> None:
    from rquant.metadata_catalog import ImmutableDuckDBMetadataCatalog
    from rquant.minute_backtest_parameter_producer import _verify_minute_study_input_files

    paths = _minute_study_input_paths(catalog, proof.prepared, proof.baseline,
        proof.prepared_binding, proof.baseline_binding)
    _verify_minute_study_input_files(paths, expected=proof.files)
    for identity in (proof.prepared.metadata_identity, proof.baseline.metadata_identity):
        ImmutableDuckDBMetadataCatalog._reject_operational_alias(
            os.stat(identity.source_path, follow_symlinks=False), catalog.forbidden_paths)


def _capture_minute_study_input_verification(*, catalog: MinuteParameterReplayCatalog,
    prepared: MinuteParameterPreparedPublication, publication: MinuteParameterPublicationReceipt,
    parameters: MinuteParameterFormalParameters, definitions: object, shard: object, as_of: datetime,
) -> _MinuteStudyInputVerification | None:
    from rquant.minute_backtest_parameter_producer import _capture_minute_study_input_files

    frozen = publication.frozen
    recipe, code = frozen.runtime.parameters, frozen.runtime.producer_commit
    before = _minute_study_input_semantic_fingerprint(recipe, producer_commit=code)
    if before is None:
        return None
    if parameters != MinuteParameterFormalParameters.from_prepared(frozen, prepared):
        raise PermissionError("minute input proof differs from complete accepted parameters")
    baseline, = (item for item in catalog.fact_sources if item.fact_identity == prepared.baseline)
    original = catalog.resolve_fact(source_key=baseline.source_key, source_version=baseline.source_version,
        owner_id=baseline.owner_id, full_input_hash=baseline.full_input_hash)
    native = definitions.read_strategy_spec(frozen.native_registration.fingerprint, as_of=as_of)
    wrapper = definitions.read_strategy_spec(frozen.wrapper_registration.fingerprint, as_of=as_of)
    if (native, wrapper) != (frozen.native_registration, frozen.wrapper_registration):
        raise PermissionError("minute input proof differs from current native/wrapper registrations")
    features = tuple(definitions.read_feature_contract(fingerprint, as_of=as_of)
        for fingerprint in dict.fromkeys((native.feature_contract_fingerprint, wrapper.feature_contract_fingerprint)))
    if any(item is None for item in features) or frozen.feature_registration not in features:
        raise PermissionError("minute input proof lacks the original complete feature records")
    paths = _minute_study_input_paths(catalog, prepared, baseline, publication.binding, original.binding)
    files = _capture_minute_study_input_files(paths)
    after = _minute_study_input_semantic_fingerprint(recipe, producer_commit=code)
    if after != before:
        raise PermissionError("minute current input semantics changed during complete proof generation")
    return _MinuteStudyInputVerification(prepared=prepared, baseline=baseline,
        prepared_binding=publication.binding, baseline_binding=original.binding,
        catalog_sha256=canonical_sha256(catalog.model_dump(mode="json")), files=files, parameters=parameters,
        native_registration=native, parameter_registration=wrapper, feature_registrations=features,
        expected_publication_hash=canonical_sha256(publication.model_dump(mode="json")),
        result_budget=frozen.result_budget, accepted_shard=_minute_study_accepted_shard(shard, work_units=parameters.work_units),
        input_semantic_fingerprint=after)


class _MinuteStudyVerifiedInputRead:
    """A lexical read minted only after the actual projection authenticates it."""

    def __init__(self) -> None:
        raise TypeError("only an authenticated projection can open a minute input read")

    def _assert_active(self, *, reader: object | None = None,
        catalog: MinuteParameterReplayCatalog | None = None, spec: object | None = None,
    ) -> _MinuteStudyInputVerification:
        if (getattr(self, "_closed", True) or getattr(self, "_scope_variable", None) is not _ACTIVE_MINUTE_INPUT_READ
                or _ACTIVE_MINUTE_INPUT_READ.get() is not self or getattr(self, "_thread", None) != get_ident()):
            raise PermissionError("minute input read is not the exact active authenticated scope")
        projection = self._projection
        projection.verify_current()
        installation = projection.installation
        if ((reader is not None and reader is not installation.reader)
                or (catalog is not None and catalog != installation.profile.parameter_catalog)
                or (spec is not None and spec != self._authority.job.spec)):
            raise PermissionError("minute input read belongs to another installed reader/catalog/spec")
        return self._certificate.input_verification

    def _definition_registry(self) -> object:
        from rquant.definition_registry import _MinuteStudyInputDefinitionView

        self._assert_active()
        return _MinuteStudyInputDefinitionView(self._projection.installation.definitions, _minute_input_read=self)


def _identity(value: os.stat_result) -> MinuteStudyProjectionPayloadIdentity:
    if not stat.S_ISREG(value.st_mode) or value.st_uid != os.getuid() or value.st_nlink != 1 or stat.S_IMODE(value.st_mode) != 0o400:
        raise PermissionError("minute derived payload must be independent owned immutable bytes")
    return MinuteStudyProjectionPayloadIdentity(device=value.st_dev, inode=value.st_ino, size=value.st_size,
        mtime_ns=value.st_mtime_ns, ctime_ns=value.st_ctime_ns, owner_uid=value.st_uid)


@contextmanager
def _directory(path: Path, expected: MinuteStudyProjectionDirectory | None = None) -> Iterator[int]:
    if not path.is_absolute() or path != Path(os.path.abspath(path)):
        raise PermissionError("minute derived directory is not normalized")
    flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW
    fd = os.open(path.anchor, flags)
    parents: list[tuple[Path, int, os.stat_result]] = []
    current = Path(path.anchor)
    try:
        for part in path.parts[1:]:
            child = os.open(part, flags, dir_fd=fd)
            parents.append((current, fd, os.fstat(fd)))
            current = current / part
            fd = child
        before = os.fstat(fd)
        if before.st_uid != os.getuid() or stat.S_IMODE(before.st_mode) != 0o700:
            raise PermissionError("minute derived directory must be private owned 0700")
        observed = MinuteStudyProjectionDirectory(path=path, device=before.st_dev, inode=before.st_ino, owner_uid=before.st_uid)
        if expected is not None and observed != expected:
            raise PermissionError("minute derived directory binding changed")
        yield fd
        checks = (*parents, (path, fd, before))
        for name, opened, prior in checks:
            after, named = os.fstat(opened), os.stat(name, follow_symlinks=False)
            attrs = ("st_dev", "st_ino", "st_mode", "st_uid")
            if any(getattr(prior, attr) != getattr(after, attr) or getattr(prior, attr) != getattr(named, attr) for attr in attrs):
                raise PermissionError("minute derived directory changed during operation")
    finally:
        os.close(fd)
        for _, parent, _ in reversed(parents):
            os.close(parent)


def _directory_reference(path: Path) -> MinuteStudyProjectionDirectory:
    with _directory(path) as fd:
        value = os.fstat(fd)
        return MinuteStudyProjectionDirectory(path=path, device=value.st_dev, inode=value.st_ino, owner_uid=value.st_uid)


def _write_new(parent: int, name: str, data: bytes, mode: int) -> None:
    fd = os.open(name, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, mode, dir_fd=parent)
    try:
        os.fchmod(fd, mode)
        with os.fdopen(os.dup(fd), "wb") as stream:
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
    finally:
        os.close(fd)
    os.fsync(parent)


def _mac(key: bytes, kind: str, value: object) -> str:
    return hmac.new(key, _DOMAIN + kind.encode("ascii") + b"\0" + canonical_json_bytes(value), hashlib.sha256).hexdigest()


def bootstrap_minute_study_projection(installation: InstalledMinuteReplay, *, state_root: Path) -> MinutePrivateFileReference:
    """Explicit private installation; an authority file alone does not enable reads."""
    installation.verify_current()
    profile = installation.profile
    # No original authority or result directory may become a projection writer root.
    if any(state_root == path or state_root.is_relative_to(path) for path in (
        profile.runtime_root, profile.runtime_deployment_root, profile.final_artifact_root,
        profile.snapshot_root, profile.research_lake_root)):
        raise PermissionError("minute projection requires an independent writer namespace")
    with _directory(state_root) as parent:
        os.mkdir("payloads", mode=0o700, dir_fd=parent)
        _write_new(parent, "authentication.bin", os.urandom(32), 0o600)
        _write_new(parent, "publish.lock", b"minute-study-projection/v1\n", 0o600)
        _write_new(parent, "index.sqlite3", b"", 0o600)
    key, key_ref = _secure_private_bytes(state_root / "authentication.bin")
    index_path = state_root / "index.sqlite3"
    with closing(sqlite3.connect(index_path)) as db:
        db.executescript("""
            CREATE TABLE projection_entry(job_id TEXT PRIMARY KEY, payload_sha256 TEXT NOT NULL, entry_json BLOB NOT NULL, entry_mac TEXT NOT NULL);
            CREATE TABLE projection_meta(id INTEGER PRIMARY KEY CHECK(id=1), body_json BLOB NOT NULL, body_mac TEXT NOT NULL);
            CREATE TABLE projection_pending(job_id TEXT PRIMARY KEY, last_attempt INTEGER NOT NULL, body_json BLOB NOT NULL, body_mac TEXT NOT NULL);
        """)
        body = {"generation": 0, "previous": "0" * 64, "commit": "0" * 64, "cursor": None, "round": 0, "pending": []}
        db.execute("INSERT INTO projection_meta VALUES(1,?,?)", (canonical_json_bytes(body), _mac(key, "generation", body)))
        db.commit()
    index_path.chmod(0o600)
    index = index_path.stat(follow_symlinks=False)
    authority = MinuteStudyProjectionAuthority(installation_reference=installation.reference, code_sha=profile.code_sha,
        deployment_profile_id=profile.deployment_profile_id, deployment_generation_hash=profile.deployment_generation_hash,
        authority_manifest_hash=profile.authority_manifest_hash, state_directory=_directory_reference(state_root),
        payload_directory=_directory_reference(state_root / "payloads"), index_device=index.st_dev, index_inode=index.st_ino,
        key_reference=key_ref, algorithm_fingerprint=projection_algorithm_fingerprint())
    with _directory(state_root, authority.state_directory) as parent:
        _write_new(parent, "authority.json", canonical_json_bytes(authority.model_dump(mode="json")), 0o600)
    installation.verify_current()
    _, reference = _secure_private_bytes(state_root / "authority.json")
    return reference


class InstalledMinuteStudyProjection:
    def __init__(self, installation: InstalledMinuteReplay, authority: MinuteStudyProjectionAuthority,
        reference: MinutePrivateFileReference, *, writable: bool) -> None:
        self.installation, self.authority, self.reference = installation, authority, reference
        self.writable, self._closed = writable, False
        self.index_path = authority.state_directory.path / "index.sqlite3"

    def close(self) -> None:
        self._closed = True

    def __enter__(self) -> Self:
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        self.close()

    def payload_path(self, job_id: UUID) -> Path:
        return self.authority.payload_directory.path / (str(UUID(str(job_id))) + ".json")

    def verify_current(self) -> None:
        self._verified_key()

    @contextmanager
    def _verified_input_read(self, prepared: MinuteParameterPreparedStudyTrial, *,
        as_of: datetime) -> Iterator[_MinuteStudyVerifiedInputRead | None]:
        from rquant.minute_backtest_parameter_contracts import _parameter_read_unit_retention
        from rquant.minute_backtest_parameter_producer import _resolved_read_retained_bytes
        from rquant.minute_backtest_parameter_adapter import _decode_prepared
        from rquant.minute_backtest_parameters import MinuteParameterSet
        from rquant.runtime_contracts import normalize_aware_utc
        from rquant.runtime_read_interrupt import ReadInterruptedError, read_interrupt_requested

        visible_at = normalize_aware_utc(as_of)
        key = self._verified_key()
        with self._connection() as db:
            generation = self._generation(db, key)
            entry = self._read_entry(db, key, prepared.trial.job_id)
        certificate = None if entry is None else self.read_certificate(entry.binding)
        if certificate is None or certificate.input_verification is None:
            yield None
            return
        proof = certificate.input_verification
        authority = self.installation.reader.get_artifact_preview_authority(prepared.trial.job_id)
        if authority is None or authority.evidence.indexed_at > visible_at or authority.job.updated_at > visible_at:
            yield None
            return
        spec = authority.job.spec
        if spec != prepared.marker.command.spec or spec.code_sha != self.installation.profile.code_sha:
            raise PermissionError("minute certified input belongs to another current task/code")
        selected = MinuteParameterFormalParameters.model_validate({item.name: item.value for item in spec.parameters.arguments})
        catalog = self.installation.profile.parameter_catalog
        if (catalog is None or canonical_sha256(catalog.model_dump(mode="json")) != proof.catalog_sha256
                or selected != proof.parameters or selected.prepared_publication_json is None
                or _decode_prepared(selected.prepared_publication_json) != proof.prepared
                or proof.expected_publication_hash != certificate.binding.publication_hash):
            raise PermissionError("minute certified input differs from current complete controls")
        recipe = MinuteParameterSet.model_validate_json(selected.parameter_set_json)
        if (recipe != prepared.trial.command.config.parameters or proof.prepared.study_binding != prepared.binding
                or (proof.prepared.full_input_hash, proof.prepared.core_input_hash, proof.prepared.seed_hash) != (
                    prepared.full_input_hash, prepared.core_input_hash, prepared.seed_hash)):
            raise PermissionError("minute certified input differs from current complete recipe/fold")
        current = _minute_study_input_semantic_fingerprint(recipe, producer_commit=spec.code_sha)
        if current is None:
            yield None
            return
        if current != proof.input_semantic_fingerprint:
            raise PermissionError("minute complete current input semantics changed")
        _verify_minute_study_input_boundaries(catalog, proof)
        fee = _resolved_read_retained_bytes((certificate, authority, prepared, generation))
        fee += sys.getsizeof(_MinuteStudyVerifiedInputRead) + 512
        with _parameter_read_unit_retention(retained_bytes=fee) as retained:
            if not retained:
                yield None
                return
            context = object.__new__(_MinuteStudyVerifiedInputRead)
            context._projection, context._certificate, context._authority = self, certificate, authority
            context._thread, context._scope_variable, context._closed = get_ident(), _ACTIVE_MINUTE_INPUT_READ, False
            token = _ACTIVE_MINUTE_INPUT_READ.set(context)
            try:
                if read_interrupt_requested():
                    raise ReadInterruptedError("minute input read stop before current certified input")
                yield context
                context._assert_active()
                _verify_minute_study_input_boundaries(catalog, proof)
                if _minute_study_input_semantic_fingerprint(recipe, producer_commit=spec.code_sha) != current:
                    raise PermissionError("minute complete current input semantics changed during read")
                if (self.installation.reader.get_artifact_preview_authority(prepared.trial.job_id) != authority
                        or self.read_certificate(certificate.binding) != certificate):
                    raise PermissionError("minute certified input authority changed during read")
                with self._connection() as db:
                    if self._generation(db, key) != generation:
                        raise PermissionError("minute certified input generation changed during read")
                if read_interrupt_requested():
                    raise ReadInterruptedError("minute input read stop after current certified input")
                self.verify_current()
            finally:
                context._closed = True
                context._projection = context._certificate = context._authority = None
                _ACTIVE_MINUTE_INPUT_READ.reset(token)

    def _verified_key(self) -> bytes:
        if self._closed:
            raise RuntimeError("minute projection runtime is closed")
        self.installation.verify_current()
        data, _ = _secure_private_bytes(self.reference.path, self.reference)
        observed = strict_model_validate_json(MinuteStudyProjectionAuthority, data)
        if observed != self.authority or observed.installation_reference != self.installation.reference:
            raise PermissionError("minute projection is not bound to the current installed authority")
        profile = self.installation.profile
        if (observed.code_sha, observed.deployment_profile_id, observed.deployment_generation_hash, observed.authority_manifest_hash) != (
            profile.code_sha, profile.deployment_profile_id, profile.deployment_generation_hash, profile.authority_manifest_hash):
            raise PermissionError("minute projection current deployment authority differs")
        key, _ = _secure_private_bytes(observed.key_reference.path, observed.key_reference)
        if len(key) != 32 or projection_algorithm_fingerprint() != observed.algorithm_fingerprint:
            raise PermissionError("minute projection key or live algorithm changed")
        return key

    @contextmanager
    def _connection(self, *, write: bool = False) -> Iterator[sqlite3.Connection]:
        if write and not self.writable:
            raise PermissionError("minute projection reads cannot publish")
        with _directory(self.authority.state_directory.path, self.authority.state_directory) as directory:
            descriptor = os.open("index.sqlite3", os.O_RDONLY | os.O_NOFOLLOW, dir_fd=directory)
            try:
                before = os.fstat(descriptor)
                if not stat.S_ISREG(before.st_mode) or (before.st_dev, before.st_ino, before.st_uid, stat.S_IMODE(before.st_mode), before.st_nlink) != (
                    self.authority.index_device, self.authority.index_inode, os.getuid(), 0o600, 1):
                    raise PermissionError("minute projection index physical authority differs")
                mode = "rw" if write else "ro"
                with closing(sqlite3.connect(self.index_path.as_uri() + "?mode=" + mode, uri=True, isolation_level=None)) as db:
                    db.execute("BEGIN IMMEDIATE" if write else "BEGIN")
                    try:
                        yield db
                        db.commit()
                    except BaseException:
                        db.rollback()
                        raise
                after, named = os.fstat(descriptor), os.stat("index.sqlite3", dir_fd=directory, follow_symlinks=False)
                attrs = ("st_dev", "st_ino", "st_mode", "st_uid", "st_nlink")
                if any(getattr(before, attr) != getattr(after, attr) or getattr(before, attr) != getattr(named, attr) for attr in attrs):
                    raise PermissionError("minute projection index changed during operation")
            finally:
                os.close(descriptor)

    def _generation(self, db: sqlite3.Connection, key: bytes) -> dict[str, object]:
        row = db.execute("SELECT body_json,body_mac FROM projection_meta WHERE id=1").fetchone()
        if row is None or len(row[0]) > MAX_MINUTE_CONTROL_BYTES:
            raise PermissionError("minute projection authenticated generation is absent or oversized")
        from rquant.strict_json import strict_json_loads
        value = strict_json_loads(row[0])
        if not isinstance(value, dict) or not hmac.compare_digest(row[1], _mac(key, "generation", value)):
            raise PermissionError("minute projection generation authentication differs")
        if type(value.get("generation")) is not int or type(value.get("pending")) is not list or len(value["pending"]) > 8:
            raise PermissionError("minute projection generation state is invalid")
        return value

    def _read_entry(self, db: sqlite3.Connection, key: bytes, job_id: UUID) -> MinuteStudyProjectionEntry | None:
        row = db.execute("SELECT payload_sha256,entry_json,entry_mac FROM projection_entry WHERE job_id=?", (str(job_id),)).fetchone()
        if row is None:
            return None
        if len(row[1]) > _ENTRY_LIMIT:
            raise PermissionError("minute projection entry exceeds original control capacity")
        value = strict_model_validate_json(MinuteStudyProjectionEntry, row[1])
        if not hmac.compare_digest(row[2], _mac(key, "entry", value.model_dump(mode="json"))) or (
            value.authority_hash, value.binding.job_id, value.payload_sha256, value.algorithm_fingerprint) != (
            self.reference.content_sha256, job_id, row[0], self.authority.algorithm_fingerprint):
            raise PermissionError("minute projection entry is not an authorized publication")
        return value

    def _payload(self, job_id: UUID) -> tuple[bytes, MinuteStudyProjectionPayloadIdentity]:
        with _directory(self.authority.payload_directory.path, self.authority.payload_directory) as directory:
            name = self.payload_path(job_id).name
            descriptor = os.open(name, os.O_RDONLY | os.O_NOFOLLOW, dir_fd=directory)
            try:
                before = _identity(os.fstat(descriptor))
                data = bytearray()
                while chunk := os.read(descriptor, min(65_536, MAX_MINUTE_CONTROL_BYTES + 1 - len(data))):
                    data.extend(chunk)
                    if len(data) > MAX_MINUTE_CONTROL_BYTES:
                        raise PermissionError("minute derived payload exceeds original control capacity")
                if before != _identity(os.fstat(descriptor)) or before != _identity(os.stat(name, dir_fd=directory, follow_symlinks=False)):
                    raise PermissionError("minute derived payload changed during complete read")
                return bytes(data), before
            finally:
                os.close(descriptor)

    def read_certificate(self, binding: MinuteStudyProjectionBinding) -> MinuteStudyProjectionCertificate | None:
        key = self._verified_key()
        with self._connection() as db:
            generation = self._generation(db, key)
            entry = self._read_entry(db, key, binding.job_id)
            if entry is None:
                result = None
            else:
                if entry.binding != binding:
                    raise PermissionError("minute projection belongs to another complete original binding")
                data, identity = self._payload(binding.job_id)
                if identity != entry.payload_identity or hashlib.sha256(data).hexdigest() != entry.payload_sha256:
                    raise PermissionError("minute projection immutable payload differs from authorized bytes")
                result = strict_model_validate_json(MinuteStudyProjectionCertificate, data)
                if result.binding != binding or result.algorithm_fingerprint != entry.algorithm_fingerprint:
                    raise PermissionError("minute projection payload binding differs")
            if self._generation(db, key) != generation:
                raise PermissionError("minute projection generation changed during read")
        self.verify_current()
        # A concurrent writer cannot silently change the authenticated read epoch.
        with self._connection() as db:
            if self._generation(db, key) != generation:
                raise PermissionError("minute projection generation changed after complete read")
        return result

    def read_trial(self, prepared: MinuteParameterPreparedStudyTrial, *, as_of: datetime) -> MinuteParameterStudyTrialResult | None:
        """Authenticated derivation, with the original current non-row checks."""
        from rquant.lab_artifact_preview import ArtifactPreviewUnavailableError
        from rquant.lab_job_protocol import LabCommandEnvelope
        from rquant.lab_finalizer import LabFinalizerMetrics
        from rquant.lab_jobs import ShardStatus
        from rquant.minute_backtest_artifact import COMPLETE_RESULT_CONTRACT_VERSION, MINUTE_RESULT_TABLE_NAMES, MinuteSealedReplayIntegrityError
        from rquant.minute_backtest_parameter_artifact import MinuteParameterSealedReplayReader
        from rquant.minute_backtest_parameter_study_execution import MinuteParameterPreparedStudyTrial, MinuteParameterStudyTrialResult
        from rquant.runtime_contracts import normalize_aware_utc
        from rquant.runtime_read_interrupt import ReadInterruptedError, read_interrupt_requested

        if type(prepared) is not MinuteParameterPreparedStudyTrial:
            raise TypeError("minute projection needs the actual original prepared trial")
        prepared = MinuteParameterPreparedStudyTrial.model_validate(prepared.model_dump(mode="python"))
        visible_at = normalize_aware_utc(as_of)
        key = self._verified_key()
        with self._connection() as db:
            entry = self._read_entry(db, key, prepared.trial.job_id)
        if entry is None:
            self.verify_current()
            return None
        certificate = self.read_certificate(entry.binding)
        assert certificate is not None
        before = projection_complete_semantic_fingerprint()
        if before is None or certificate.complete_semantic_fingerprint is None or certificate.original_byte_fingerprint is None:
            return None
        if before != certificate.complete_semantic_fingerprint:
            raise PermissionError("minute complete result/derivation live semantics changed")
        if read_interrupt_requested():
            raise ReadInterruptedError("minute projection stop before fresh original validation")
        job_id = prepared.trial.job_id
        authority = self.installation.reader.get_artifact_preview_authority(job_id)
        if authority is None or authority.evidence.indexed_at > visible_at or authority.job.updated_at > visible_at:
            return None
        spec = authority.job.spec
        if spec != prepared.marker.command.spec:
            raise PermissionError("minute projected study differs from the original complete task")
        selected = MinuteParameterSealedReplayReader._parameter_model().model_validate({item.name: item.value for item in spec.parameters.arguments})
        recipe = prepared.trial.command.config.parameters
        if (selected.owner_id, selected.native_strategy_id, selected.native_strategy_version) != (
            prepared.trial.command.actor_id, recipe.definition_id, recipe.definition_version):
            raise PermissionError("minute projected study differs from requested owner/native version")
        if certificate.input_verification is not None:
            with self._verified_input_read(prepared, as_of=visible_at) as input_read:
                if input_read is not None:
                    return self._read_verified_input_trial(prepared, input_read=input_read,
                        certificate=certificate, authority=authority, before=before, as_of=visible_at)
        from contextlib import nullcontext
        from rquant.minute_backtest_parameter_adapter import MinuteParameterFormalReplayAdapter, _decode_prepared
        from rquant.minute_backtest_parameter_producer import resolved_minute_parameter_read_unit
        from rquant.strategy_job_adapters import StrategyJobAdapterRegistry

        unit_scope = (nullcontext(None) if selected.prepared_publication_json is None else
            resolved_minute_parameter_read_unit(self.installation.profile.parameter_catalog,
                _decode_prepared(selected.prepared_publication_json)))
        with unit_scope as unit:
            artifacts = ArtifactPreviewReader(reader=self.installation.reader, artifact_root=self.installation.authority.final_artifact_root)
            reader = MinuteParameterSealedReplayReader(reader=self.installation.reader, artifact_reader=artifacts,
                submission_facade=(self.installation.parameter_submission_facade(spec) if unit is None else
                    self.installation.parameter_submission_facade(spec, resolved_read_unit=unit)), catalog=self.installation.profile.parameter_catalog)
            adapter = (reader._adapter() if unit is None else
                MinuteParameterFormalReplayAdapter(reader.catalog, resolved_read_unit=unit))
            parameters = adapter.parameters(spec)
            expected = adapter.expected(parameters)
            frozen = expected.frozen
            if (frozen.runtime.parameters, frozen.runtime.study_binding) != (recipe, prepared.binding):
                raise PermissionError("minute projected study changed the full recipe/selection")
            facade = reader.submission_facade
            assert facade.experiment_registry is not None and facade.definition_registry is not None
            intent = facade.experiment_registry.get_submission_intent_for_job(job_id)
            if intent is None:
                raise MinuteSealedReplayIntegrityError("minute projected job has no original accepted submission intent")
            try:
                envelope = strict_model_validate_json(LabCommandEnvelope, intent.envelope_json)
                if envelope.command.job_id != job_id or envelope.command.spec != spec:
                    raise MinuteSealedReplayIntegrityError("minute projected job differs from full accepted request")
                facade.validate_prepared_experiment_submission(envelope, observed_at=visible_at)
                native = facade.definition_registry.read_strategy_spec(frozen.native_registration.fingerprint, as_of=visible_at)
                if native != frozen.native_registration:
                    raise MinuteSealedReplayIntegrityError("minute projected native registration differs from original registry")
                assert spec.experiment is not None and spec.experiment.formal_plan_id is not None
                plan = facade.experiment_registry.resolve_formal_plan_by_id(spec.experiment.formal_plan_id, as_of=visible_at)
            except MinuteSealedReplayIntegrityError:
                raise
            except Exception as exc:
                raise MinuteSealedReplayIntegrityError("minute projected accepted request/registry authority is invalid") from exc
            shards = self.installation.reader.list_shards(job_id)
            definition, = (reader._registry() if unit is None else StrategyJobAdapterRegistry((adapter,))).plan(spec)
            if len(shards) != 1 or shards[0].status is not ShardStatus.SUCCEEDED:
                raise MinuteSealedReplayIntegrityError("minute projected original shard is incomplete")
            shard = shards[0]
            if (shard.shard_id, shard.shard_index, shard.plan_hash, shard.payload_hash, shard.payload_json,
                shard.adapter_id, shard.adapter_version, shard.work_units) != (
                definition.shard_id, definition.shard_index, definition.plan_hash, definition.payload_hash,
                definition.payload_json, definition.adapter_id, definition.adapter_version, parameters.work_units):
                raise MinuteSealedReplayIntegrityError("minute projected original shard differs from full accepted plan")
            budget = frozen.result_budget
            try:
                evidence = artifacts.read_complete_byte_evidence(job_id, table_names=MINUTE_RESULT_TABLE_NAMES,
                    budget=ArtifactCompleteTableBudget(max_table_count=budget.table_count,
                        max_table_bytes=budget.table_bytes, max_total_bytes=budget.total_bytes))
            except ArtifactPreviewUnavailableError:
                return None
            if evidence.authority != authority or evidence.spec != spec:
                raise MinuteSealedReplayIntegrityError("minute projected Lab authority changed during complete byte read")
            manifest = evidence.manifest
            if (manifest.plan_hash, manifest.adapter_id, manifest.adapter_version, manifest.result_contract_version) != (
                shard.plan_hash, adapter.adapter_id, adapter.adapter_version, COMPLETE_RESULT_CONTRACT_VERSION):
                raise MinuteSealedReplayIntegrityError("minute projected physical adapter/plan identity conflicts")
            try:
                metrics = strict_model_validate_json(LabFinalizerMetrics, canonical_json_bytes(evidence.metrics))
            except Exception as exc:
                raise MinuteSealedReplayIntegrityError("minute projected original finalizer metrics are invalid") from exc
            if (metrics.job_id, metrics.spec_hash, metrics.plan_hash, metrics.adapter_id, metrics.adapter_version,
                metrics.result_contract_version, metrics.finalizer_code_sha, metrics.shard_count) != (
                job_id, authority.job.spec_hash, shard.plan_hash, adapter.adapter_id, adapter.adapter_version,
                COMPLETE_RESULT_CONTRACT_VERSION, spec.code_sha, 1) or metrics.shards[0].shard_id != shard.shard_id:
                raise MinuteSealedReplayIntegrityError("minute projected original finalizer identity conflicts")
            binding = certificate.binding
            if (binding.owner_id, binding.job_id, binding.shard_id, binding.spec_hash, binding.plan_hash,
                binding.payload_hash, binding.manifest_hash, binding.complete_result_hash, binding.completed_at,
                binding.full_input_hash, binding.core_input_hash, binding.seed_hash, binding.profile_hash,
                binding.parameter_hash, binding.study_binding_hash, binding.publication_hash, binding.formal_plan_id) != (
                selected.owner_id, job_id, shard.shard_id, authority.job.spec_hash, shard.plan_hash,
                shard.payload_hash, authority.evidence.manifest_hash, authority.evidence.complete_result_hash, authority.evidence.indexed_at,
                prepared.full_input_hash, prepared.core_input_hash, prepared.seed_hash, prepared.profile_hash,
                recipe.fingerprint, canonical_sha256(prepared.binding.model_dump(mode="json")),
                canonical_sha256(expected.model_dump(mode="json")), plan.plan_id):
                raise PermissionError("minute authenticated study derivation differs from current complete original binding")
            if certificate.original_table_bytes != evidence.encoded_table_bytes or certificate.original_byte_fingerprint != _byte_fingerprint(evidence):
                raise PermissionError("minute authenticated study derivation differs from complete original bytes")
            observed = MinuteParameterStudyTrialResult(prepared=prepared, spec_hash=binding.spec_hash,
                manifest_hash=binding.manifest_hash, complete_result_hash=binding.complete_result_hash,
                result_hash=binding.result_hash, completed_at=binding.completed_at, read_at=visible_at,
                training=certificate.training, validation=certificate.validation, independent_test=certificate.independent_test)
            if self.installation.reader.get_artifact_preview_authority(job_id) != authority:
                raise MinuteSealedReplayIntegrityError("minute projected authority changed after complete validation")
            if projection_complete_semantic_fingerprint() != before or self.read_certificate(binding) != certificate:
                raise PermissionError("minute complete live semantics/projection changed during read")
            if read_interrupt_requested():
                raise ReadInterruptedError("minute projection stop after complete original validation")
            self.verify_current()
            return observed

    def _read_verified_input_trial(self, prepared: MinuteParameterPreparedStudyTrial, *,
        input_read: _MinuteStudyVerifiedInputRead, certificate: MinuteStudyProjectionCertificate,
        authority: LabArtifactPreviewAuthority, before: str, as_of: datetime,
    ) -> MinuteParameterStudyTrialResult | None:
        from rquant.lab_artifact_preview import ArtifactPreviewUnavailableError
        from rquant.lab_job_protocol import LabCommandEnvelope
        from rquant.lab_finalizer import LabFinalizerMetrics
        from rquant.lab_jobs import ShardStatus
        from rquant.minute_backtest_artifact import COMPLETE_RESULT_CONTRACT_VERSION, MINUTE_RESULT_TABLE_NAMES, MinuteSealedReplayIntegrityError
        from rquant.minute_backtest_parameter_study_execution import MinuteParameterStudyTrialResult
        from rquant.runtime_read_interrupt import ReadInterruptedError, read_interrupt_requested

        spec = authority.job.spec
        proof = input_read._assert_active(reader=self.installation.reader,
            catalog=self.installation.profile.parameter_catalog, spec=spec)
        if input_read._certificate != certificate or input_read._authority != authority:
            raise PermissionError("minute input read differs from the complete authenticated result")
        parameters = proof.parameters
        facade = self.installation.parameter_submission_facade(spec, _minute_input_read=input_read)
        intent = facade.experiment_registry.get_submission_intent_for_job(prepared.trial.job_id)
        if intent is None:
            raise MinuteSealedReplayIntegrityError("minute projected job has no original accepted submission intent")
        try:
            envelope = strict_model_validate_json(LabCommandEnvelope, intent.envelope_json)
            if envelope.command.job_id != prepared.trial.job_id or envelope.command.spec != spec:
                raise MinuteSealedReplayIntegrityError("minute projected job differs from full accepted request")
            facade.validate_prepared_experiment_submission(envelope, observed_at=as_of)
            native = facade.definition_registry.read_strategy_spec(proof.native_registration.fingerprint, as_of=as_of)
            if native != proof.native_registration:
                raise MinuteSealedReplayIntegrityError("minute projected native registration differs from original registry")
            assert spec.experiment is not None and spec.experiment.formal_plan_id is not None
            plan = facade.experiment_registry.resolve_formal_plan_by_id(spec.experiment.formal_plan_id, as_of=as_of)
        except MinuteSealedReplayIntegrityError:
            raise
        except Exception as exc:
            raise MinuteSealedReplayIntegrityError("minute projected accepted request/registry authority is invalid") from exc
        shards = self.installation.reader.list_shards(prepared.trial.job_id)
        if len(shards) != 1 or shards[0].status is not ShardStatus.SUCCEEDED:
            raise MinuteSealedReplayIntegrityError("minute projected original shard is incomplete")
        shard = shards[0]
        if _minute_study_accepted_shard(shard, work_units=parameters.work_units) != proof.accepted_shard:
            raise MinuteSealedReplayIntegrityError("minute projected original shard differs from full accepted plan")
        artifacts = ArtifactPreviewReader(reader=self.installation.reader,
            artifact_root=self.installation.authority.final_artifact_root)
        budget = proof.result_budget
        try:
            evidence = artifacts.read_complete_byte_evidence(prepared.trial.job_id, table_names=MINUTE_RESULT_TABLE_NAMES,
                budget=ArtifactCompleteTableBudget(max_table_count=budget.table_count,
                    max_table_bytes=budget.table_bytes, max_total_bytes=budget.total_bytes))
        except ArtifactPreviewUnavailableError:
            return None
        if evidence.authority != authority or evidence.spec != spec:
            raise MinuteSealedReplayIntegrityError("minute projected Lab authority changed during complete byte read")
        manifest = evidence.manifest
        if (manifest.plan_hash, manifest.adapter_id, manifest.adapter_version, manifest.result_contract_version) != (
            shard.plan_hash, shard.adapter_id, shard.adapter_version, COMPLETE_RESULT_CONTRACT_VERSION):
            raise MinuteSealedReplayIntegrityError("minute projected physical adapter/plan identity conflicts")
        try:
            metrics = strict_model_validate_json(LabFinalizerMetrics, canonical_json_bytes(evidence.metrics))
        except Exception as exc:
            raise MinuteSealedReplayIntegrityError("minute projected original finalizer metrics are invalid") from exc
        if (metrics.job_id, metrics.spec_hash, metrics.plan_hash, metrics.adapter_id, metrics.adapter_version,
            metrics.result_contract_version, metrics.finalizer_code_sha, metrics.shard_count) != (
            prepared.trial.job_id, authority.job.spec_hash, shard.plan_hash, shard.adapter_id, shard.adapter_version,
            COMPLETE_RESULT_CONTRACT_VERSION, spec.code_sha, 1) or metrics.shards[0].shard_id != shard.shard_id:
            raise MinuteSealedReplayIntegrityError("minute projected original finalizer identity conflicts")
        binding = certificate.binding
        recipe = prepared.trial.command.config.parameters
        if (binding.owner_id, binding.job_id, binding.shard_id, binding.spec_hash, binding.plan_hash,
            binding.payload_hash, binding.manifest_hash, binding.complete_result_hash, binding.completed_at,
            binding.full_input_hash, binding.core_input_hash, binding.seed_hash, binding.profile_hash,
            binding.parameter_hash, binding.study_binding_hash, binding.publication_hash, binding.formal_plan_id) != (
            parameters.owner_id, prepared.trial.job_id, shard.shard_id, authority.job.spec_hash, shard.plan_hash,
            shard.payload_hash, authority.evidence.manifest_hash, authority.evidence.complete_result_hash, authority.evidence.indexed_at,
            prepared.full_input_hash, prepared.core_input_hash, prepared.seed_hash, prepared.profile_hash,
            recipe.fingerprint, canonical_sha256(prepared.binding.model_dump(mode="json")),
            proof.expected_publication_hash, plan.plan_id):
            raise PermissionError("minute authenticated study derivation differs from current complete original binding")
        if certificate.original_table_bytes != evidence.encoded_table_bytes or certificate.original_byte_fingerprint != _byte_fingerprint(evidence):
            raise PermissionError("minute authenticated study derivation differs from complete original bytes")
        observed = MinuteParameterStudyTrialResult(prepared=prepared, spec_hash=binding.spec_hash,
            manifest_hash=binding.manifest_hash, complete_result_hash=binding.complete_result_hash,
            result_hash=binding.result_hash, completed_at=binding.completed_at, read_at=as_of,
            training=certificate.training, validation=certificate.validation, independent_test=certificate.independent_test)
        if self.installation.reader.get_artifact_preview_authority(prepared.trial.job_id) != authority:
            raise MinuteSealedReplayIntegrityError("minute projected authority changed after complete validation")
        if projection_complete_semantic_fingerprint() != before or self.read_certificate(binding) != certificate:
            raise PermissionError("minute complete live semantics/projection changed during read")
        if read_interrupt_requested():
            raise ReadInterruptedError("minute projection stop after complete original validation")
        self.verify_current()
        return observed

    def _publish_verified(self, certificate: MinuteStudyProjectionCertificate) -> bool:
        """Private commit boundary; only materialize_original supplies product values."""
        if not self.writable:
            raise PermissionError("minute projection reads cannot publish")
        key = self._verified_key()
        certificate = MinuteStudyProjectionCertificate.model_validate(certificate)
        if certificate.algorithm_fingerprint != self.authority.algorithm_fingerprint:
            raise PermissionError("minute projection was derived under different live semantics")
        data = canonical_json_bytes(certificate.model_dump(mode="json"))
        # Worst-case physical identity integers have a bounded 64-bit representation.
        allowance = len(data) + _ENTRY_LIMIT
        if allowance > MAX_MINUTE_CONTROL_BYTES or allowance + certificate.original_table_bytes > MAX_RESULT_TOTAL_BYTES or allowance + certificate.complete_wire_bytes > MAX_RESULT_WIRE_BYTES:
            return False
        with _directory(self.authority.state_directory.path, self.authority.state_directory) as directory:
            lock = os.open("publish.lock", os.O_RDONLY | os.O_NOFOLLOW, dir_fd=directory)
            try:
                identity = os.fstat(lock)
                if not stat.S_ISREG(identity.st_mode) or identity.st_uid != os.getuid() or stat.S_IMODE(identity.st_mode) != 0o600 or identity.st_nlink != 1:
                    raise PermissionError("minute publication lock is not private")
                fcntl.flock(lock, fcntl.LOCK_EX)
                with self._connection(write=True) as db:
                    generation = self._generation(db, key)
                    existing = self._read_entry(db, key, certificate.binding.job_id)
                    if existing is not None:
                        prior, payload_identity = self._payload(certificate.binding.job_id)
                        if prior != data or payload_identity != existing.payload_identity or existing.binding != certificate.binding:
                            raise PermissionError("minute same-key projection publication conflicts")
                        return True
                    with _directory(self.authority.payload_directory.path, self.authority.payload_directory) as payloads:
                        final = self.payload_path(certificate.binding.job_id).name
                        temporary = "." + uuid4().hex + ".tmp"
                        try:
                            _write_new(payloads, temporary, data, 0o400)
                            try:
                                os.link(temporary, final, src_dir_fd=payloads, dst_dir_fd=payloads, follow_symlinks=False)
                            except FileExistsError:
                                prior, _ = self._payload(certificate.binding.job_id)
                                if prior != data:
                                    raise PermissionError("minute unindexed publication conflicts with verified content")
                        finally:
                            try:
                                os.unlink(temporary, dir_fd=payloads)
                            except FileNotFoundError:
                                pass
                        os.fsync(payloads)
                    observed, payload_identity = self._payload(certificate.binding.job_id)
                    if observed != data:
                        raise PermissionError("minute publication candidate changed before commit")
                    entry = MinuteStudyProjectionEntry(authority_hash=self.reference.content_sha256,
                        binding=certificate.binding, algorithm_fingerprint=certificate.algorithm_fingerprint,
                        payload_sha256=hashlib.sha256(data).hexdigest(), payload_identity=payload_identity)
                    body = canonical_json_bytes(entry.model_dump(mode="json"))
                    if len(body) > _ENTRY_LIMIT:
                        raise PermissionError("minute projection entry exceeds bounded identity capacity")
                    self.verify_current()
                    from rquant.runtime_read_interrupt import ReadInterruptedError, read_interrupt_requested
                    if read_interrupt_requested():
                        raise ReadInterruptedError("minute projection stop before authenticated commit")
                    db.execute("INSERT INTO projection_entry VALUES(?,?,?,?)", (str(certificate.binding.job_id), entry.payload_sha256,
                        body, _mac(key, "entry", entry.model_dump(mode="json"))))
                    updated = dict(generation, generation=generation["generation"] + 1,
                        previous=canonical_sha256(generation), commit=canonical_sha256(entry.model_dump(mode="json")))
                    db.execute("UPDATE projection_meta SET body_json=?,body_mac=? WHERE id=1", (canonical_json_bytes(updated), _mac(key, "generation", updated)))
            finally:
                os.close(lock)
        self.verify_current()
        return True

    @contextmanager
    def _state_transaction(self) -> Iterator[tuple[sqlite3.Connection, bytes, dict[str, object]]]:
        if not self.writable:
            raise PermissionError("minute projection reads cannot reconcile")
        key = self._verified_key()
        with _directory(self.authority.state_directory.path, self.authority.state_directory) as parent:
            lock = os.open("publish.lock", os.O_RDONLY | os.O_NOFOLLOW, dir_fd=parent)
            try:
                value = os.fstat(lock)
                if not stat.S_ISREG(value.st_mode) or value.st_uid != os.getuid() or stat.S_IMODE(value.st_mode) != 0o600 or value.st_nlink != 1:
                    raise PermissionError("minute publication lock is not private")
                fcntl.flock(lock, fcntl.LOCK_EX)
                with self._connection(write=True) as db:
                    yield db, key, self._generation(db, key)
            finally:
                os.close(lock)
        self.verify_current()

    def _save_generation(self, db: sqlite3.Connection, key: bytes, prior: dict[str, object],
        *, commit: object, **changes: object) -> dict[str, object]:
        updated = dict(prior, **changes)
        updated.update(generation=prior["generation"] + 1, previous=canonical_sha256(prior), commit=canonical_sha256(commit))
        encoded = canonical_json_bytes(updated)
        if len(encoded) > MAX_MINUTE_CONTROL_BYTES:
            raise PermissionError("minute projection reconciliation control exceeds original capacity")
        db.execute("UPDATE projection_meta SET body_json=?,body_mac=? WHERE id=1", (encoded, _mac(key, "generation", updated)))
        return updated

    def _pending(self, db: sqlite3.Connection, key: bytes) -> list[dict[str, object]]:
        from rquant.strict_json import strict_json_loads
        rows = db.execute("SELECT job_id,last_attempt,body_json,body_mac FROM projection_pending ORDER BY last_attempt,job_id LIMIT 8").fetchall()
        result = []
        for job_id, attempt, data, signature in rows:
            if len(data) > _ENTRY_LIMIT:
                raise PermissionError("minute projection retry identity is oversized")
            body = strict_json_loads(data)
            if not isinstance(body, dict) or not hmac.compare_digest(signature, _mac(key, "pending", body)) or (
                body.get("job_id"), body.get("last_attempt"), body.get("authority_hash")) != (job_id, attempt, self.reference.content_sha256):
                raise PermissionError("minute projection pending authentication differs")
            result.append(body)
        return result

    def reconcile_one(self, *, as_of: datetime) -> MinuteStudyProjectionReconcileResult:
        from rquant.lab_jobs import JobStatus, LabJobListFilters
        from rquant.runtime_read_interrupt import ReadInterruptedError, read_interrupt_requested

        if read_interrupt_requested():
            raise ReadInterruptedError("minute projection stop before reconciliation")
        key = self._verified_key()
        with self._connection() as db:
            state = self._generation(db, key)
            pending = self._pending(db, key)
        # A persisted retry is attempted on restart before moving another page.
        # Further failed units remain durable rows while new pages continue fairly.
        if not pending or state.get("scan_next", False):
            page = self.installation.reader.list_jobs(filters=LabJobListFilters(statuses=(JobStatus.SUCCEEDED,)),
                limit=8, cursor=state["cursor"])
            if page.next_cursor is not None and len(page.next_cursor.encode()) > 4096:
                raise PermissionError("minute original reconciliation cursor exceeds control capacity")
            with self._state_transaction() as (db, key, prior):
                if prior["cursor"] != state["cursor"]:
                    raise PermissionError("minute projection cursor changed during original page read")
                enqueued = []
                for item in page.items:
                    if self._read_entry(db, key, item.job_id) is not None:
                        continue
                    body = {"job_id": str(item.job_id), "last_attempt": 0, "authority_hash": self.reference.content_sha256}
                    db.execute("INSERT OR IGNORE INTO projection_pending VALUES(?,?,?,?)", (
                        body["job_id"], 0, canonical_json_bytes(body), _mac(key, "pending", body)))
                    enqueued.append(body)
                state = self._save_generation(db, key, prior, commit=enqueued, cursor=page.next_cursor,
                    round=prior["round"] + (page.next_cursor is None), scan_next=False)
                pending = self._pending(db, key)
        if not pending:
            return MinuteStudyProjectionReconcileResult(sequence=state["generation"], candidate_job_id=None, published=False, pending_jobs=0)
        candidate = UUID(pending[0]["job_id"])
        with self._state_transaction() as (db, key, prior):
            body = dict(pending[0], last_attempt=prior["generation"] + 1)
            db.execute("UPDATE projection_pending SET last_attempt=?,body_json=?,body_mac=? WHERE job_id=?", (
                body["last_attempt"], canonical_json_bytes(body), _mac(key, "pending", body), str(candidate)))
            # No catch-and-delete: failure/stop leaves this authenticated unit ready
            # for a later fair retry; cursors never discard an unrecorded unit.
            state = self._save_generation(db, key, prior, commit=body, scan_next=bool(pending[0]["last_attempt"]))
        published = self.materialize_original(candidate, as_of=as_of)
        if read_interrupt_requested():
            raise ReadInterruptedError("minute projection stop after materialization")
        with self._state_transaction() as (db, key, prior):
            db.execute("DELETE FROM projection_pending WHERE job_id=?", (str(candidate),))
            state = self._save_generation(db, key, prior, commit={"completed": str(candidate)}, scan_next=True)
            pending = self._pending(db, key)
        return MinuteStudyProjectionReconcileResult(sequence=state["generation"], candidate_job_id=candidate,
            published=published, pending_jobs=len(pending))

    @minute_parameter_validation_request
    def materialize_original(self, job_id: UUID, *, as_of: datetime) -> bool:
        from rquant.runtime_contracts import normalize_aware_utc
        from rquant.runtime_read_interrupt import ReadInterruptedError, read_interrupt_requested

        if not self.writable:
            raise PermissionError("minute projection reads cannot materialize")
        self.verify_current()
        if read_interrupt_requested():
            raise ReadInterruptedError("minute projection stop before original indexed read")
        visible_at = normalize_aware_utc(as_of)
        authority = self.installation.reader.get_artifact_preview_authority(job_id)
        if authority is None or authority.evidence.indexed_at > visible_at or authority.job.updated_at > visible_at:
            self.verify_current()
            return False
        if authority.job.spec.parameters.strategy_name != "minute_parameter_replay":
            self.verify_current()
            return False
        return self._materialize_indexed(authority, as_of=visible_at)

    def _materialize_indexed(self, authority: LabArtifactPreviewAuthority, *, as_of: datetime) -> bool:
        from rquant.lab_artifact_preview import ArtifactCompleteTableBudget, ArtifactPreviewReader
        from rquant.minute_backtest_artifact import MINUTE_RESULT_TABLE_NAMES
        from rquant.minute_backtest_parameter_artifact import MinuteParameterSealedReplayReader
        from rquant.minute_backtest_parameter_study_execution import project_minute_parameter_study_sealed_windows
        from rquant.runtime_read_interrupt import ReadInterruptedError, read_interrupt_requested

        before = projection_complete_semantic_fingerprint()
        spec = authority.job.spec
        selected = MinuteParameterSealedReplayReader._parameter_model().model_validate(
            {item.name: item.value for item in spec.parameters.arguments})
        from rquant.minute_backtest_parameters import MinuteParameterSet
        input_before = (None if self.installation.profile.parameter_catalog is None else
            _minute_study_input_semantic_fingerprint(
                MinuteParameterSet.model_validate_json(selected.parameter_set_json), producer_commit=spec.code_sha))
        artifacts = ArtifactPreviewReader(reader=self.installation.reader,
            artifact_root=self.installation.authority.final_artifact_root)
        reader = MinuteParameterSealedReplayReader(reader=self.installation.reader, artifact_reader=artifacts,
            submission_facade=self.installation.parameter_submission_facade(spec), catalog=self.installation.profile.parameter_catalog)
        sealed = reader.read(authority.job.job_id, owner_id=selected.owner_id,
            native_id=selected.native_strategy_id, native_version=selected.native_strategy_version, as_of=as_of)
        if sealed is None:
            return False
        study_binding = sealed.result.replay.study_binding
        if study_binding is None:
            return False
        if read_interrupt_requested():
            raise ReadInterruptedError("minute projection stop after complete original validation")
        windows = project_minute_parameter_study_sealed_windows(sealed, as_of=as_of)
        if read_interrupt_requested():
            raise ReadInterruptedError("minute projection stop after original window derivation")
        budget = sealed.result.publication.frozen.result_budget
        evidence = artifacts.read_complete_byte_evidence(sealed.job_id, table_names=MINUTE_RESULT_TABLE_NAMES,
            budget=ArtifactCompleteTableBudget(max_table_count=budget.table_count,
                max_table_bytes=budget.table_bytes, max_total_bytes=budget.total_bytes))
        if evidence.authority != authority or evidence.spec != spec or evidence.manifest != sealed.manifest:
            raise PermissionError("minute original indexed seal changed during derivation")
        binding = MinuteStudyProjectionBinding(owner_id=sealed.owner_id, job_id=sealed.job_id, shard_id=sealed.shard_id,
            spec_hash=sealed.spec_hash, plan_hash=sealed.plan_hash, payload_hash=sealed.payload_hash,
            manifest_hash=sealed.manifest_hash, complete_result_hash=sealed.complete_result_hash,
            result_hash=sealed.result_hash, full_input_hash=sealed.full_input_hash, core_input_hash=sealed.core_input_hash,
            seed_hash=sealed.seed_hash, profile_hash=sealed.result.replay.profile_hash,
            parameter_hash=sealed.result.replay.parameters.fingerprint, study_binding_hash=canonical_sha256(study_binding.model_dump(mode="json")),
            publication_hash=canonical_sha256(sealed.result.publication.model_dump(mode="json")),
            formal_plan_id=sealed.formal_plan.plan_id, completed_at=sealed.completed_at)
        after = projection_complete_semantic_fingerprint()
        if before != after:
            raise PermissionError("minute complete live result/derivation semantics changed during materialization")
        input_verification = None
        if input_before is not None and selected.prepared_publication_json is not None:
            from rquant.minute_backtest_parameter_adapter import _decode_prepared

            shard, = self.installation.reader.list_shards(sealed.job_id)
            input_verification = _capture_minute_study_input_verification(
                catalog=self.installation.profile.parameter_catalog,
                prepared=_decode_prepared(selected.prepared_publication_json), publication=sealed.result.publication,
                parameters=selected, definitions=reader.submission_facade.definition_registry, shard=shard, as_of=as_of)
            if input_verification is not None and input_verification.input_semantic_fingerprint != input_before:
                raise PermissionError("minute complete input semantics changed across original sealed verification")
        certificate = MinuteStudyProjectionCertificate(binding=binding, training=windows.training,
            validation=windows.validation, independent_test=windows.independent_test,
            original_table_bytes=evidence.encoded_table_bytes,
            complete_wire_bytes=len(sealed.model_dump_json().encode()), algorithm_fingerprint=projection_algorithm_fingerprint(),
            original_byte_fingerprint=_byte_fingerprint(evidence), complete_semantic_fingerprint=after,
            input_verification=input_verification)
        self.verify_current()
        if self.installation.reader.get_artifact_preview_authority(sealed.job_id) != authority:
            raise PermissionError("minute original authority changed before derived publication")
        if read_interrupt_requested():
            raise ReadInterruptedError("minute projection stop before derived publication")
        return self._publish_verified(certificate)


def load_minute_study_projection(installation: InstalledMinuteReplay, authority_path: Path, *,
    expected_sha256: str, writable: bool = False) -> InstalledMinuteStudyProjection:
    data, reference = _secure_private_bytes(authority_path)
    if len(data) > MAX_MINUTE_CONTROL_BYTES or reference.content_sha256 != expected_sha256:
        raise PermissionError("minute projection authority is not the privately configured identity")
    authority = strict_model_validate_json(MinuteStudyProjectionAuthority, data)
    value = InstalledMinuteStudyProjection(installation, authority, reference, writable=writable)
    value.verify_current()
    with _directory(authority.state_directory.path, authority.state_directory), _directory(authority.payload_directory.path, authority.payload_directory):
        pass
    return value


def _byte_fingerprint(evidence: ArtifactCompleteByteEvidence) -> str:
    return canonical_sha256({"authority": evidence.authority.model_dump(mode="json"),
        "manifest": evidence.manifest.model_dump(mode="json"), "spec": evidence.spec.model_dump(mode="json"),
        "files": tuple(item.model_dump(mode="json") for item in evidence.file_identities),
        "tables": tuple(item.model_dump(mode="json") for item in evidence.tables),
        "encoded_table_bytes": evidence.encoded_table_bytes, "verified_bundle_bytes": evidence.verified_bundle_bytes})
