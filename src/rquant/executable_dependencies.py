"""Deterministic fingerprints for the dependencies executable code actually reads."""

from __future__ import annotations

import ast
import builtins
import dis
import hashlib
import inspect
import json
import math
import re
import sys
import textwrap
from collections.abc import Callable, Mapping
from dataclasses import dataclass, fields, is_dataclass
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
from enum import Enum
from types import CellType, CodeType, GetSetDescriptorType, MemberDescriptorType, ModuleType
from zoneinfo import ZoneInfo

from pydantic import BaseModel


class ExecutableDependencyError(TypeError):
    """An executable dependency cannot be fingerprinted safely and deterministically."""


@dataclass(frozen=True, slots=True)
class DependencyFingerprintLimits:
    max_nodes: int = 8_192
    max_depth: int = 64
    max_bytes: int = 1024 * 1024


@dataclass(frozen=True, slots=True)
class ExecutableBinding:
    owner_module: ModuleType
    binding_path: tuple[str, ...]
    implementation: Callable[..., object]

    @classmethod
    def from_callable(cls, implementation: Callable[..., object]) -> ExecutableBinding:
        if not inspect.isfunction(implementation):
            raise ExecutableDependencyError(
                "executable root must be a resolvable top-level Python function"
            )
        module_name = implementation.__module__
        qualified_name = implementation.__qualname__
        owner_module = sys.modules.get(module_name)
        if (
            not isinstance(owner_module, ModuleType)
            or not qualified_name
            or "<locals>" in qualified_name
        ):
            raise ExecutableDependencyError(
                "executable root must be a resolvable top-level Python function"
            )
        binding = cls(
            owner_module=owner_module,
            binding_path=tuple(qualified_name.split(".")),
            implementation=implementation,
        )
        if binding.resolve() is not implementation:
            raise ExecutableDependencyError("executable root module binding is not current")
        return binding

    @property
    def owner_module_name(self) -> str:
        return self.owner_module.__name__

    def resolve(self) -> object:
        if sys.modules.get(self.owner_module_name) is not self.owner_module:
            raise ExecutableDependencyError(
                f"executable module binding changed: {self.owner_module_name}"
            )
        value: object = self.owner_module
        for name in self.binding_path:
            try:
                value = inspect.getattr_static(value, name)
            except AttributeError as exc:
                path = ".".join((self.owner_module_name, *self.binding_path))
                raise ExecutableDependencyError(
                    f"executable binding is unavailable: {path}"
                ) from exc
            if isinstance(value, (staticmethod, classmethod)):
                value = value.__func__
        return value


@dataclass(frozen=True, slots=True)
class _StaticBoundMethodDependency:
    owner: object
    function: object


@dataclass(frozen=True, slots=True)
class ExecutableDependencyGuard:
    bindings: tuple[ExecutableBinding, ...]
    contract: str
    fingerprint: str
    limits: DependencyFingerprintLimits
    include_global_dependencies: bool
    binding_probes: tuple[_CapturedBindingProbe, ...]
    function_probes: tuple[_CapturedFunctionProbe, ...]

    def current_fingerprint(self) -> str:
        return fingerprint_executable_bindings(
            self.bindings,
            contract=self.contract,
            limits=self.limits,
            include_global_dependencies=self.include_global_dependencies,
        )

    def assert_unchanged(self) -> None:
        for binding in self.bindings:
            if binding.resolve() is not binding.implementation:
                raise ExecutableDependencyError("executable dependency fingerprint changed")
        for probe in self.binding_probes:
            probe.assert_unchanged()
        for probe in self.function_probes:
            probe.assert_unchanged()


@dataclass(frozen=True, slots=True)
class _CapturedBindingProbe:
    owner_module: ModuleType
    binding_name: str
    attribute_path: tuple[str, ...]
    expected_value: object
    content_fingerprint: str | None
    identity_required: bool
    recursive_package_roots: frozenset[str]
    limits: DependencyFingerprintLimits

    @property
    def key(self) -> tuple[str, str, tuple[str, ...]]:
        return (self.owner_module.__name__, self.binding_name, self.attribute_path)

    def assert_unchanged(self) -> None:
        if sys.modules.get(self.owner_module.__name__) is not self.owner_module:
            raise ExecutableDependencyError("executable dependency fingerprint changed")
        try:
            root = inspect.getattr_static(self.owner_module, self.binding_name)
            resolved_path, current = _resolve_dependency_attribute_path(
                root,
                self.attribute_path,
            )
        except ExecutableDependencyError:
            raise
        except AttributeError as exc:
            raise ExecutableDependencyError("executable dependency fingerprint changed") from exc
        if resolved_path != self.attribute_path:
            raise ExecutableDependencyError("executable dependency fingerprint changed")
        if self.identity_required and not _same_binding_identity(
            current,
            self.expected_value,
        ):
            raise ExecutableDependencyError("executable dependency fingerprint changed")
        if self.content_fingerprint is not None and (
            _fingerprint_probe_content(
                current,
                recursive_package_roots=self.recursive_package_roots,
                limits=self.limits,
            )
            != self.content_fingerprint
        ):
            raise ExecutableDependencyError("executable dependency fingerprint changed")


@dataclass(frozen=True, slots=True)
class _CapturedFunctionProbe:
    implementation: Callable[..., object]
    code: CodeType
    defaults: tuple[object, ...] | None
    defaults_fingerprint: str | None
    keyword_defaults: dict[str, object] | None
    keyword_defaults_fingerprint: str | None
    closure: tuple[CellType, ...] | None
    closure_fingerprints: tuple[str, ...] | None
    recursive_package_roots: frozenset[str]
    limits: DependencyFingerprintLimits

    def assert_unchanged(self) -> None:
        if (
            self.implementation.__code__ is not self.code
            or self.implementation.__defaults__ is not self.defaults
            or self.implementation.__kwdefaults__ is not self.keyword_defaults
            or self.implementation.__closure__ is not self.closure
        ):
            raise ExecutableDependencyError("executable dependency fingerprint changed")
        if self.defaults_fingerprint is not None and (
            _fingerprint_probe_content(
                self.defaults,
                recursive_package_roots=self.recursive_package_roots,
                limits=self.limits,
            )
            != self.defaults_fingerprint
        ):
            raise ExecutableDependencyError("executable dependency fingerprint changed")
        if self.keyword_defaults_fingerprint is not None and (
            _fingerprint_probe_content(
                self.keyword_defaults,
                recursive_package_roots=self.recursive_package_roots,
                limits=self.limits,
            )
            != self.keyword_defaults_fingerprint
        ):
            raise ExecutableDependencyError("executable dependency fingerprint changed")
        if self.closure_fingerprints is not None:
            if self.closure is None or len(self.closure) != len(self.closure_fingerprints):
                raise ExecutableDependencyError("executable dependency fingerprint changed")
            current = tuple(
                _fingerprint_probe_content(
                    _closure_cell_value(cell),
                    recursive_package_roots=self.recursive_package_roots,
                    limits=self.limits,
                )
                for cell in self.closure
            )
            if current != self.closure_fingerprints:
                raise ExecutableDependencyError("executable dependency fingerprint changed")


@dataclass(slots=True)
class _Budget:
    limits: DependencyFingerprintLimits
    nodes: int = 0
    bytes_used: int = 0

    def consume(self, *, depth: int, byte_count: int = 0) -> None:
        if depth > self.limits.max_depth:
            raise ExecutableDependencyError("executable dependency depth budget exceeded")
        self.nodes += 1
        if self.nodes > self.limits.max_nodes:
            raise ExecutableDependencyError("executable dependency node budget exceeded")
        self.bytes_used += byte_count
        if self.bytes_used > self.limits.max_bytes:
            raise ExecutableDependencyError("executable dependency byte budget exceeded")


@dataclass(slots=True)
class _SnapshotState:
    budget: _Budget
    require_root_source: bool
    graph: dict[str, object]
    functions: dict[str, Callable[..., object]]
    visiting_functions: set[str]
    recursive_package_roots: frozenset[str]
    include_global_dependencies: bool
    binding_probes: dict[tuple[str, str, tuple[str, ...]], _CapturedBindingProbe]
    function_probes: dict[str, _CapturedFunctionProbe]


def capture_executable_dependency_guard(
    bindings: tuple[ExecutableBinding, ...],
    *,
    contract: str,
    limits: DependencyFingerprintLimits | None = None,
    include_global_dependencies: bool = True,
) -> ExecutableDependencyGuard:
    selected_limits = limits or DependencyFingerprintLimits()
    normalized = _normalize_bindings(bindings)
    fingerprint, state = _capture_executable_bindings(
        normalized,
        contract=contract,
        limits=selected_limits,
        include_global_dependencies=include_global_dependencies,
    )
    return ExecutableDependencyGuard(
        bindings=normalized,
        contract=contract,
        fingerprint=fingerprint,
        limits=selected_limits,
        include_global_dependencies=include_global_dependencies,
        binding_probes=tuple(state.binding_probes[key] for key in sorted(state.binding_probes)),
        function_probes=tuple(state.function_probes[key] for key in sorted(state.function_probes)),
    )


def fingerprint_callable(
    implementation: Callable[..., object],
    *,
    implementation_version: str,
    contract: str,
    require_source: bool,
    limits: DependencyFingerprintLimits | None = None,
) -> str:
    binding = ExecutableBinding.from_callable(implementation)
    return fingerprint_executable_bindings(
        (binding,),
        contract=contract,
        implementation_version=implementation_version,
        require_root_source=require_source,
        limits=limits,
    )


def fingerprint_executable_bindings(
    bindings: tuple[ExecutableBinding, ...],
    *,
    contract: str,
    implementation_version: str | None = None,
    require_root_source: bool = False,
    limits: DependencyFingerprintLimits | None = None,
    include_global_dependencies: bool = True,
) -> str:
    return _capture_executable_bindings(
        bindings,
        contract=contract,
        implementation_version=implementation_version,
        require_root_source=require_root_source,
        limits=limits,
        include_global_dependencies=include_global_dependencies,
    )[0]


def _capture_executable_bindings(
    bindings: tuple[ExecutableBinding, ...],
    *,
    contract: str,
    implementation_version: str | None = None,
    require_root_source: bool = False,
    limits: DependencyFingerprintLimits | None = None,
    include_global_dependencies: bool = True,
) -> tuple[str, _SnapshotState]:
    if not contract or contract != contract.strip():
        raise ExecutableDependencyError("executable fingerprint contract is invalid")
    if implementation_version is not None and (
        not implementation_version or implementation_version != implementation_version.strip()
    ):
        raise ExecutableDependencyError("executable implementation version is invalid")
    selected_limits = limits or DependencyFingerprintLimits()
    state = _SnapshotState(
        budget=_Budget(selected_limits),
        require_root_source=require_root_source,
        graph={},
        functions={},
        visiting_functions=set(),
        recursive_package_roots=frozenset(
            binding.owner_module_name.partition(".")[0] for binding in bindings
        ),
        include_global_dependencies=include_global_dependencies,
        binding_probes={},
        function_probes={},
    )
    roots: list[dict[str, object]] = []
    for binding in _normalize_bindings(bindings):
        current = binding.resolve()
        if not inspect.isfunction(current):
            path = ".".join((binding.owner_module_name, *binding.binding_path))
            raise ExecutableDependencyError(f"executable root is not a Python function: {path}")
        roots.append(
            {
                "owner_module": binding.owner_module_name,
                "binding_path": binding.binding_path,
                "implementation": _snapshot_function(
                    current,
                    state=state,
                    depth=1,
                    require_source=require_root_source,
                ),
            }
        )
    payload: dict[str, object] = {
        "contract": contract,
        "implementation_version": implementation_version,
        "roots": roots,
        "function_graph": {key: state.graph[key] for key in sorted(state.graph)},
    }
    if not include_global_dependencies:
        payload["dependency_scope"] = "explicit_roots_and_closure"
    encoded = _canonical_bytes(payload)
    if len(encoded) > selected_limits.max_bytes:
        raise ExecutableDependencyError("executable dependency byte budget exceeded")
    return hashlib.sha256(encoded).hexdigest(), state


def fingerprint_dependency_value(
    value: object,
    *,
    contract: str,
    limits: DependencyFingerprintLimits | None = None,
    _recursive_package_roots: frozenset[str] | None = None,
) -> str:
    if not contract or contract != contract.strip():
        raise ExecutableDependencyError("dependency value fingerprint contract is invalid")
    selected_limits = limits or DependencyFingerprintLimits()
    state = _SnapshotState(
        budget=_Budget(selected_limits),
        require_root_source=False,
        graph={},
        functions={},
        visiting_functions=set(),
        recursive_package_roots=(
            _recursive_package_roots
            if _recursive_package_roots is not None
            else frozenset({_dependency_module_name(value).partition(".")[0]})
        ),
        include_global_dependencies=True,
        binding_probes={},
        function_probes={},
    )
    payload = {
        "contract": contract,
        "value": _snapshot_dependency(
            value,
            state=state,
            depth=1,
            active=frozenset(),
        ),
        "function_graph": {key: state.graph[key] for key in sorted(state.graph)},
    }
    encoded = _canonical_bytes(payload)
    if len(encoded) > selected_limits.max_bytes:
        raise ExecutableDependencyError("executable dependency byte budget exceeded")
    return hashlib.sha256(encoded).hexdigest()


def _normalize_bindings(
    bindings: tuple[ExecutableBinding, ...],
) -> tuple[ExecutableBinding, ...]:
    if not bindings:
        raise ExecutableDependencyError("executable roots cannot be empty")
    by_path: dict[tuple[str, tuple[str, ...]], ExecutableBinding] = {}
    for binding in bindings:
        if not binding.binding_path or any(not name for name in binding.binding_path):
            raise ExecutableDependencyError("executable binding path is invalid")
        key = (binding.owner_module_name, binding.binding_path)
        existing = by_path.get(key)
        if existing is not None and existing.implementation is not binding.implementation:
            raise ExecutableDependencyError("ambiguous executable root binding")
        by_path[key] = binding
    return tuple(by_path[key] for key in sorted(by_path))


def _snapshot_function(
    value: Callable[..., object],
    *,
    state: _SnapshotState,
    depth: int,
    require_source: bool,
) -> dict[str, str]:
    if not inspect.isfunction(value):
        raise ExecutableDependencyError("Python function dependency is not inspectable")
    identity = _callable_identity(value)
    existing = state.functions.get(identity)
    if existing is not None:
        if existing is not value:
            raise ExecutableDependencyError(
                f"ambiguous Python function identity in dependency graph: {identity}"
            )
        return {"function_ref": identity}
    state.budget.consume(depth=depth, byte_count=len(identity.encode("utf-8")))
    state.functions[identity] = value
    state.visiting_functions.add(identity)
    state.graph[identity] = {"state": "visiting"}
    try:
        normalized_ast: str | None = None
        if require_source:
            try:
                source = textwrap.dedent(inspect.getsource(value))
                normalized_ast = ast.dump(
                    ast.parse(source),
                    annotate_fields=True,
                    include_attributes=False,
                )
                state.budget.consume(
                    depth=depth + 1,
                    byte_count=len(normalized_ast.encode("utf-8")),
                )
            except (OSError, TypeError, IndentationError, SyntaxError) as exc:
                raise ExecutableDependencyError(
                    f"executable source is unavailable: {identity}"
                ) from exc
        closure_cells = value.__closure__ or ()
        if len(closure_cells) != len(value.__code__.co_freevars):
            raise ExecutableDependencyError(
                f"executable closure bindings are unavailable: {identity}"
            )
        dependencies = (
            _snapshot_function_dependencies(
                value,
                state=state,
                depth=depth + 1,
            )
            if state.include_global_dependencies
            else {}
        )
        node: dict[str, object] = {
            "identity": identity,
            "normalized_ast": normalized_ast,
            "code": _snapshot_code(value.__code__, state=state, depth=depth + 1),
            "defaults": _snapshot_value(
                value.__defaults__,
                state=state,
                depth=depth + 1,
                active=frozenset(),
            ),
            "keyword_defaults": _snapshot_value(
                value.__kwdefaults__,
                state=state,
                depth=depth + 1,
                active=frozenset(),
            ),
            "dependencies": dependencies,
        }
        if closure_cells:
            node["closure"] = {
                name: _snapshot_dependency(
                    _closure_cell_value(cell),
                    state=state,
                    depth=depth + 1,
                    active=frozenset(),
                )
                for name, cell in zip(
                    value.__code__.co_freevars,
                    closure_cells,
                    strict=True,
                )
            }
        state.graph[identity] = node
        _record_function_probe(state, value)
        return {"function_ref": identity}
    finally:
        state.visiting_functions.discard(identity)


def _snapshot_function_dependencies(
    function: Callable[..., object],
    *,
    state: _SnapshotState,
    depth: int,
) -> dict[str, object]:
    paths = _referenced_global_paths(function.__code__)
    dependency_module = sys.modules.get(str(function.__globals__.get("__name__", "")))
    if not isinstance(dependency_module, ModuleType):
        raise ExecutableDependencyError(
            f"executable global module is unavailable: {_callable_identity(function)}"
        )
    dependencies: dict[str, object] = {}
    for name in sorted(paths):
        if name in function.__globals__:
            root = function.__globals__[name]
            owner = dependency_module
        else:
            try:
                root = inspect.getattr_static(builtins, name)
            except AttributeError as exc:
                raise ExecutableDependencyError(
                    f"executable global binding is unavailable: {dependency_module.__name__}.{name}"
                ) from exc
            owner = builtins
        referenced_paths = paths[name]
        follows_attribute_bindings = isinstance(root, (ModuleType, type)) or (
            any(referenced_paths)
            and () not in referenced_paths
            and not isinstance(
                root,
                (BaseModel, Mapping, list, tuple, set, frozenset),
            )
            and not (is_dataclass(root) and not isinstance(root, type))
        )
        if follows_attribute_bindings and any(referenced_paths):
            attribute_values: dict[tuple[str, ...], object] = {}
            for attribute_path in sorted(path for path in referenced_paths if path):
                bound_path, endpoint = _resolve_dependency_attribute_path(
                    root,
                    attribute_path,
                )
                attribute_values.setdefault(bound_path, endpoint)
                _record_binding_probe(
                    state,
                    owner_module=owner,
                    binding_name=name,
                    attribute_path=bound_path,
                    value=endpoint,
                )
            entries = [
                {
                    "attribute_path": attribute_path,
                    "value": _snapshot_dependency(
                        endpoint,
                        state=state,
                        depth=depth + len(attribute_path),
                        active=frozenset(),
                    ),
                }
                for attribute_path, endpoint in sorted(attribute_values.items())
            ]
            if () in referenced_paths:
                _record_binding_probe(
                    state,
                    owner_module=owner,
                    binding_name=name,
                    attribute_path=(),
                    value=root,
                )
                entries.append(
                    {
                        "attribute_path": (),
                        "value": (
                            {"module_binding": root.__name__}
                            if isinstance(root, ModuleType)
                            else _snapshot_dependency(
                                root,
                                state=state,
                                depth=depth,
                                active=frozenset(),
                            )
                        ),
                    }
                )
            dependency_value: object = {"attribute_bindings": entries}
        else:
            _record_binding_probe(
                state,
                owner_module=owner,
                binding_name=name,
                attribute_path=(),
                value=root,
            )
            if isinstance(root, ModuleType):
                dependency_value = {"module_binding": root.__name__}
            else:
                dependency_value = _snapshot_dependency(
                    root,
                    state=state,
                    depth=depth,
                    active=frozenset(),
                )
        dependencies[name] = {
            "owner_module": owner.__name__,
            "binding_name": name,
            "value": dependency_value,
        }
    return dependencies


def _referenced_global_paths(code: CodeType) -> dict[str, set[tuple[str, ...]]]:
    result: dict[str, set[tuple[str, ...]]] = {}
    pending = [code]
    while pending:
        current = pending.pop()
        instructions = tuple(dis.get_instructions(current, adaptive=False, show_caches=False))
        for index, instruction in enumerate(instructions):
            if instruction.opname not in {"LOAD_GLOBAL", "LOAD_NAME"}:
                continue
            name = instruction.argval
            if not isinstance(name, str):
                raise ExecutableDependencyError("executable global bytecode name is invalid")
            attribute_path: list[str] = []
            for following in instructions[index + 1 :]:
                if following.opname not in {"LOAD_ATTR", "LOAD_METHOD"}:
                    break
                attribute = following.argval
                if not isinstance(attribute, str):
                    raise ExecutableDependencyError("executable attribute bytecode name is invalid")
                attribute_path.append(attribute)
            result.setdefault(name, set()).add(tuple(attribute_path))
        pending.extend(constant for constant in current.co_consts if isinstance(constant, CodeType))
    return result


def _resolve_dependency_attribute_path(
    root: object,
    path: tuple[str, ...],
) -> tuple[tuple[str, ...], object]:
    value = root
    for index, name in enumerate(path):
        try:
            attribute = inspect.getattr_static(value, name)
        except AttributeError as exc:
            raise ExecutableDependencyError(
                f"executable attribute binding is unavailable: {name}"
            ) from exc
        if isinstance(attribute, staticmethod):
            value = attribute.__func__
        elif isinstance(attribute, classmethod):
            value = _StaticBoundMethodDependency(
                owner=value if isinstance(value, type) else type(value),
                function=attribute.__func__,
            )
        elif (
            inspect.isfunction(attribute)
            and not isinstance(value, (ModuleType, type))
            or inspect.ismethoddescriptor(attribute)
            and not isinstance(value, type)
        ):
            value = _StaticBoundMethodDependency(owner=value, function=attribute)
        elif isinstance(attribute, MemberDescriptorType) and not isinstance(value, type):
            try:
                value = attribute.__get__(value, type(value))
            except AttributeError as exc:
                raise ExecutableDependencyError(
                    f"executable attribute binding is unavailable: {name}"
                ) from exc
        elif isinstance(attribute, (property, GetSetDescriptorType)) or (
            inspect.getattr_static(type(attribute), "__get__", None) is not None
            and not inspect.isfunction(attribute)
            and not inspect.ismethoddescriptor(attribute)
        ):
            raise ExecutableDependencyError(f"executable attribute descriptor is unsafe: {name}")
        else:
            value = attribute
        if isinstance(value, _StaticBoundMethodDependency) and index != len(path) - 1:
            raise ExecutableDependencyError(
                f"executable attribute chain after method binding is unsafe: {name}"
            )
    return path, value


def _static_metadata_text(value: object, name: str) -> str | None:
    try:
        attribute = inspect.getattr_static(value, name)
    except AttributeError:
        return None
    if isinstance(attribute, str):
        return attribute
    if isinstance(attribute, (property, GetSetDescriptorType, MemberDescriptorType)) or (
        inspect.getattr_static(type(attribute), "__get__", None) is not None
    ):
        raise ExecutableDependencyError(
            f"executable callable metadata descriptor is unsafe: {name}"
        )
    return None


def _static_data_attribute(value: object, name: str) -> object:
    try:
        attribute = inspect.getattr_static(value, name)
    except AttributeError as exc:
        raise ExecutableDependencyError(
            f"executable dependency attribute is unavailable: {name}"
        ) from exc
    if isinstance(attribute, MemberDescriptorType):
        try:
            return attribute.__get__(value, type(value))
        except AttributeError as exc:
            raise ExecutableDependencyError(
                f"executable dependency attribute is unavailable: {name}"
            ) from exc
    if isinstance(attribute, (property, GetSetDescriptorType)) or (
        inspect.getattr_static(type(attribute), "__get__", None) is not None
    ):
        raise ExecutableDependencyError(
            f"executable dependency attribute descriptor is unsafe: {name}"
        )
    return attribute


def _record_binding_probe(
    state: _SnapshotState,
    *,
    owner_module: ModuleType,
    binding_name: str,
    attribute_path: tuple[str, ...],
    value: object,
) -> None:
    identity_required = True
    content_fingerprint = (
        _fingerprint_probe_content(
            value,
            recursive_package_roots=state.recursive_package_roots,
            limits=state.budget.limits,
        )
        if _requires_content_probe(value)
        else None
    )
    probe = _CapturedBindingProbe(
        owner_module=owner_module,
        binding_name=binding_name,
        attribute_path=attribute_path,
        expected_value=value,
        content_fingerprint=content_fingerprint,
        identity_required=identity_required,
        recursive_package_roots=state.recursive_package_roots,
        limits=state.budget.limits,
    )
    existing = state.binding_probes.get(probe.key)
    if (
        existing is not None
        and not _same_binding_identity(existing.expected_value, value)
        and (
            existing.identity_required
            or probe.identity_required
            or existing.content_fingerprint != probe.content_fingerprint
        )
    ):
        raise ExecutableDependencyError("ambiguous executable dependency binding")
    state.binding_probes[probe.key] = probe


def _requires_content_probe(value: object) -> bool:
    return isinstance(
        value,
        (_StaticBoundMethodDependency, BaseModel, Mapping, list, tuple, set, frozenset),
    ) or (is_dataclass(value) and not isinstance(value, type))


def _same_binding_identity(current: object, expected: object) -> bool:
    if isinstance(current, _StaticBoundMethodDependency) and isinstance(
        expected,
        _StaticBoundMethodDependency,
    ):
        return current.owner is expected.owner and current.function is expected.function
    if inspect.ismethod(current) and inspect.ismethod(expected):
        return current.__self__ is expected.__self__ and current.__func__ is expected.__func__
    return current is expected


def _fingerprint_probe_content(
    value: object,
    *,
    recursive_package_roots: frozenset[str],
    limits: DependencyFingerprintLimits,
) -> str:
    return fingerprint_dependency_value(
        value,
        contract="executable-dependency-content-probe/v1",
        limits=limits,
        _recursive_package_roots=recursive_package_roots,
    )


def _record_function_probe(
    state: _SnapshotState,
    value: Callable[..., object],
) -> None:
    identity = _callable_identity(value)
    closure = value.__closure__
    probe = _CapturedFunctionProbe(
        implementation=value,
        code=value.__code__,
        defaults=value.__defaults__,
        defaults_fingerprint=(
            _fingerprint_probe_content(
                value.__defaults__,
                recursive_package_roots=state.recursive_package_roots,
                limits=state.budget.limits,
            )
            if value.__defaults__ is not None
            else None
        ),
        keyword_defaults=value.__kwdefaults__,
        keyword_defaults_fingerprint=(
            _fingerprint_probe_content(
                value.__kwdefaults__,
                recursive_package_roots=state.recursive_package_roots,
                limits=state.budget.limits,
            )
            if value.__kwdefaults__ is not None
            else None
        ),
        closure=closure,
        closure_fingerprints=(
            tuple(
                _fingerprint_probe_content(
                    _closure_cell_value(cell),
                    recursive_package_roots=state.recursive_package_roots,
                    limits=state.budget.limits,
                )
                for cell in closure
            )
            if closure is not None
            else None
        ),
        recursive_package_roots=state.recursive_package_roots,
        limits=state.budget.limits,
    )
    existing = state.function_probes.get(identity)
    if existing is not None and existing.implementation is not value:
        raise ExecutableDependencyError(
            f"ambiguous Python function identity in dependency graph: {identity}"
        )
    state.function_probes[identity] = probe


def _snapshot_dependency(
    value: object,
    *,
    state: _SnapshotState,
    depth: int,
    active: frozenset[int],
) -> object:
    if isinstance(value, _StaticBoundMethodDependency):
        owner = value.owner
        return {
            "bound_method": {
                "owner": _snapshot_bound_method_owner(
                    owner,
                    state=state,
                    depth=depth + 1,
                    active=active,
                ),
                "function": (
                    _snapshot_function(
                        value.function,
                        state=state,
                        depth=depth + 1,
                        require_source=False,
                    )
                    if inspect.isfunction(value.function)
                    else _snapshot_dependency(
                        value.function,
                        state=state,
                        depth=depth + 1,
                        active=active,
                    )
                ),
            }
        }
    if inspect.isfunction(value):
        module = getattr(value, "__module__", "")
        if module.partition(".")[0] not in state.recursive_package_roots:
            identity = _callable_identity(value)
            state.budget.consume(depth=depth, byte_count=len(identity.encode("utf-8")))
            code_payload = _snapshot_code(value.__code__, state=state, depth=depth + 1)
            _record_function_probe(state, value)
            return {
                "external_python_callable": {
                    "identity": identity,
                    "code_sha256": hashlib.sha256(_canonical_bytes(code_payload)).hexdigest(),
                }
            }
        return _snapshot_function(
            value,
            state=state,
            depth=depth,
            require_source=False,
        )
    if inspect.ismethod(value) and inspect.isfunction(value.__func__):
        owner = value.__self__
        return {
            "bound_method": {
                "owner": _snapshot_bound_method_owner(
                    owner,
                    state=state,
                    depth=depth + 1,
                    active=active,
                ),
                "function": _snapshot_function(
                    value.__func__,
                    state=state,
                    depth=depth + 1,
                    require_source=False,
                ),
            }
        }
    if (
        inspect.isbuiltin(value)
        or inspect.ismethoddescriptor(value)
        or (callable(value) and type(value).__module__ == "builtins")
    ):
        module = getattr(value, "__module__", None)
        qualified_name = getattr(value, "__qualname__", getattr(value, "__name__", None))
        bound_owner = getattr(value, "__self__", None)
        if (not isinstance(module, str) or not module) and isinstance(bound_owner, type):
            module = bound_owner.__module__
        descriptor_owner = getattr(value, "__objclass__", None)
        if (not isinstance(module, str) or not module) and isinstance(descriptor_owner, type):
            module = descriptor_owner.__module__
        if not isinstance(module, str) or not module or not isinstance(qualified_name, str):
            raise ExecutableDependencyError(
                "builtin/C callable identity is unavailable: "
                f"{type(value).__module__}:{type(value).__qualname__}:"
                f"{module}:{qualified_name}"
            )
        state.budget.consume(
            depth=depth,
            byte_count=len(module.encode("utf-8")) + len(qualified_name.encode("utf-8")),
        )
        return {"builtin_callable": {"module": module, "qualname": qualified_name}}
    if isinstance(value, type):
        return {"type": _type_identity(value)}
    if callable(value):
        module = _static_metadata_text(value, "__module__")
        qualified_name = _static_metadata_text(value, "__qualname__")
        if qualified_name is None:
            qualified_name = _static_metadata_text(value, "__name__")
        if isinstance(module, str) and module and isinstance(qualified_name, str):
            state.budget.consume(
                depth=depth,
                byte_count=len(module.encode("utf-8")) + len(qualified_name.encode("utf-8")),
            )
            return {
                "native_callable": {
                    "type": _type_identity(type(value)),
                    "module": module,
                    "qualname": qualified_name,
                }
            }
        raise ExecutableDependencyError(
            "native callable identity is unavailable without executing a descriptor"
        )
    return _snapshot_value(value, state=state, depth=depth, active=active)


def _snapshot_bound_method_owner(
    owner: object,
    *,
    state: _SnapshotState,
    depth: int,
    active: frozenset[int],
) -> object:
    if isinstance(owner, type):
        return {"type": _type_identity(owner)}
    owner_type = type(owner)
    package_name = owner_type.__module__.partition(".")[0]
    package = sys.modules.get(package_name)
    binding_name = owner_type.__name__.casefold()
    if isinstance(package, ModuleType):
        try:
            exported = inspect.getattr_static(package, binding_name)
        except AttributeError:
            pass
        else:
            if exported is owner:
                state.budget.consume(
                    depth=depth,
                    byte_count=len(package_name.encode("utf-8"))
                    + len(binding_name.encode("utf-8")),
                )
                return {
                    "module_singleton": {
                        "module": package_name,
                        "binding": binding_name,
                        "type": _type_identity(owner_type),
                    }
                }
    return _snapshot_value(
        owner,
        state=state,
        depth=depth,
        active=active,
    )


def _snapshot_value(
    value: object,
    *,
    state: _SnapshotState,
    depth: int,
    active: frozenset[int],
) -> object:
    state.budget.consume(depth=depth, byte_count=_scalar_byte_count(value))
    if value is None:
        return {"none": True}
    if type(value) is bool:
        return {"bool": value}
    if type(value) is int:
        return {"int": str(value)}
    if type(value) is str:
        return {"str": value}
    if type(value) is bytes:
        return {"bytes": value.hex()}
    if type(value) is float:
        if not math.isfinite(value):
            raise ExecutableDependencyError("executable dependency numeric values must be finite")
        return {"float": value.hex()}
    if type(value) is complex:
        if not math.isfinite(value.real) or not math.isfinite(value.imag):
            raise ExecutableDependencyError("executable dependency numeric values must be finite")
        return {"complex": (value.real.hex(), value.imag.hex())}
    if isinstance(value, Decimal):
        if not value.is_finite():
            raise ExecutableDependencyError("executable dependency numeric values must be finite")
        decimal_tuple = value.as_tuple()
        return {
            "decimal": {
                "sign": decimal_tuple.sign,
                "digits": decimal_tuple.digits,
                "exponent": decimal_tuple.exponent,
            }
        }
    if isinstance(value, datetime):
        return {"datetime": value.isoformat(timespec="microseconds")}
    if isinstance(value, date):
        return {"date": value.isoformat()}
    if isinstance(value, time):
        return {"time": value.isoformat(timespec="microseconds")}
    if isinstance(value, timedelta):
        return {
            "timedelta": {
                "days": value.days,
                "seconds": value.seconds,
                "microseconds": value.microseconds,
            }
        }
    if isinstance(value, timezone):
        offset = value.utcoffset(None)
        return {
            "timezone": {
                "offset_microseconds": int(offset.total_seconds() * 1_000_000),
                "name": value.tzname(None),
            }
        }
    if isinstance(value, ZoneInfo):
        return {"zoneinfo": value.key}
    if isinstance(value, re.Pattern):
        pattern = value.pattern.hex() if isinstance(value.pattern, bytes) else value.pattern
        return {
            "regex": {
                "pattern_type": type(value.pattern).__name__,
                "pattern": pattern,
                "flags": value.flags,
            }
        }
    if isinstance(value, Enum):
        return {
            "enum": {
                "type": _type_identity(type(value)),
                "name": value.name,
                "value": _snapshot_value(
                    value.value,
                    state=state,
                    depth=depth + 1,
                    active=active,
                ),
            }
        }
    if (
        inspect.isfunction(value)
        or inspect.ismethod(value)
        or inspect.isbuiltin(value)
        or inspect.ismethoddescriptor(value)
        or callable(value)
    ):
        return _snapshot_dependency(value, state=state, depth=depth + 1, active=active)
    if isinstance(value, type):
        return {"type": _type_identity(value)}
    typing_name = inspect.getattr_static(value, "_name", None)
    if type(value).__module__ == "typing" and isinstance(typing_name, str) and typing_name:
        return {"typing": typing_name}
    if type(value).__module__.startswith("pandas.") and type(value).__qualname__ == "NAType":
        return {"singleton": _type_identity(type(value))}
    if isinstance(value, ModuleType):
        return {"module_binding": value.__name__}
    if isinstance(value, ExecutableDependencyGuard):
        return {
            "executable_dependency_guard": {
                "contract": value.contract,
                "fingerprint": value.fingerprint,
                "dependency_scope": (
                    "referenced_globals"
                    if value.include_global_dependencies
                    else "explicit_roots_and_closure"
                ),
                "limits": {
                    "max_nodes": value.limits.max_nodes,
                    "max_depth": value.limits.max_depth,
                    "max_bytes": value.limits.max_bytes,
                },
            }
        }
    identity = id(value)
    if identity in active:
        raise ExecutableDependencyError("executable dependency cycle detected")
    nested_active = active | {identity}
    if isinstance(value, BaseModel):
        return {
            "model": {
                "type": _type_identity(type(value)),
                "fields": _snapshot_value(
                    value.model_dump(mode="python"),
                    state=state,
                    depth=depth + 1,
                    active=nested_active,
                ),
            }
        }
    if is_dataclass(value) and not isinstance(value, type):
        return {
            "dataclass": {
                "type": _type_identity(type(value)),
                "fields": _snapshot_value(
                    {
                        field.name: _static_data_attribute(value, field.name)
                        for field in fields(value)
                    },
                    state=state,
                    depth=depth + 1,
                    active=nested_active,
                ),
            }
        }
    if isinstance(value, tuple):
        return {
            "tuple": [
                _snapshot_value(
                    item,
                    state=state,
                    depth=depth + 1,
                    active=nested_active,
                )
                for item in value
            ]
        }
    if isinstance(value, list):
        return {
            "list": [
                _snapshot_value(
                    item,
                    state=state,
                    depth=depth + 1,
                    active=nested_active,
                )
                for item in value
            ]
        }
    if isinstance(value, (set, frozenset)):
        members = [
            _snapshot_value(
                item,
                state=state,
                depth=depth + 1,
                active=nested_active,
            )
            for item in value
        ]
        return {
            "frozenset" if isinstance(value, frozenset) else "set": _stable_unique_values(
                members,
                label="set",
            )
        }
    if isinstance(value, Mapping):
        try:
            iterator = iter(value.items())
        except Exception as exc:
            raise ExecutableDependencyError(
                "executable mapping dependency changed while fingerprinting"
            ) from exc
        entries: list[tuple[object, object]] = []
        while True:
            try:
                raw_entry = next(iterator)
            except StopIteration:
                break
            except Exception as exc:
                raise ExecutableDependencyError(
                    "executable mapping dependency changed while fingerprinting"
                ) from exc
            try:
                key, item = raw_entry
            except (TypeError, ValueError) as exc:
                raise ExecutableDependencyError(
                    "executable mapping dependency yielded an invalid item"
                ) from exc
            entries.append(
                (
                    _snapshot_value(
                        key,
                        state=state,
                        depth=depth + 1,
                        active=nested_active,
                    ),
                    _snapshot_value(
                        item,
                        state=state,
                        depth=depth + 1,
                        active=nested_active,
                    ),
                )
            )
        return {"mapping": _stable_mapping_entries(entries)}
    instance_state = _snapshot_instance_state(
        value,
        state=state,
        depth=depth,
        active=nested_active,
    )
    if instance_state is not None:
        return instance_state
    raise ExecutableDependencyError(
        f"executable dependency is not canonically fingerprintable: {_type_identity(type(value))}"
    )


def _snapshot_instance_state(
    value: object,
    *,
    state: _SnapshotState,
    depth: int,
    active: frozenset[int],
) -> object | None:
    namespace: dict[str, object] | None = None
    slots: dict[str, object] = {}
    for owner_type in type(value).__mro__:
        owner_namespace = vars(owner_type)
        dictionary_descriptor = owner_namespace.get("__dict__")
        if namespace is None and isinstance(dictionary_descriptor, GetSetDescriptorType):
            observed = dictionary_descriptor.__get__(value, type(value))
            if type(observed) is not dict:
                raise ExecutableDependencyError(
                    "executable dependency instance namespace is unsafe"
                )
            namespace = observed
        for name, descriptor in owner_namespace.items():
            if not isinstance(descriptor, MemberDescriptorType):
                continue
            try:
                slot_value = descriptor.__get__(value, type(value))
            except AttributeError:
                slots[name] = {"unset_slot": True}
            else:
                slots[name] = _snapshot_value(
                    slot_value,
                    state=state,
                    depth=depth + 1,
                    active=active,
                )
    if namespace is None and not slots:
        return None
    return {
        "instance": {
            "type": _type_identity(type(value)),
            "namespace": (
                _snapshot_value(
                    namespace,
                    state=state,
                    depth=depth + 1,
                    active=active,
                )
                if namespace is not None
                else None
            ),
            "slots": slots,
        }
    }


def _snapshot_code(
    code: CodeType,
    *,
    state: _SnapshotState,
    depth: int,
) -> dict[str, object]:
    state.budget.consume(depth=depth, byte_count=len(code.co_code) + len(code.co_exceptiontable))
    instructions = [
        {"opname": instruction.opname, "arg": instruction.arg}
        for instruction in dis.get_instructions(code, adaptive=False, show_caches=False)
    ]
    return {
        "argcount": code.co_argcount,
        "posonlyargcount": code.co_posonlyargcount,
        "kwonlyargcount": code.co_kwonlyargcount,
        "nlocals": code.co_nlocals,
        "stacksize": code.co_stacksize,
        "flags": code.co_flags,
        "instructions": instructions,
        "constants": [
            (
                {"code": _snapshot_code(constant, state=state, depth=depth + 1)}
                if isinstance(constant, CodeType)
                else _snapshot_value(
                    constant,
                    state=state,
                    depth=depth + 1,
                    active=frozenset(),
                )
            )
            for constant in code.co_consts
        ],
        "names": code.co_names,
        "varnames": code.co_varnames,
        "freevars": code.co_freevars,
        "cellvars": code.co_cellvars,
        "exception_table": code.co_exceptiontable.hex(),
    }


def _stable_unique_values(values: list[object], *, label: str) -> list[object]:
    encoded = sorted(
        ((_canonical_bytes(value), value) for value in values),
        key=lambda item: item[0],
    )
    for (left, _), (right, _) in zip(encoded, encoded[1:], strict=False):
        if left == right:
            raise ExecutableDependencyError(
                f"executable dependency {label} has ambiguous canonical members"
            )
    return [value for _, value in encoded]


def _stable_mapping_entries(entries: list[tuple[object, object]]) -> list[object]:
    encoded = sorted(
        ((_canonical_bytes(key), key, value) for key, value in entries),
        key=lambda item: item[0],
    )
    for (left, _, _), (right, _, _) in zip(encoded, encoded[1:], strict=False):
        if left == right:
            raise ExecutableDependencyError(
                "executable mapping dependency has ambiguous canonical keys"
            )
    return [[key, value] for _, key, value in encoded]


def _callable_identity(value: Callable[..., object]) -> str:
    if not inspect.isfunction(value):
        raise ExecutableDependencyError("Python function identity is unavailable")
    module = value.__module__
    qualified_name = value.__qualname__
    if not module or not qualified_name:
        raise ExecutableDependencyError("Python function identity is unavailable")
    return f"{module}:{qualified_name}"


def _closure_cell_value(cell: CellType) -> object:
    try:
        return cell.cell_contents
    except ValueError as exc:
        raise ExecutableDependencyError("executable closure cell is empty") from exc


def _type_identity(value: type[object]) -> str:
    module = value.__module__
    qualified_name = value.__qualname__
    if not module or not qualified_name or "<locals>" in qualified_name:
        raise ExecutableDependencyError("executable dependency type identity is unavailable")
    return f"{module}:{qualified_name}"


def _dependency_module_name(value: object) -> str:
    if inspect.isfunction(value):
        return value.__module__
    if inspect.ismethod(value) and inspect.isfunction(value.__func__):
        return value.__func__.__module__
    return type(value).__module__


def _scalar_byte_count(value: object) -> int:
    if isinstance(value, str):
        return len(value.encode("utf-8"))
    if isinstance(value, bytes):
        return len(value) * 2
    if type(value) is int:
        try:
            return len(str(value))
        except ValueError as exc:
            raise ExecutableDependencyError(
                "executable integer dependency exceeds its byte budget"
            ) from exc
    return 16


def _canonical_bytes(value: object) -> bytes:
    try:
        return json.dumps(
            value,
            ensure_ascii=True,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    except (TypeError, ValueError, RecursionError) as exc:
        raise ExecutableDependencyError("executable dependency payload is not canonical") from exc
