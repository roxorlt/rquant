"""Immutable instruction plans retain the complete live dependency checks."""

from __future__ import annotations

import dis
from dataclasses import dataclass
from types import CodeType, SimpleNamespace
from typing import Any

import pytest

from rquant import executable_dependencies as owner


@dataclass
class _Reader:
    values: list[dict[str, int]]

    def read(self) -> int:
        return self.values[0]["value"]


@dataclass
class _OpaqueConstant:
    value: int


_ROOT_NS = SimpleNamespace(reader=_Reader([{"value": 1}]))
_LIVE_ATTR = SimpleNamespace(data={"nested": [1]})


def _invoke() -> int:
    return _ROOT_NS.reader.read()


def _nested_template() -> Any:
    def nested() -> int:
        return _LIVE_ATTR.data["nested"][0]

    return nested


def _read_method(closure: dict[str, list[int]]) -> Any:
    # Real mutable defaults are required to check each subsequent live read.
    def read(
        self: _Reader,
        default: dict[str, int] = {"value": 1},  # noqa: B006
        *,
        keyword: dict[str, int] = {"value": 1},  # noqa: B006
    ) -> int:
        return (
            self.values[0]["value"] + default["value"] + keyword["value"]
            + closure["nested"][0] + _LIVE_ATTR.data["nested"][0]
        )

    read.__qualname__ = "_Reader.read"
    return read


@pytest.fixture
def probe_source(monkeypatch: pytest.MonkeyPatch) -> tuple[_Reader, dict[str, list[int]]]:
    closure = {"nested": [1]}
    reader = _Reader([{"value": 1}])
    monkeypatch.setattr(_Reader, "read", _read_method(closure))
    monkeypatch.setattr(_ROOT_NS, "reader", reader)
    monkeypatch.setattr(_LIVE_ATTR, "data", {"nested": [1]})
    return reader, closure


def _guard(*, compile_bytes: int | None = None) -> owner.ExecutableDependencyGuard:
    guard = owner.capture_executable_dependency_guard(
        (owner.ExecutableBinding.from_callable(_invoke),
         owner.ExecutableBinding.from_callable(_nested_template)),
        contract="focused-code-plan/v1",
    )
    if compile_bytes is not None and hasattr(guard, "with_compiled_code_plan"):
        return guard.with_compiled_code_plan(max_retained_bytes=compile_bytes)
    return guard


def _fingerprint(
    value: object, *, limits: owner.DependencyFingerprintLimits | None = None,
    plan: object | None = None,
) -> str:
    keywords = {} if plan is None else {"_code_plan": plan}
    return owner.fingerprint_dependency_value(
        value, contract="code-plan-equivalence/v1", limits=limits,
        _recursive_package_roots=frozenset({__name__.partition(".")[0]}),
        **keywords,
    )


def test_real_bound_method_reuse_does_not_reparse_immutable_instructions(
    probe_source: tuple[_Reader, dict[str, list[int]]], monkeypatch: pytest.MonkeyPatch,
) -> None:
    original = dis.get_instructions
    calls: list[CodeType] = []

    def observed(code: CodeType, *args: Any, **kwargs: Any) -> Any:
        calls.append(code)
        return original(code, *args, **kwargs)

    monkeypatch.setattr(dis, "get_instructions", observed)
    guard = _guard(compile_bytes=1024 * 1024)
    calls.clear()
    guard.assert_unchanged()
    guard.assert_unchanged()
    assert calls == []
    assert guard.code_plan_retained_bytes > 0


@pytest.mark.parametrize("compiled", (False, True), ids=("original", "compiled"))
@pytest.mark.parametrize(
    "change", ("binding", "nested_owner", "default", "keyword", "closure", "attr", "code"),
)
def test_each_reuse_reads_actual_live_content(
    probe_source: tuple[_Reader, dict[str, list[int]]], monkeypatch: pytest.MonkeyPatch,
    compiled: bool, change: str,
) -> None:
    reader, closure = probe_source
    guard = _guard(compile_bytes=1024 * 1024 if compiled else None)
    guard.assert_unchanged()
    if change == "binding":
        monkeypatch.setattr(_ROOT_NS, "reader", _Reader([{"value": 1}]))
    elif change == "nested_owner":
        reader.values[0]["value"] = 2
    elif change == "default":
        _Reader.read.__defaults__[0]["value"] = 2
    elif change == "keyword":
        _Reader.read.__kwdefaults__["keyword"]["value"] = 2
    elif change == "closure":
        closure["nested"][0] = 2
    elif change == "attr":
        _LIVE_ATTR.data["nested"][0] = 2
    else:
        monkeypatch.setattr(_Reader.read, "__code__", _Reader.read.__code__.replace())
    with pytest.raises(owner.ExecutableDependencyError):
        guard.assert_unchanged()


@pytest.mark.parametrize("compiled", (False, True), ids=("original", "compiled"))
def test_nested_mutable_code_constants_are_read_on_every_snapshot(
    probe_source: tuple[_Reader, dict[str, list[int]]], monkeypatch: pytest.MonkeyPatch,
    compiled: bool,
) -> None:
    constant = {"nested": [1]}
    code = _Reader.read.__code__.replace(co_consts=(*_Reader.read.__code__.co_consts, constant))
    monkeypatch.setattr(_Reader.read, "__code__", code)
    guard = _guard(compile_bytes=1024 * 1024 if compiled else None)
    guard.assert_unchanged()
    constant["nested"][0] = 2
    with pytest.raises(owner.ExecutableDependencyError):
        guard.assert_unchanged()


def test_compiled_and_original_complete_fingerprints_are_equal(
    probe_source: tuple[_Reader, dict[str, list[int]]],
) -> None:
    reader, _ = probe_source
    guard = _guard(compile_bytes=1024 * 1024)
    plan = getattr(guard, "code_plan", None)
    current = owner._StaticBoundMethodDependency(reader, _Reader.read)
    assert _fingerprint(current) == _fingerprint(current, plan=plan)
    assert guard.current_fingerprint() == guard.fingerprint


def test_recursive_code_snapshot_and_budget_charges_match_original(
    probe_source: tuple[_Reader, dict[str, list[int]]],
) -> None:
    guard = _guard(compile_bytes=1024 * 1024)
    plan = getattr(guard, "code_plan", None)
    results = []
    for selected in (None, plan):
        extra = {} if selected is None else {"code_plan": selected}
        state = owner._SnapshotState(
            budget=owner._Budget(owner.DependencyFingerprintLimits()),
            require_root_source=False, graph={}, functions={}, visiting_functions=set(),
            recursive_package_roots=frozenset({__name__.partition(".")[0]}),
            include_global_dependencies=True, binding_probes={}, function_probes={}, **extra,
        )
        payload = owner._snapshot_code(_nested_template.__code__, state=state, depth=1)
        results.append((payload, state.budget.nodes, state.budget.bytes_used))
    assert results[0] == results[1]
    expected = owner._referenced_global_paths(_nested_template.__code__)
    if plan is not None:
        assert owner._referenced_global_paths(_nested_template.__code__, code_plan=plan) == expected
    assert expected["_LIVE_ATTR"] == {("data",)}


@pytest.mark.parametrize("limit", ("nodes", "depth", "bytes"))
def test_compiled_snapshots_keep_original_budget_failures(
    probe_source: tuple[_Reader, dict[str, list[int]]], limit: str,
) -> None:
    reader, _ = probe_source
    guard = _guard(compile_bytes=1024 * 1024)
    values = {"max_nodes": 8192, "max_depth": 64, "max_bytes": 1024 * 1024}
    values["max_" + limit] = 4
    limits = owner.DependencyFingerprintLimits(**values)
    current = owner._StaticBoundMethodDependency(reader, _Reader.read)
    with pytest.raises(owner.ExecutableDependencyError) as original:
        _fingerprint(current, limits=limits)
    with pytest.raises(owner.ExecutableDependencyError) as compiled:
        _fingerprint(current, limits=limits, plan=getattr(guard, "code_plan", None))
    assert str(compiled.value) == str(original.value)


def test_tiny_retained_allowance_uses_original_path(
    probe_source: tuple[_Reader, dict[str, list[int]]], monkeypatch: pytest.MonkeyPatch,
) -> None:
    original = dis.get_instructions
    calls = []

    def observed(code: CodeType, *args: Any, **kwargs: Any) -> Any:
        calls.append(code)
        return original(code, *args, **kwargs)

    guard = _guard(compile_bytes=1)
    monkeypatch.setattr(dis, "get_instructions", observed)
    guard.assert_unchanged()
    assert calls
    assert getattr(guard, "code_plan_retained_bytes", 0) == 0


def test_partial_plan_and_lowered_allowance_keep_complete_retained_charge_bounded(
    probe_source: tuple[_Reader, dict[str, list[int]]],
) -> None:
    original = _guard()
    complete = original.with_compiled_code_plan(max_retained_bytes=1024 * 1024)
    complete_bytes = complete.code_plan_retained_bytes
    assert 0 < complete_bytes <= 1024 * 1024
    for allowance in (0, 1, complete_bytes - 1, complete_bytes):
        current = original.with_compiled_code_plan(max_retained_bytes=allowance)
        assert 0 <= current.code_plan_retained_bytes <= allowance
        current.assert_unchanged()
        if allowance == complete_bytes - 1:
            assert current.code_plan is not None
            assert len(current.code_plan.descriptors) < len(complete.code_plan.descriptors)
    lowered = complete.with_compiled_code_plan(max_retained_bytes=0)
    assert lowered.code_plan_retained_bytes == 0
    assert all(probe.code_plan is None for probe in lowered.binding_probes)
    assert all(probe.code_plan is None for probe in lowered.function_probes)
    lowered.assert_unchanged()


def test_opaque_code_constant_keeps_original_snapshot_and_mutation_guard(
    probe_source: tuple[_Reader, dict[str, list[int]]], monkeypatch: pytest.MonkeyPatch,
) -> None:
    constant = _OpaqueConstant(1)
    code = _Reader.read.__code__.replace(co_consts=(*_Reader.read.__code__.co_consts, constant))
    monkeypatch.setattr(_Reader.read, "__code__", code)
    guard = _guard(compile_bytes=1024 * 1024)
    plan = getattr(guard, "code_plan", None)
    if plan is not None:
        assert plan.lookup(code) is None
    guard.assert_unchanged()
    constant.value = 2
    with pytest.raises(owner.ExecutableDependencyError):
        guard.assert_unchanged()
