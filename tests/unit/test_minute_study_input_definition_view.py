"""An input proof cannot create an authenticated minute definition view."""

from __future__ import annotations

from contextvars import Context
from pathlib import Path
from threading import Thread
from typing import Any

import pytest

from rquant import definition_registry as definitions
from rquant import minute_backtest_parameter_study_projection as projection


def _capabilities() -> tuple[type[Any], type[Any]]:
    context_type = getattr(projection, "_MinuteStudyVerifiedInputRead", None)
    view_type = getattr(definitions, "_MinuteStudyInputDefinitionView", None)
    assert isinstance(context_type, type) and isinstance(view_type, type), (
        "authenticated input-context and readonly definition-view capability is unavailable"
    )
    return context_type, view_type


def test_direct_context_constructor_cannot_mint_authenticated_read() -> None:
    context_type, _ = _capabilities()
    with pytest.raises(TypeError, match="authenticated projection"):
        context_type()


class _DuckInputRead:
    def __init__(self) -> None:
        self.calls = 0

    def _assert_active(self, **kwargs: object) -> object:
        self.calls += 1
        raise AssertionError("a duck input read cannot supply authentication")


class _DerivedTrustedRegistry(definitions.TrustedExecutableRegistry):
    pass


def _original_registry(root: Path) -> definitions.ImmutableDefinitionRegistry:
    return definitions.ImmutableDefinitionRegistry(
        root,
        execution_registry=definitions.TrustedExecutableRegistry(features=(), strategies=()),
    )


@pytest.mark.parametrize("candidate", ("none", "bare-proof", "duck"))
def test_view_rejects_non_context_values_before_consulting_them(
    tmp_path: Path, candidate: str,
) -> None:
    _, view_type = _capabilities()
    original = _original_registry(tmp_path / "definitions")
    duck = _DuckInputRead()
    value = {
        "none": None,
        "bare-proof": projection._MinuteStudyInputVerification.model_construct(),
        "duck": duck,
    }[candidate]
    with pytest.raises(TypeError, match="actual authenticated input read"):
        view_type(original, _minute_input_read=value)
    assert duck.calls == 0
    assert not original.root.exists()


def test_uninitialized_exact_context_cannot_mint_view(tmp_path: Path) -> None:
    context_type, view_type = _capabilities()
    context = object.__new__(context_type)
    original = _original_registry(tmp_path / "definitions")
    with pytest.raises(PermissionError, match="exact active authenticated scope"):
        view_type(original, _minute_input_read=context)
    assert not original.root.exists()


def test_uninitialized_context_assert_refuses_before_missing_fields() -> None:
    context_type, _ = _capabilities()
    context = object.__new__(context_type)
    with pytest.raises(PermissionError, match="exact active authenticated scope"):
        context._assert_active()
    assert vars(context) == {}


@pytest.mark.parametrize("execution_context", ("empty-context", "foreign-thread"))
def test_uninitialized_context_stays_unprivileged_in_other_execution_contexts(
    tmp_path: Path, execution_context: str,
) -> None:
    context_type, view_type = _capabilities()
    context = object.__new__(context_type)
    original = _original_registry(tmp_path / "definitions")
    failures: list[BaseException] = []

    def reject() -> None:
        try:
            with pytest.raises(PermissionError, match="exact active authenticated scope"):
                context._assert_active()
            with pytest.raises(PermissionError, match="exact active authenticated scope"):
                view_type(original, _minute_input_read=context)
        except BaseException as exc:
            failures.append(exc)

    if execution_context == "empty-context":
        Context().run(reject)
    else:
        thread = Thread(target=reject, name="minute-view-negative-test")
        thread.start()
        try:
            thread.join()
        finally:
            thread.join()
        assert not thread.is_alive()
    assert failures == []
    assert vars(context) == {}
    assert not original.root.exists()


@pytest.mark.parametrize("kind", ("features", "strategies"))
def test_readonly_publish_refuses_before_payload_or_root_access(
    tmp_path: Path, kind: str,
) -> None:
    _, view_type = _capabilities()
    view = object.__new__(view_type)
    model = (
        definitions.FeatureContractRegistration
        if kind == "features" else definitions.StrategySpecRegistration
    )
    # A bare typed record grants no read scope or permission to publish.
    record = model.model_construct()
    with pytest.raises(PermissionError, match="readonly"):
        view._publish(
            kind=kind, logical_id="untrusted-record", version=1,
            fingerprint="0" * 64, record=record, model=model,
        )
    assert vars(view) == {}
    assert tuple(tmp_path.iterdir()) == ()


@pytest.mark.parametrize("candidate", ("none", "duck", "subclass"))
def test_original_registry_still_requires_concrete_trusted_registry(
    tmp_path: Path, candidate: str,
) -> None:
    _capabilities()
    value = {
        "none": None,
        "duck": object(),
        "subclass": _DerivedTrustedRegistry(features=(), strategies=()),
    }[candidate]
    root = tmp_path / "definitions"
    with pytest.raises(TypeError, match="concrete TrustedExecutableRegistry"):
        definitions.ImmutableDefinitionRegistry(root, execution_registry=value)
    assert not root.exists()


@pytest.mark.parametrize("resolver_name", ("feature_binding_resolver", "strategy_binding_resolver"))
def test_original_registry_still_rejects_arbitrary_binding_resolver(
    tmp_path: Path, resolver_name: str,
) -> None:
    _capabilities()
    calls: list[object] = []

    def resolver(value: object) -> object:
        calls.append(value)
        raise AssertionError("an arbitrary resolver must not be called")

    root = tmp_path / "definitions"
    concrete = definitions.TrustedExecutableRegistry(features=(), strategies=())
    with pytest.raises(TypeError, match="arbitrary execution binding resolvers"):
        definitions.ImmutableDefinitionRegistry(
            root, execution_registry=concrete, **{resolver_name: resolver},
        )
    assert calls == []
    assert not root.exists()
