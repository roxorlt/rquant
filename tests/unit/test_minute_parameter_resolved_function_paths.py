"""Immutable function paths retain all live checks on a real resolved unit."""

from __future__ import annotations

import sys
from collections.abc import Iterator
from contextlib import ExitStack, contextmanager
from types import FrameType, SimpleNamespace
from typing import cast

import pytest

from rquant import minute_backtest_parameter_contracts as contracts
from rquant import minute_backtest_parameter_producer as producer
from rquant.executable_dependencies import ExecutableDependencyError
from rquant.minute_backtest_parameter_definition import minute_parameter_validation_scope
from tests.unit.test_minute_parameter_study_projection import resolved_original_source  # noqa: F401


@contextmanager
def parsed_global_path_calls() -> Iterator[list[int]]:
    target = contracts._parameter_parsed_global_paths.__code__
    previous = sys.getprofile()
    calls = [0]

    def observe(frame: FrameType, event: str, arg: object) -> None:
        if event == "call" and frame.f_code is target:
            calls[0] += 1

    sys.setprofile(observe)
    try:
        yield calls
    finally:
        sys.setprofile(previous)


def require_original_receipt(value: object, expected: object) -> None:
    if value != expected:
        raise AssertionError("the complete resolved receipt differs from the original fixture")


def test_two_real_resolves_use_zero_function_path_parses_and_independent_copies(
    resolved_original_source: object, request: pytest.FixtureRequest,
) -> None:
    value = cast(SimpleNamespace, resolved_original_source)
    previous = sys.getprofile()
    with minute_parameter_validation_scope():
        with producer.resolved_minute_parameter_read_unit(value.catalog, value.carrier) as unit:
            assert unit is not None, "the original complete fixture did not retain a real unit"
            with parsed_global_path_calls() as calls:
                first = unit.resolve(value.catalog, value.carrier)
                second = unit.resolve(value.catalog, value.carrier)
            assert sys.getprofile() is previous
            require_original_receipt(first, value.published.receipt)
            require_original_receipt(second, value.published.receipt)
            assert first is not second and first is not value.published.receipt
            assert first.frozen is not second.frozen
            assert first.frozen.runtime is not second.frozen.runtime
            assert first.frozen.runtime.materials[0] is not second.frozen.runtime.materials[0]
            object.__setattr__(first.frozen.runtime, "owner_id", "changed-return-copy-only")
            require_original_receipt(second, value.published.receipt)
            require_original_receipt(unit._content.expected, value.published.receipt)
            request.node.user_properties.append(("resolved_calls_observed", 2))
            request.node.user_properties.append(("parameter_parsed_global_paths_calls", calls[0]))
            assert calls[0] == 0, "immutable function reference paths were parsed again during resolve"
    assert sys.getprofile() is previous


@pytest.mark.parametrize("change", ("used_global", "nested_default", "nested_closure", "generator_code"))
def test_actual_live_function_changes_reject_and_restored_original_exit_passes(
    resolved_original_source: object, monkeypatch: pytest.MonkeyPatch, change: str,
) -> None:
    value = cast(SimpleNamespace, resolved_original_source)
    generator = producer.MinuteParameterReplayCatalog._metadata_gate.__wrapped__
    default = {"outer": [{"leaf": "original"}]}
    closure = {"outer": [{"leaf": "original"}]}

    def nested_default_factory() -> object:
        return closure

    with monkeypatch.context() as defaults:
        if change == "nested_default":
            defaults.setattr(generator, "__defaults__", (default,))
        elif change == "nested_closure":
            defaults.setattr(generator, "__defaults__", (nested_default_factory,))
        with minute_parameter_validation_scope():
            with producer.resolved_minute_parameter_read_unit(value.catalog, value.carrier) as unit:
                assert unit is not None, "the complete real fixture did not retain its current policy"
                try:
                    with monkeypatch.context() as live:
                        if change == "used_global":
                            live.setattr(producer, "MAX_INPUT_BYTES", producer.MAX_INPUT_BYTES + 1)
                        elif change == "generator_code":
                            code = generator.__code__
                            live.setattr(generator, "__code__", code.replace(co_firstlineno=code.co_firstlineno + 1))
                        elif change == "nested_default":
                            default["outer"][0]["leaf"] = "changed"
                        else:
                            closure["outer"][0]["leaf"] = "changed"
                        with pytest.raises((PermissionError, ExecutableDependencyError)):
                            unit.resolve(value.catalog, value.carrier)
                finally:
                    default["outer"][0]["leaf"] = "original"
                    closure["outer"][0]["leaf"] = "original"
                require_original_receipt(unit.resolve(value.catalog, value.carrier), value.published.receipt)
            with pytest.raises(PermissionError, match="closed"):
                unit.resolve(value.catalog, value.carrier)


@pytest.mark.parametrize("full", ("bytes", "slots"))
def test_public_unit_falls_back_at_original_byte_or_combined_slot_limit(
    resolved_original_source: object, full: str,
) -> None:
    value = cast(SimpleNamespace, resolved_original_source)
    assert contracts._MAX_PARAMETER_CONTENT_BYTES == 16 * 1024 * 1024
    assert contracts._MAX_PARAMETER_CONTENT_ENTRIES == 8
    with minute_parameter_validation_scope(), ExitStack() as held:
        if full == "bytes":
            assert held.enter_context(
                contracts._parameter_read_unit_retention(retained_bytes=16 * 1024 * 1024),
            ) is True
        else:
            for _ in range(8):
                assert held.enter_context(contracts._parameter_read_unit_retention(retained_bytes=0)) is True
        state = contracts._PARAMETER_CONTENT_STATE.get()
        assert state is not None
        before = (state.retained_bytes, state.resolved_read_unit_bytes, state.resolved_read_unit_count)
        with producer.resolved_minute_parameter_read_unit(value.catalog, value.carrier) as unit:
            assert unit is None
        assert (state.retained_bytes, state.resolved_read_unit_bytes, state.resolved_read_unit_count) == before
    assert state.retained_bytes == state.resolved_read_unit_count == state.resolved_read_unit_bytes == 0


def test_real_unit_fits_last_combined_slot_and_original_shared_byte_limit(
    resolved_original_source: object,
) -> None:
    value = cast(SimpleNamespace, resolved_original_source)
    with minute_parameter_validation_scope(), ExitStack() as held:
        for _ in range(7):
            assert held.enter_context(contracts._parameter_read_unit_retention(retained_bytes=0)) is True
        state = contracts._PARAMETER_CONTENT_STATE.get()
        assert state is not None
        with producer.resolved_minute_parameter_read_unit(value.catalog, value.carrier) as unit:
            assert unit is not None, "the original supported unit lost the eighth shared slot"
            require_original_receipt(unit.resolve(value.catalog, value.carrier), value.published.receipt)
            assert len(state.entries) + state.resolved_read_unit_count == 8
            assert state.resolved_read_unit_bytes >= unit.retained_bytes > 0
            assert state.retained_bytes <= 16 * 1024 * 1024
            assert state.retained_bytes == (
                state.policy_bytes + sum(entry.retained_bytes for entry in state.entries.values())
                + state.resolved_read_unit_bytes
            )
        assert state.resolved_read_unit_count == 7
        assert state.resolved_read_unit_bytes == 0
    assert state.retained_bytes == state.resolved_read_unit_count == 0


def test_exception_closes_only_real_unit_and_keeps_prior_content_and_reservation(
    resolved_original_source: object,
) -> None:
    value = cast(SimpleNamespace, resolved_original_source)
    with minute_parameter_validation_scope():
        original = value.published.receipt.frozen.runtime.content
        state = contracts._PARAMETER_CONTENT_STATE.get()
        assert state is not None and state.entries
        entries = dict(state.entries)
        with contracts._parameter_read_unit_retention(retained_bytes=97) as held:
            assert held is True
            with pytest.raises(RuntimeError, match="trial failed"):
                with producer.resolved_minute_parameter_read_unit(value.catalog, value.carrier) as unit:
                    assert unit is not None, "the supported real unit failed to retain alongside prior content"
                    require_original_receipt(unit.resolve(value.catalog, value.carrier), value.published.receipt)
                    assert state.resolved_read_unit_count == 2
                    assert state.retained_bytes <= 16 * 1024 * 1024
                    assert len(state.entries) + state.resolved_read_unit_count <= 8
                    raise RuntimeError("trial failed")
            assert state.entries == entries
            assert state.resolved_read_unit_count == 1
            assert state.resolved_read_unit_bytes == 97
            assert state.retained_bytes == (
                state.policy_bytes + sum(entry.retained_bytes for entry in state.entries.values()) + 97
            )
            require_original_receipt(value.published.receipt.frozen.runtime.content, original)
            with pytest.raises(PermissionError, match="closed"):
                unit.resolve(value.catalog, value.carrier)
        assert state.resolved_read_unit_bytes == state.resolved_read_unit_count == 0
    assert state.retained_bytes == 0 and contracts._PARAMETER_CONTENT_STATE.get() is None
