"""Resolved read units share the original request's content retention budget."""

from __future__ import annotations

import sys
from collections.abc import Iterator
from contextlib import AbstractContextManager, ExitStack, contextmanager
from contextvars import Context
from types import FrameType

import pytest

from rquant import minute_backtest_parameter_contracts as contracts
from rquant.minute_backtest_parameter_definition import (
    minute_parameter_validation_plan,
    minute_parameter_validation_scope,
)
from rquant.minute_backtest_parameters import MinuteNShapeParameters, MinuteParameterSet


def _content_entries(
    *, control_bytes: int = 0,
) -> AbstractContextManager[contracts._ParameterReadUnitContentEntries | None]:
    method = getattr(contracts, "_parameter_read_unit_content_entries", None)
    assert callable(method), "trial-owned content entry lifetime is unavailable"
    return method(control_bytes=control_bytes)


def test_trial_content_entries_without_owner_do_not_allocate() -> None:
    with _content_entries() as lease:
        assert lease is None
        assert contracts._PARAMETER_CONTENT_STATE.get() is None


def test_trial_content_entries_release_real_additions_and_preserve_old_entries(
    frozen_runtime: contracts.FrozenMinuteParameterInput,
) -> None:
    with minute_parameter_validation_scope():
        expected = frozen_runtime.content
        state = contracts._PARAMETER_CONTENT_STATE.get()
        before_entries, before_fee = dict(state.entries), state.retained_bytes
        with contracts._parameter_read_unit_retention(retained_bytes=73) as retained:
            assert retained
            with _content_entries(control_bytes=97) as lease:
                assert lease is not None
                assert state.resolved_read_unit_count == 1
                control_fee = state.resolved_read_unit_bytes - 73
                assert control_fee >= 97
                for version in (2, 3):
                    assert frozen_runtime.model_copy(update={"source_version": version}).content.source_version == version
                assert len(state.entries) == len(before_entries) + 2
                assert len(state.entries) + state.resolved_read_unit_count <= 8
                lease.release_new_content()
                assert state.entries == before_entries
                assert state.retained_bytes == before_fee + 73 + control_fee
                assert frozen_runtime.content == expected
            assert state.retained_bytes == before_fee + 73
            assert state.resolved_read_unit_count == 1
        assert state.retained_bytes == before_fee
        assert state.resolved_read_unit_bytes == state.resolved_read_unit_count == 0


def test_trial_content_entries_nested_and_exception_cleanup_keep_parent_additions(
    frozen_runtime: contracts.FrozenMinuteParameterInput,
) -> None:
    with minute_parameter_validation_scope():
        frozen_runtime.content
        state = contracts._PARAMETER_CONTENT_STATE.get()
        baseline, before_fee = dict(state.entries), state.retained_bytes
        with _content_entries():
            frozen_runtime.model_copy(update={"source_version": 2}).content
            outer, outer_fee = dict(state.entries), state.retained_bytes
            with pytest.raises(KeyboardInterrupt):
                with _content_entries():
                    frozen_runtime.model_copy(update={"source_version": 3}).content
                    raise KeyboardInterrupt()
            assert state.entries == outer
            assert state.retained_bytes == outer_fee
        assert state.entries == baseline
        assert state.retained_bytes == before_fee


def test_trial_content_entries_tiny_budget_has_original_unretained_behavior(monkeypatch: pytest.MonkeyPatch) -> None:
    with contracts._minute_parameter_content_scope():
        state = contracts._PARAMETER_CONTENT_STATE.get()
        monkeypatch.setattr(contracts, "_MAX_PARAMETER_CONTENT_BYTES", 1)
        with _content_entries(control_bytes=2) as lease:
            assert lease is None
            assert state.retained_bytes == state.resolved_read_unit_bytes == 0
            assert state.resolved_read_unit_count == 0


def test_trial_content_entries_changed_context_and_closed_lease_cannot_release_owner() -> None:
    with contracts._minute_parameter_content_scope():
        state = contracts._PARAMETER_CONTENT_STATE.get()
        with _content_entries() as lease:
            assert lease is not None
            before_fee = state.retained_bytes
            Context().run(lease.release_new_content)
            assert state.retained_bytes == before_fee
        assert state.retained_bytes == 0
        lease.release_new_content()
        assert state.retained_bytes == 0


@pytest.fixture(scope="module")
def frozen_runtime(tmp_path_factory: pytest.TempPathFactory) -> contracts.FrozenMinuteParameterInput:
    from tests.support.minute_parameter_runtime_fixture import parameter_runtime_fixture

    parameters = MinuteParameterSet(parameters=MinuteNShapeParameters(freq="60min"))
    with minute_parameter_validation_scope():
        value, _ = parameter_runtime_fixture(
            tmp_path_factory.mktemp("resolved-capacity-synthetic-runtime") / "runtime", parameters,
        )
    return value


@contextmanager
def content_derivations() -> Iterator[list[None]]:
    watched = contracts.MinuteParameterRuntimeContent.complete_parameter_input.__code__
    calls: list[None] = []
    previous = sys.getprofile()

    def observe(frame: FrameType, event: str, arg: object) -> None:
        if event == "call" and frame.f_code is watched:
            calls.append(None)

    sys.setprofile(observe)
    try:
        yield calls
    finally:
        sys.setprofile(previous)


def test_without_content_scope_never_reserves() -> None:
    assert contracts._parameter_read_unit_capacity() is None
    with contracts._parameter_read_unit_retention(retained_bytes=1) as retained:
        assert retained is False
        assert contracts._PARAMETER_CONTENT_STATE.get() is None
    assert contracts._PARAMETER_CONTENT_STATE.get() is None


def test_original_limits_and_nested_scope_are_shared() -> None:
    assert contracts._MAX_PARAMETER_CONTENT_ENTRIES == 8
    assert contracts._MAX_PARAMETER_CONTENT_BYTES == 16 * 1024 * 1024
    with contracts._minute_parameter_content_scope():
        state = contracts._PARAMETER_CONTENT_STATE.get()
        assert state is not None
        assert contracts._parameter_read_unit_capacity() == 16 * 1024 * 1024
        with contracts._parameter_read_unit_retention(retained_bytes=73) as retained:
            assert retained is True
            with contracts._minute_parameter_content_scope():
                assert contracts._PARAMETER_CONTENT_STATE.get() is state
                assert state.resolved_read_unit_count == 1
                assert state.resolved_read_unit_bytes == state.retained_bytes == 73
            assert contracts._parameter_read_unit_capacity() == 16 * 1024 * 1024 - 73
        assert state.resolved_read_unit_count == state.resolved_read_unit_bytes == 0
        assert state.retained_bytes == 0


@pytest.mark.parametrize("retained_bytes,expected", [
    (0, True), (16 * 1024 * 1024 - 1, True), (16 * 1024 * 1024, True),
    (16 * 1024 * 1024 + 1, False), (-1, False),
])
def test_exact_byte_boundary_and_invalid_charge_degrade(
    retained_bytes: int, expected: bool,
) -> None:
    with contracts._minute_parameter_content_scope():
        state = contracts._PARAMETER_CONTENT_STATE.get()
        assert state is not None
        with contracts._parameter_read_unit_retention(retained_bytes=retained_bytes) as retained:
            assert retained is expected
            assert state.retained_bytes == (retained_bytes if expected else 0)
            assert state.resolved_read_unit_bytes == state.retained_bytes
            assert state.resolved_read_unit_count == int(expected)
            assert contracts._parameter_read_unit_capacity() == (
                16 * 1024 * 1024 - state.retained_bytes
            )
        assert state.retained_bytes == state.resolved_read_unit_bytes == 0
        assert state.resolved_read_unit_count == 0


def test_zero_byte_units_still_consume_all_eight_slots() -> None:
    with contracts._minute_parameter_content_scope(), ExitStack() as units:
        state = contracts._PARAMETER_CONTENT_STATE.get()
        assert state is not None
        for _ in range(8):
            assert units.enter_context(
                contracts._parameter_read_unit_retention(retained_bytes=0),
            ) is True
        assert state.resolved_read_unit_count == 8
        assert state.retained_bytes == 0
        assert contracts._parameter_read_unit_capacity() is None
        with contracts._parameter_read_unit_retention(retained_bytes=0) as retained:
            assert retained is False
            assert state.resolved_read_unit_count == 8
    assert state.resolved_read_unit_count == state.retained_bytes == 0


@pytest.mark.parametrize("changed", ["bytes", "slots"])
def test_reservation_rechecks_capacity_when_entered(changed: str) -> None:
    with contracts._minute_parameter_content_scope(), ExitStack() as units:
        state = contracts._PARAMETER_CONTENT_STATE.get()
        assert state is not None
        assert contracts._parameter_read_unit_capacity() == 16 * 1024 * 1024
        delayed = contracts._parameter_read_unit_retention(retained_bytes=1)
        if changed == "bytes":
            assert units.enter_context(
                contracts._parameter_read_unit_retention(retained_bytes=16 * 1024 * 1024),
            ) is True
        else:
            for _ in range(8):
                assert units.enter_context(
                    contracts._parameter_read_unit_retention(retained_bytes=0),
                ) is True
        before = (state.retained_bytes, state.resolved_read_unit_bytes, state.resolved_read_unit_count)
        with delayed as retained:
            assert retained is False
        assert (state.retained_bytes, state.resolved_read_unit_bytes, state.resolved_read_unit_count) == before


def test_unit_slot_blocks_ninth_content_then_released_slot_can_be_adopted(
    frozen_runtime: contracts.FrozenMinuteParameterInput,
) -> None:
    with minute_parameter_validation_scope():
        state = contracts._PARAMETER_CONTENT_STATE.get()
        assert state is not None
        with contracts._parameter_read_unit_retention(retained_bytes=17) as retained:
            assert retained is True
            for version in range(1, 9):
                value = frozen_runtime.model_copy(update={"source_version": version})
                assert value.content.source_version == version
            assert len(state.entries) == 7
            assert state.resolved_read_unit_count == 1
            with content_derivations() as calls:
                assert value.content.source_version == 8
                assert value.content.source_version == 8
            assert len(calls) == 2
        with content_derivations() as calls:
            first, second = value.content, value.content
        assert len(calls) == 1
        assert first == second and first is not second
        assert len(state.entries) == 8
        assert state.resolved_read_unit_count == 0
        assert contracts._parameter_read_unit_capacity() is None


def test_eight_real_content_entries_block_read_unit_retention(
    frozen_runtime: contracts.FrozenMinuteParameterInput,
) -> None:
    with minute_parameter_validation_scope():
        for version in range(1, 9):
            assert frozen_runtime.model_copy(update={"source_version": version}).content.source_version == version
        state = contracts._PARAMETER_CONTENT_STATE.get()
        assert state is not None and len(state.entries) == 8
        before = state.retained_bytes
        assert contracts._parameter_read_unit_capacity() is None
        with contracts._parameter_read_unit_retention(retained_bytes=0) as retained:
            assert retained is False
        assert state.retained_bytes == before
        assert state.resolved_read_unit_count == state.resolved_read_unit_bytes == 0


def test_full_unit_byte_charge_keeps_real_content_legal_without_adoption(
    frozen_runtime: contracts.FrozenMinuteParameterInput,
) -> None:
    expected = frozen_runtime.content
    with minute_parameter_validation_scope():
        state = contracts._PARAMETER_CONTENT_STATE.get()
        assert state is not None
        with contracts._parameter_read_unit_retention(retained_bytes=16 * 1024 * 1024) as retained:
            assert retained is True
            with content_derivations() as calls:
                first, second = frozen_runtime.content, frozen_runtime.content
            assert first == second == expected and first is not second
            assert len(calls) == 2
            assert state.entries == {}
            assert state.policy_bytes == state.validation_plan_bytes == 0
            assert state.retained_bytes == state.resolved_read_unit_bytes == 16 * 1024 * 1024
            assert contracts._parameter_read_unit_capacity() == 0
        assert state.retained_bytes == 0


def test_remaining_bytes_include_actual_content_policy_and_validation_plan(
    frozen_runtime: contracts.FrozenMinuteParameterInput,
) -> None:
    with minute_parameter_validation_scope():
        first = frozen_runtime.content
        state = contracts._PARAMETER_CONTENT_STATE.get()
        assert state is not None
        before = state.retained_bytes
        assert state.policy_bytes > 0 and state.validation_plan_bytes > 0
        assert before == state.policy_bytes + sum(entry.retained_bytes for entry in state.entries.values())
        remaining = contracts._parameter_read_unit_capacity()
        assert remaining == 16 * 1024 * 1024 - before
        with contracts._parameter_read_unit_retention(retained_bytes=remaining + 1) as retained:
            assert retained is False
        assert state.retained_bytes == before
        with contracts._parameter_read_unit_retention(retained_bytes=remaining) as retained:
            assert retained is True
            assert state.retained_bytes == 16 * 1024 * 1024
            other = frozen_runtime.model_copy(update={"source_version": 2})
            with content_derivations() as calls:
                assert other.content.source_version == 2
                assert other.content.source_version == 2
            assert len(calls) == 2
            assert len(state.entries) == 1
        assert state.retained_bytes == before
        assert frozen_runtime.content == first


def test_exception_releases_only_each_unit_and_preserves_new_content_and_guards(
    frozen_runtime: contracts.FrozenMinuteParameterInput,
) -> None:
    with minute_parameter_validation_scope():
        state = contracts._PARAMETER_CONTENT_STATE.get()
        assert state is not None
        with pytest.raises(RuntimeError, match="outer failed"):
            with contracts._parameter_read_unit_retention(retained_bytes=123) as outer:
                assert outer is True
                with pytest.raises(RuntimeError, match="inner failed"):
                    with contracts._parameter_read_unit_retention(retained_bytes=789) as inner:
                        assert inner is True
                        content = frozen_runtime.content
                        plan = minute_parameter_validation_plan(
                            frozen_runtime.parameters, producer_commit=frozen_runtime.producer_commit,
                        )
                        assert plan is not None
                        assert state.resolved_read_unit_count == 2
                        assert state.resolved_read_unit_bytes == 912
                        raise RuntimeError("inner failed")
                assert state.resolved_read_unit_count == 1
                assert state.resolved_read_unit_bytes == 123
                pure_bytes = state.policy_bytes + sum(entry.retained_bytes for entry in state.entries.values())
                assert pure_bytes > 0 and state.validation_plan_bytes > 0
                assert state.retained_bytes == pure_bytes + 123
                raise RuntimeError("outer failed")
        assert state.retained_bytes == pure_bytes
        assert state.resolved_read_unit_count == state.resolved_read_unit_bytes == 0
        assert state.guards and state.entries and state.validation_plan_bytes > 0
        assert frozen_runtime.content == content
        assert minute_parameter_validation_plan(
            frozen_runtime.parameters, producer_commit=frozen_runtime.producer_commit,
        ) == plan


def test_scope_exit_clears_live_reservation_and_late_finally_cannot_charge_new_scope() -> None:
    with contracts._minute_parameter_content_scope():
        state = contracts._PARAMETER_CONTENT_STATE.get()
        assert state is not None
        unit = contracts._parameter_read_unit_retention(retained_bytes=91)
        assert unit.__enter__() is True
    assert contracts._PARAMETER_CONTENT_STATE.get() is None
    assert state.retained_bytes == state.policy_bytes == state.validation_plan_bytes == 0
    assert state.resolved_read_unit_count == state.resolved_read_unit_bytes == 0
    with contracts._minute_parameter_content_scope():
        current = contracts._PARAMETER_CONTENT_STATE.get()
        assert current is not None and current is not state
        with contracts._parameter_read_unit_retention(retained_bytes=37) as retained:
            assert retained is True
            unit.__exit__(None, None, None)
            assert current.resolved_read_unit_count == 1
            assert current.resolved_read_unit_bytes == current.retained_bytes == 37
            assert state.retained_bytes == state.resolved_read_unit_bytes == 0
    assert current.retained_bytes == current.resolved_read_unit_count == 0


def test_independent_context_has_no_borrowed_capacity_or_reservation() -> None:
    with contracts._minute_parameter_content_scope():
        state = contracts._PARAMETER_CONTENT_STATE.get()
        assert state is not None
        with contracts._parameter_read_unit_retention(retained_bytes=16 * 1024 * 1024) as retained:
            assert retained is True

            def independent_request() -> None:
                assert contracts._parameter_read_unit_capacity() is None
                with contracts._minute_parameter_content_scope():
                    current = contracts._PARAMETER_CONTENT_STATE.get()
                    assert current is not None and current is not state
                    assert contracts._parameter_read_unit_capacity() == 16 * 1024 * 1024
                    with contracts._parameter_read_unit_retention(retained_bytes=31) as own:
                        assert own is True
                        assert current.resolved_read_unit_bytes == current.retained_bytes == 31
                assert current.retained_bytes == current.resolved_read_unit_count == 0

            Context().run(independent_request)
            assert state.resolved_read_unit_bytes == state.retained_bytes == 16 * 1024 * 1024
            assert state.resolved_read_unit_count == 1
