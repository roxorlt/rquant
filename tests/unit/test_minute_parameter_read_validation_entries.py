"""Trial lexical cleanup of new pure validation entries and their original fees."""

from __future__ import annotations

from contextvars import Context
from typing import Any

import pytest

from rquant import minute_backtest_parameter_contracts as ledger
from rquant import minute_backtest_parameter_definition as owner
from rquant.minute_backtest_parameters import MinuteNShapeParameters, MinutePaperParameters, MinuteParameterSet


def _scope() -> Any:
    method = getattr(owner, "_minute_parameter_read_validation_entries", None)
    assert callable(method), "trial-owned validation entry lifetime is unavailable"
    return method()


def _parameters(index: int = 0) -> MinuteParameterSet:
    return MinuteParameterSet(parameters=MinuteNShapeParameters(
        freq="60min", paper=MinutePaperParameters(stop_loss_pct=0.04 + index * 0.01)))


def _plan(index: int = 0) -> owner.MinuteParameterValidationPlan:
    plan = owner.minute_parameter_validation_plan(_parameters(index), producer_commit="a" * 40)
    assert plan is not None
    return plan


def test_no_owner_keeps_original_unretained_path() -> None:
    with _scope() as lease:
        assert lease is None
        assert owner._PARAMETER_VALIDATION_STATE.get() is None
        assert ledger._PARAMETER_CONTENT_STATE.get() is None


def test_actual_distinct_recipe_fees_leave_prior_entries_and_outputs_intact() -> None:
    with owner.minute_parameter_validation_scope():
        expected = _plan()
        initial_entries = dict(owner._PARAMETER_VALIDATION_STATE.get())
        state = ledger._PARAMETER_CONTENT_STATE.get()
        initial_fee = state.retained_bytes
        for index in range(1, 5):
            with _scope() as lease:
                assert lease is not None
                assert state.retained_bytes > initial_fee
                observed = _plan(index)
                entry = owner._PARAMETER_VALIDATION_STATE.get()[(_parameters(index).model_dump_json(), "a" * 40)]
                charge = entry.builder_guard.code_plan_retained_bytes + entry.executable_guard.code_plan_retained_bytes
                assert charge > 0
                assert len(owner._PARAMETER_VALIDATION_STATE.get()) == len(initial_entries) + 1
                assert state.retained_bytes <= ledger._MAX_PARAMETER_CONTENT_BYTES
                assert _plan(index).model_dump_json() == observed.model_dump_json()
            assert owner._PARAMETER_VALIDATION_STATE.get() == initial_entries
            assert state.retained_bytes == state.policy_bytes == state.validation_plan_bytes == initial_fee
            assert state.resolved_read_unit_bytes == state.resolved_read_unit_count == 0
        assert _plan().model_dump_json() == expected.model_dump_json()
    assert state.retained_bytes == 0


@pytest.mark.parametrize("interrupt", [ValueError, KeyboardInterrupt])
def test_exception_and_nested_scope_release_only_their_new_validation_entries(interrupt: type[BaseException]) -> None:
    with owner.minute_parameter_validation_scope():
        _plan()
        entries = owner._PARAMETER_VALIDATION_STATE.get()
        state = ledger._PARAMETER_CONTENT_STATE.get()
        before_entries, before_fee = dict(entries), state.retained_bytes
        with _scope():
            _plan(1)
            outer_entries, outer_fee = dict(entries), state.retained_bytes
            with pytest.raises(interrupt):
                with _scope():
                    _plan(2)
                    raise interrupt()
            assert entries == outer_entries
            assert state.retained_bytes == outer_fee
        assert entries == before_entries
        assert state.retained_bytes == before_fee


def test_different_current_context_never_releases_the_original_owners() -> None:
    with owner.minute_parameter_validation_scope():
        _plan()
        state = ledger._PARAMETER_CONTENT_STATE.get()
        entries = owner._PARAMETER_VALIDATION_STATE.get()
        baseline_entries, baseline_fee = dict(entries), state.retained_bytes
        with _scope() as lease:
            _plan(1)
            owned_entries, owned_fee = dict(entries), state.retained_bytes
            def other_context() -> None:
                lease.release_new_content()
            Context().run(other_context)
            assert entries == owned_entries
            assert state.retained_bytes == owned_fee
        assert entries == baseline_entries
        assert state.retained_bytes == baseline_fee


def test_new_trial_plan_still_detects_live_executable_mutation(monkeypatch: pytest.MonkeyPatch) -> None:
    from rquant.executable_dependencies import ExecutableDependencyError

    with owner.minute_parameter_validation_scope():
        with _scope():
            _plan(1)
            function = owner.build_minute_parameter_definition
            with monkeypatch.context() as patch:
                patch.setattr(function, "__code__", function.__code__.replace(co_firstlineno=function.__code__.co_firstlineno + 1))
                with pytest.raises(ExecutableDependencyError):
                    _plan(1)


def test_different_content_ledger_is_never_charged_for_original_validation_entries() -> None:
    with owner.minute_parameter_validation_scope():
        _plan()
        original_ledger = ledger._PARAMETER_CONTENT_STATE.get()
        context = _scope()
        context.__enter__()
        _plan(1)
        before = original_ledger.retained_bytes
        foreign_ledger = ledger._ParameterContentState()
        token = ledger._PARAMETER_CONTENT_STATE.set(foreign_ledger)
        try:
            context.__exit__(None, None, None)
            assert foreign_ledger.retained_bytes == foreign_ledger.policy_bytes == foreign_ledger.validation_plan_bytes == 0
            assert original_ledger.retained_bytes == before
        finally:
            ledger._PARAMETER_CONTENT_STATE.reset(token)
    assert original_ledger.retained_bytes == 0
