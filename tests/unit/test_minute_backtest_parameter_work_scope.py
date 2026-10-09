"""Request-local pure work reuse over the accepted complete joint91 input."""

from __future__ import annotations

import hashlib
import json
import pstats
from datetime import datetime
from pathlib import Path
from typing import Any

import pytest

from rquant.executable_dependencies import ExecutableDependencyError


@pytest.fixture(scope="module")
def original_work_input() -> tuple[dict[str, Any], object]:
    from rquant.minute_backtest_contracts import MinuteReplayMaterial, MinuteReplayWork
    from rquant.minute_backtest_parameter_contracts import MinuteParameterWork
    from rquant.minute_backtest_parameter_features import MinuteParameterSessionFacts
    from rquant.minute_backtest_parameter_study import MinuteParameterStudyBinding
    from rquant.minute_backtest_parameters import MinuteParameterSet

    path = Path(__file__).parent / "fixtures/minute-parameter-work-joint91.json"
    raw = path.read_bytes()
    assert hashlib.sha256(raw).hexdigest() == "c92c192d7637aea5ef1fefe58c0fd46047fa9f937b5a2f134761559ae6318527"
    value = json.loads(raw)
    assert value["original_complete_json_sha256"] == "f937c2e45fc1fee4984ef298c1c09c0c2e074cc17cac2cdbc1771516cccd5a54"
    fields = value["arguments"]
    arguments = dict(parameters=MinuteParameterSet.model_validate(fields["parameters"]),
        materials=tuple(MinuteReplayMaterial.model_validate(item) for item in fields["materials"]),
        tick_times=tuple(datetime.fromisoformat(item) for item in fields["tick_times"]),
        warmup_available_at=datetime.fromisoformat(fields["warmup_available_at"]),
        runtime_work=MinuteReplayWork.model_validate(fields["runtime_work"]),
        session_facts=tuple(MinuteParameterSessionFacts.model_validate(item) for item in fields["session_facts"]),
        study_binding=MinuteParameterStudyBinding.model_validate(fields["study_binding"]))
    return arguments, MinuteParameterWork.model_validate(value["expected_work"])


def test_complete_work_is_computed_once_and_returns_fresh_original_values(original_work_input: tuple[dict[str, Any], object]) -> None:
    import cProfile
    from rquant import minute_backtest_parameter_source as owner
    from rquant.minute_backtest_parameter_definition import minute_parameter_validation_scope

    arguments, expected = original_work_input
    profile = cProfile.Profile()
    with minute_parameter_validation_scope():
        profile.enable()
        first = owner.measure_minute_parameter_work(**arguments)
        second = owner.measure_minute_parameter_work(**arguments)
        profile.disable()
        assert first == second == expected and first is not second
        assert len(owner._PARAMETER_WORK_STATE.get()) == 1
    calls = sum(row[1] for key, row in pstats.Stats(profile).stats.items()
        if key[2] == "_measure_minute_parameter_work")
    assert calls == 1
    assert owner._PARAMETER_WORK_STATE.get() is None


@pytest.mark.parametrize("changed", ("parameters", "study_binding", "tick_times", "materials"))
def test_complete_input_changes_cannot_reuse_previous_work(original_work_input: tuple[dict[str, Any], object], changed: str) -> None:
    from rquant import minute_backtest_parameter_source as owner
    from rquant.minute_backtest_parameter_definition import minute_parameter_validation_scope

    arguments, expected = original_work_input
    other = dict(arguments)
    if changed == "parameters":
        parameters = arguments["parameters"]
        other[changed] = type(parameters)(parameters=parameters.parameters.model_copy(update={"max_hold_days": 7}))
    elif changed == "study_binding":
        other[changed] = arguments[changed].model_copy(update={"request_hash": "f" * 64})
    elif changed == "tick_times":
        other[changed] = arguments[changed][1:]
    else:
        other[changed] = tuple(item for item in arguments[changed] if item.relative_path != "warmup.parquet")
    with minute_parameter_validation_scope():
        assert owner.measure_minute_parameter_work(**arguments) == expected
        with pytest.raises((KeyError, ValueError, PermissionError)):
            owner.measure_minute_parameter_work(**other)
        assert len(owner._PARAMETER_WORK_STATE.get()) == 1


@pytest.mark.parametrize("changed", ("code", "defaults", "projection"))
def test_work_reuse_rechecks_actual_owner_dependencies(original_work_input: tuple[dict[str, Any], object], monkeypatch: pytest.MonkeyPatch, changed: str) -> None:
    from rquant import minute_backtest_parameter_source as owner
    from rquant.minute_backtest_parameter_definition import minute_parameter_validation_scope

    arguments, _ = original_work_input
    with minute_parameter_validation_scope():
        owner.measure_minute_parameter_work(**arguments)
        if changed == "projection":
            monkeypatch.setattr(owner, "parameter_archive_projections", lambda *args, **kwargs: ())
        elif changed == "code":
            monkeypatch.setattr(owner._measure_minute_parameter_work, "__code__", (lambda *args, **kwargs: None).__code__)
        else:
            monkeypatch.setattr(owner._measure_minute_parameter_work, "__kwdefaults__", {"study_binding": "changed"})
        with pytest.raises(ExecutableDependencyError):
            owner.measure_minute_parameter_work(**arguments)


def test_work_payload_pollution_and_exception_release(original_work_input: tuple[dict[str, Any], object]) -> None:
    from rquant import minute_backtest_parameter_source as owner
    from rquant.minute_backtest_parameter_definition import minute_parameter_validation_scope

    arguments, expected = original_work_input
    with pytest.raises(ExecutableDependencyError):
        with minute_parameter_validation_scope():
            assert owner.measure_minute_parameter_work(**arguments) == expected
            state = owner._PARAMETER_WORK_STATE.get()
            entry, = state.values()
            object.__setattr__(entry, "payload", entry.payload.replace('"prefix_rows":63', '"prefix_rows":64'))
            owner.measure_minute_parameter_work(**arguments)
    assert state == {} and owner._PARAMETER_WORK_STATE.get() is None
    with minute_parameter_validation_scope():
        assert owner._PARAMETER_WORK_STATE.get() == {}
        assert owner.measure_minute_parameter_work(**arguments) == expected
