"""Queue quantity evidence and operator application ordering, using private state."""

import os
from datetime import timedelta
from pathlib import Path
from uuid import uuid4

import pytest

from rquant.paper_signal_worker import PaperSignalQueueStore
from tests.unit.test_paper_portfolio_core import config_data, materials
from tests.unit.test_paper_signal_worker import EXECUTION_TIME, _policy, _quote, _signal


def test_original_prepare_refresh_and_restart_use_one_typed_quantity_basis(tmp_path: Path) -> None:
    from rquant.paper_portfolio_target import prepare_paper_target_quantity

    _, value = materials(tmp_path)
    authority = prepare_paper_target_quantity(value)
    queue = PaperSignalQueueStore(tmp_path / "queue.sqlite", policy=_policy())
    signal, quote = _signal(), _quote(price="1")
    queue.ingest(signal, received_at=signal.available_at)
    first = queue.prepare(signal.signal_id, quote=quote, prepared_at=EXECUTION_TIME,
                          target_quantity_authority=authority)
    second = queue.refresh_prepared(signal.signal_id, quote=quote, prepared_at=EXECUTION_TIME,
                                    target_quantity_authority=authority)
    assert first == second
    assert first.intent.quantity == 800
    assert first.target_quantity_authority == authority
    reopened = PaperSignalQueueStore(queue.path, policy=queue.policy)
    assert reopened.record(signal.signal_id) == first
    with pytest.raises((TypeError, ValueError)):
        queue.refresh_prepared(signal.signal_id, quote=quote, prepared_at=EXECUTION_TIME, quantity=100)


def test_wrong_or_mutated_target_authority_does_not_prepare(tmp_path: Path) -> None:
    from rquant.paper_portfolio_target import prepare_paper_target_quantity

    _, value = materials(tmp_path)
    authority = prepare_paper_target_quantity(value)
    queue = PaperSignalQueueStore(tmp_path / "queue.sqlite", policy=_policy())
    signal = _signal(seed="9")
    queued = queue.ingest(signal, received_at=signal.available_at)
    with pytest.raises(ValueError):
        queue.prepare(signal.signal_id, quote=_quote(price="1"), prepared_at=EXECUTION_TIME,
                      target_quantity_authority=authority)
    assert queue.record(signal.signal_id) == queued


def operator_fixture(tmp_path: Path):
    from rquant.paper_operator import PaperOperatorControlStore
    from rquant.paper_portfolio_models import PaperPortfolioConfiguration
    from rquant.paper_portfolio_state import PaperPortfolioStateStore

    configuration = PaperPortfolioConfiguration(**config_data())
    state = PaperPortfolioStateStore(tmp_path / "operator.sqlite", configuration=configuration)
    operator = PaperOperatorControlStore(state, root=tmp_path / "control" / "operator",
                                        clock=lambda: EXECUTION_TIME)
    return state, operator


def request(operator, *, sequence: int = 0, expected_paused: bool = True, paused: bool = False):
    from rquant.paper_operator_commands import SetPaperAccountPaused

    return SetPaperAccountPaused(command_id=str(uuid4()), requested_at=EXECUTION_TIME - timedelta(days=30),
                                 generation_id="generation-a", account_id=operator.state.configuration.binding.account_id,
                                 configuration_fingerprint=operator.state.configuration.fingerprint,
                                 expected_sequence=sequence, expected_paused=expected_paused, paused=paused)


def confirm(operator, value):
    return operator.commit_confirmed_control(value, authenticated_actor_id="alice",
                                              original_command_id=value.command_id)


def test_published_control_waits_until_actual_application_and_original_retry(tmp_path: Path) -> None:
    _, operator = operator_fixture(tmp_path)
    assert operator.current().status == "unavailable" and operator.current().paused
    value = request(operator)
    control = confirm(operator, value)
    operator.publish(control)
    assert operator.current().status == "waiting"
    applied = operator.apply(observed_at=EXECUTION_TIME)
    assert applied.status == "applied" and not applied.paused and applied.sequence == 1
    assert applied.control_fingerprint == control.fingerprint
    assert operator.lookup(value, authenticated_actor_id="alice") == control
    assert confirm(operator, value) == control
    assert control.issued_at == EXECUTION_TIME


def test_same_id_changed_body_and_foreign_owner_have_no_control_effect(tmp_path: Path) -> None:
    _, operator = operator_fixture(tmp_path)
    value = request(operator)
    confirm(operator, value)
    with pytest.raises(ValueError):
        confirm(operator, value.model_copy(update={"paused": True}))
    with pytest.raises(PermissionError):
        operator.lookup(value, authenticated_actor_id="bob")
    assert not operator.path.exists()


def test_old_file_replacement_missing_and_restart_do_not_resume(tmp_path: Path) -> None:
    from rquant.paper_operator import PaperOperatorControlStore

    state, operator = operator_fixture(tmp_path)
    resume = confirm(operator, request(operator))
    operator.publish(resume)
    operator.apply(observed_at=EXECUTION_TIME)
    old = operator.path.read_bytes()
    pause = confirm(operator, request(operator, sequence=1, expected_paused=False, paused=True))
    operator.publish(pause)
    assert operator.apply(observed_at=EXECUTION_TIME).paused
    operator.path.write_bytes(old)
    broken = operator.apply(observed_at=EXECUTION_TIME)
    assert broken.status == "unavailable" and broken.paused and broken.sequence == 2
    operator.path.unlink()
    restarted = PaperOperatorControlStore(state, root=operator.root, clock=lambda: EXECUTION_TIME)
    missing = restarted.apply(observed_at=EXECUTION_TIME)
    assert missing.status == "unavailable" and missing.paused and missing.sequence == 2
    replacement = operator.root.with_name("other")
    replacement.mkdir(mode=0o700)
    os.rename(operator.root, operator.root.with_name("old"))
    os.rename(replacement, operator.root)
    with pytest.raises(ValueError):
        operator.publish(pause)


def test_control_publication_interruption_resumes_same_confirmed_body(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _, operator = operator_fixture(tmp_path)
    value = request(operator)
    control = confirm(operator, value)
    def fail() -> None:
        raise RuntimeError("interrupted after rename")
    monkeypatch.setattr(operator, "_after_file_replace", fail)
    with pytest.raises(RuntimeError, match="interrupted"):
        operator.publish(control)
    assert operator.current().status == "waiting"
    assert operator.apply(observed_at=EXECUTION_TIME).paused
    monkeypatch.setattr(operator, "_after_file_replace", lambda: None)
    operator.publish(confirm(operator, value))
    applied = operator.apply(observed_at=EXECUTION_TIME)
    assert applied.sequence == 1 and not applied.paused and applied.control_fingerprint == control.fingerprint
