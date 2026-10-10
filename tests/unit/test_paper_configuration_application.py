"""A saved configuration reaches the role through its original accepted command."""

from datetime import timedelta
from pathlib import Path
from uuid import uuid4

import pytest

from rquant.page_control import PageControlStatus
from rquant.paper_operator_commands import SavePaperPortfolioConfiguration
from tests.unit.test_paper_portfolio_page_control import service_for, submit
from tests.unit.test_paper_signal_worker import EXECUTION_TIME


def save_request(runtime):
    old = runtime.state.configuration
    return SavePaperPortfolioConfiguration(command_id=str(uuid4()), requested_at=EXECUTION_TIME,
        generation_id="generation-a", account_id=old.binding.account_id,
        expected_configuration_fingerprint=old.fingerprint,
        weight_rule=old.weight_rule.model_copy(update={"max_positions": 2}), drawdown_rule=old.drawdown_rule)


@pytest.mark.parametrize("paused", [False, True])
def test_saved_configuration_preserves_control_intent_until_role_applies(tmp_path: Path, paused: bool) -> None:
    from tests.unit.test_paper_portfolio_admission import request

    service, backend, operator, runtime = service_for(tmp_path)
    if paused:
        pause = request(operator, sequence=1, expected_paused=False, paused=True)
        confirmation = backend.prepare_confirmation(pause, authenticated_actor_id="alice", expected_identity=runtime.state.identity())
        submit(service, backend, pause, confirmation_id=confirmation.confirmation_id)
        operator.apply(observed_at=EXECUTION_TIME)
    before = operator.current()
    command = save_request(runtime)
    receipt = submit(service, backend, command)
    assert receipt.status is PageControlStatus.SUCCEEDED
    # Saving/publication is never an actual role application receipt.
    assert operator.current().status == "waiting" and operator.current().paused
    assert operator.current().sequence == before.sequence
    actual = operator.apply(observed_at=EXECUTION_TIME + timedelta(seconds=1))
    assert actual.status == "applied" and actual.paused == paused
    assert actual.sequence == before.sequence + 1
    assert actual.configuration_fingerprint == runtime.state.configuration.fingerprint
    assert submit(service, backend, command) == receipt
    assert operator.current() == actual


def test_saved_control_publication_interruption_recovers_original_then_cannot_roll_back(tmp_path: Path, monkeypatch) -> None:
    service, backend, operator, runtime = service_for(tmp_path)
    command = save_request(runtime)
    monkeypatch.setattr(operator, "_after_file_replace", lambda: (_ for _ in ()).throw(RuntimeError("after configuration control rename")))
    first = submit(service, backend, command)
    assert first.status is PageControlStatus.PENDING
    assert runtime.state.configuration.version == 2
    assert operator.apply(observed_at=EXECUTION_TIME).paused
    monkeypatch.setattr(operator, "_after_file_replace", lambda: None)
    recovered = service._resume_trusted_paper_portfolio(command, authenticated_actor_id="alice")
    assert recovered.status is PageControlStatus.SUCCEEDED
    v2 = operator.apply(observed_at=EXECUTION_TIME + timedelta(seconds=1))
    assert v2.status == "applied" and v2.sequence == 2 and not v2.paused
    next_command = save_request(runtime)
    assert submit(service, backend, next_command).status is PageControlStatus.SUCCEEDED
    v3 = operator.apply(observed_at=EXECUTION_TIME + timedelta(seconds=2))
    original_file = operator.path.read_bytes()
    assert service._resume_trusted_paper_portfolio(command, authenticated_actor_id="alice") == recovered
    assert operator.path.read_bytes() == original_file
    assert operator.current() == v3 and runtime.state.configuration.version == 3


def test_missing_control_save_stays_closed_and_old_pause_retry_cannot_restore_old_configuration(tmp_path: Path) -> None:
    from tests.unit.test_paper_portfolio_admission import request

    service, backend, operator, runtime = service_for(tmp_path)
    pause = request(operator, sequence=1, expected_paused=False, paused=True)
    confirmation = backend.prepare_confirmation(pause, authenticated_actor_id="alice", expected_identity=runtime.state.identity())
    first = submit(service, backend, pause, confirmation_id=confirmation.confirmation_id)
    operator.apply(observed_at=EXECUTION_TIME)
    operator.path.unlink()
    command = save_request(runtime)
    assert submit(service, backend, command).status is PageControlStatus.SUCCEEDED
    actual = operator.apply(observed_at=EXECUTION_TIME + timedelta(seconds=1))
    assert actual.status == "applied" and actual.paused and actual.sequence == 3
    original_file = operator.path.read_bytes()
    assert service._resume_trusted_paper_portfolio(pause, authenticated_actor_id="alice") == first
    assert operator.path.read_bytes() == original_file and operator.current() == actual
