"""Original journal confirmation, exact retries and source identity own paper effects."""

from pathlib import Path
from datetime import timedelta
from uuid import uuid4

import pytest

from rquant.page_control import PageControlCommandConflictError, PageControlStatus
from rquant.page_control_service import build_page_control_service
from tests.unit.test_paper_portfolio_pause_chain import runtime_fixture
from tests.unit.test_paper_portfolio_admission import request
from tests.unit.test_paper_signal_worker import EXECUTION_TIME


def service_for(tmp_path: Path):
    from rquant.paper_portfolio_commands import PaperPortfolioPageControlBackend
    from rquant.paper_portfolio_runtime import PaperPortfolioRuntimeCatalog

    _, _, operator, runtime = runtime_fixture(tmp_path)
    backend = PaperPortfolioPageControlBackend(PaperPortfolioRuntimeCatalog((runtime,)), clock=lambda: EXECUTION_TIME,
                                               editor_users=("alice",), enabled=True)
    service = build_page_control_service(outbox_path=tmp_path / "journal.sqlite", data_dir=tmp_path / "private-data",
                                        log_dir=tmp_path / "private-logs", allowed_lab_export_roots=(),
                                        load_default_lab_backend=False, paper_portfolio_backend=backend,
                                        clock=lambda: EXECUTION_TIME)
    return service, backend, operator, runtime


def paused_request(operator):
    return request(operator, sequence=1, expected_paused=False, paused=True)


def submit(service, backend, value, *, confirmation_id=None):
    return service._submit_trusted_paper_portfolio(value, authenticated_actor_id="alice",
                                                  verified_metadata_identity=backend.identity(value.account_id, authenticated_actor_id="alice"),
                                                  confirmation_id=confirmation_id)


def test_two_step_and_original_journal_receipt_do_not_claim_applied(tmp_path: Path) -> None:
    service, backend, operator, _ = service_for(tmp_path)
    value = paused_request(operator)
    with pytest.raises(ValueError):
        submit(service, backend, value)
    assert service.outbox.receipt(value.command_id) is None
    confirmation = backend.prepare_confirmation(value, authenticated_actor_id="alice", expected_identity=operator.state.identity())
    assert service.outbox.receipt(value.command_id) is None and not operator.current().paused
    receipt = submit(service, backend, value, confirmation_id=confirmation.confirmation_id)
    assert receipt.status is PageControlStatus.SUCCEEDED
    assert service.outbox.effect(value.command_id).result == receipt.result
    assert receipt.result["sequence"] == 2 and operator.current().status == "waiting"
    with pytest.raises(ValueError):
        service.submit(value)
    assert operator.apply(observed_at=EXECUTION_TIME).paused


def test_original_retry_precedes_new_head_confirmation_expiry_and_same_id_change(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    service, backend, operator, runtime = service_for(tmp_path)
    value = paused_request(operator)
    confirmation = backend.prepare_confirmation(value, authenticated_actor_id="alice", expected_identity=operator.state.identity())
    original = submit(service, backend, value, confirmation_id=confirmation.confirmation_id)
    def forbidden(*args, **kwargs):
        raise AssertionError("original journal retry cannot compile a new current request")
    monkeypatch.setattr(backend, "compile", forbidden)
    monkeypatch.setattr(runtime.state, "refresh_configuration", forbidden)
    recovered = service._submit_trusted_paper_portfolio(value, authenticated_actor_id="alice",
                                                       verified_metadata_identity=operator.state.identity().model_copy(update={"instance_id": str(uuid4())}),
                                                       confirmation_id="expired-or-unknown")
    assert recovered == original
    with pytest.raises(PageControlCommandConflictError):
        service._lookup_trusted_paper_portfolio(value.model_copy(update={"paused": False}), authenticated_actor_id="alice")
    with pytest.raises(PermissionError):
        service._lookup_trusted_paper_portfolio(value, authenticated_actor_id="bob")


def test_post_confirmation_publish_interruption_restores_one_original_body(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    service, backend, operator, _ = service_for(tmp_path)
    value = paused_request(operator)
    confirmation = backend.prepare_confirmation(value, authenticated_actor_id="alice", expected_identity=operator.state.identity())
    def interrupted() -> None:
        raise RuntimeError("after control rename")
    monkeypatch.setattr(operator, "_after_file_replace", interrupted)
    first = submit(service, backend, value, confirmation_id=confirmation.confirmation_id)
    assert first.status is PageControlStatus.PENDING
    assert operator.apply(observed_at=EXECUTION_TIME).paused
    monkeypatch.setattr(operator, "_after_file_replace", lambda: None)
    recovered = service._resume_trusted_paper_portfolio(value, authenticated_actor_id="alice")
    assert recovered.status is PageControlStatus.SUCCEEDED and recovered.result["sequence"] == 2
    assert operator.current().status == "waiting"
    assert operator.apply(observed_at=EXECUTION_TIME).control_fingerprint == operator.lookup(value, authenticated_actor_id="alice").fingerprint


def test_immutable_configuration_and_original_retry_keep_new_series(tmp_path: Path) -> None:
    from rquant.paper_operator_commands import SavePaperPortfolioConfiguration

    service, backend, operator, runtime = service_for(tmp_path)
    old = runtime.state.configuration
    value = SavePaperPortfolioConfiguration(command_id=str(uuid4()), requested_at=EXECUTION_TIME, generation_id="generation-a",
                                            account_id=old.binding.account_id, expected_configuration_fingerprint=old.fingerprint,
                                            weight_rule={"method": "rank_score", "max_positions": 3, "cash_reserve": ".2"},
                                            drawdown_rule={"action": "block_new_positions", "trigger_drawdown": ".2", "release_drawdown": ".05"})
    receipt = submit(service, backend, value)
    assert receipt.status is PageControlStatus.SUCCEEDED and receipt.result["version"] == 2
    assert runtime.state.configuration.version == 2 and runtime.state.observations() == ()
    assert operator.current().paused and operator.current().status == "waiting"
    assert submit(service, backend, value) == receipt and runtime.state.configuration.version == 2


def test_exact_five_minute_expiry_has_no_journal_or_control_effect(tmp_path: Path) -> None:
    service, backend, operator, _ = service_for(tmp_path)
    value = paused_request(operator)
    confirmation = backend.prepare_confirmation(value, authenticated_actor_id="alice", expected_identity=operator.state.identity())
    backend.clock = lambda: EXECUTION_TIME + timedelta(minutes=5)
    with pytest.raises(ValueError):
        submit(service, backend, value, confirmation_id=confirmation.confirmation_id)
    assert service.outbox.receipt(value.command_id) is None
    assert operator.current().sequence == 1 and not operator.current().paused


def test_two_accepted_commands_share_one_cas_predecessor(tmp_path: Path) -> None:
    service, backend, operator, _ = service_for(tmp_path)
    first = paused_request(operator)
    second = first.model_copy(update={"command_id": str(uuid4())})
    owned = []
    for value in (first, second):
        confirmation = backend.prepare_confirmation(value, authenticated_actor_id="alice", expected_identity=operator.state.identity())
        owned.append(backend.compile(value, authenticated_actor_id="alice", expected_identity=operator.state.identity(),
                                     confirmation_id=confirmation.confirmation_id))
    receipts = [service.outbox.enqueue_trusted_paper_portfolio(value) for value in owned]
    first_result = service._settle(owned[0], receipts[0], paper_portfolio_command=owned[0])
    second_result = service._settle(owned[1], receipts[1], paper_portfolio_command=owned[1])
    assert first_result.status is PageControlStatus.SUCCEEDED
    assert second_result.status is PageControlStatus.FAILED
    assert operator.lookup(first, authenticated_actor_id="alice").sequence == 2
    assert operator.lookup(second, authenticated_actor_id="alice") is None
    assert operator.current().status == "waiting"


def test_successful_original_receipt_cannot_follow_replaced_metadata(tmp_path: Path) -> None:
    service, backend, operator, runtime = service_for(tmp_path)
    value = paused_request(operator)
    confirmation = backend.prepare_confirmation(value, authenticated_actor_id="alice", expected_identity=operator.state.identity())
    original = submit(service, backend, value, confirmation_id=confirmation.confirmation_id)
    source = runtime.state.path
    source.rename(source.with_suffix(".original"))
    source.write_bytes(source.with_suffix(".original").read_bytes())
    source.chmod(0o600)
    with pytest.raises(ValueError, match="identity|changed|replaced"):
        service._resume_trusted_paper_portfolio(value, authenticated_actor_id="alice")
    assert service.outbox.receipt(value.command_id) == original


def test_public_owned_payload_and_disabled_backend_are_refused_before_effect(tmp_path: Path) -> None:
    from rquant.page_control import parse_page_control_command

    service, backend, operator, _ = service_for(tmp_path)
    value = paused_request(operator)
    confirmation = backend.prepare_confirmation(value, authenticated_actor_id="alice", expected_identity=operator.state.identity())
    owned = backend.compile(value, authenticated_actor_id="alice", expected_identity=operator.state.identity(), confirmation_id=confirmation.confirmation_id)
    for public in (value, owned, owned.model_dump(mode="json")):
        with pytest.raises(ValueError):
            parse_page_control_command(public)
    with pytest.raises(ValueError):
        service.outbox.enqueue(owned)
    backend.enabled = False
    with pytest.raises(PermissionError):
        submit(service, backend, value, confirmation_id=confirmation.confirmation_id)
    assert service.outbox.receipt(value.command_id) is None
