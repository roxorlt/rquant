"""Owned original run commands target only their captured ledger and request."""

from datetime import timedelta
from pathlib import Path

import pytest

from tests.unit.test_factor_run_configuration import _configured
from tests.unit.test_factor_source_prepare import _AS_OF


def test_page_run_is_owned_targeted_and_replays_without_new_source(tmp_path: Path) -> None:
    from rquant.factor.run_backend import FactorRunPageControlBackend
    from rquant.page_control import PageControlConsumer, PageControlOutbox, PageControlService

    root, reference, request = _configured(tmp_path)
    backend = FactorRunPageControlBackend(root, reference, clock=lambda: _AS_OF)
    outbox = PageControlOutbox(tmp_path / "outbox.sqlite")
    consumer = PageControlConsumer(
        outbox=outbox,
        data_dir=tmp_path,
        log_dir=tmp_path,
        factor_run_backend=backend,
        clock=lambda: _AS_OF,
    )
    service = PageControlService(outbox=outbox, consumer=consumer)
    receipt = service._submit_trusted_factor_run(
        request,
        authenticated_actor_id="alice",
        verified_registry_instance_id=backend.configuration().registry_identity.instance_id,
    )
    assert receipt.status.value == "succeeded"
    original = receipt.result
    assert original["status"] == "queued"
    backend.reference = backend.reference.model_copy(update={"sha256": "0" * 64})
    restored = service._resume_trusted_factor_run(request, authenticated_actor_id="alice")
    assert restored.result == original
    with pytest.raises(PermissionError):
        service._resume_trusted_factor_run(request, authenticated_actor_id="bob")
    with pytest.raises(ValueError):
        service._resume_trusted_factor_run(
            request.model_copy(
                update={"parameters": request.parameters.model_copy(update={"holding_sessions": 1})}
            ),
            authenticated_actor_id="alice",
        )


def test_ledger_commit_before_effect_recovers_same_job_and_does_not_drain_other(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from rquant.factor.run_backend import FactorRunPageControlBackend
    from rquant.factor.run_configuration import open_factor_run_configuration
    from rquant.page_control import (
        PageControlConsumer,
        PageControlOutbox,
        PageControlService,
        SaveCanvas,
        SubmitFactorRun,
        parse_page_control_command,
    )

    root, reference, request = _configured(tmp_path)
    backend = FactorRunPageControlBackend(root, reference, clock=lambda: _AS_OF)
    outbox = PageControlOutbox(tmp_path / "outbox.sqlite")
    unrelated = SaveCanvas(command_id="unrelated-pending", requested_at=_AS_OF, name="待办")
    outbox.enqueue(unrelated)
    consumer = PageControlConsumer(
        outbox=outbox,
        data_dir=tmp_path,
        log_dir=tmp_path,
        factor_run_backend=backend,
        clock=lambda: _AS_OF,
    )
    service = PageControlService(outbox=outbox, consumer=consumer)
    finish = outbox.finish_effect

    def crash(*args: object, **kwargs: object) -> None:
        raise KeyboardInterrupt("after ledger commit, before PageControl effect receipt")

    monkeypatch.setattr(outbox, "finish_effect", crash)
    with pytest.raises(KeyboardInterrupt):
        service._submit_trusted_factor_run(
            request,
            authenticated_actor_id="alice",
            verified_registry_instance_id=backend.configuration().registry_identity.instance_id,
        )
    matched = outbox.lookup_factor_run_command(request, authenticated_actor_id="alice")
    assert matched is not None
    owned = matched[0]
    with open_factor_run_configuration(root, reference) as loaded:
        ledger = loaded.open_ledger(clock=lambda: _AS_OF)
    original = ledger.lookup_command(request.command_id, owned.spec.spec_sha256)
    assert original is not None
    assert outbox.effect(request.command_id).status.value == "started"
    for command in (
        SubmitFactorRun(
            command_id=request.command_id, requested_at=request.requested_at, request=request
        ),
        owned,
    ):
        with pytest.raises(ValueError):
            outbox.enqueue(command)
        with pytest.raises(ValueError):
            service.submit(command)
        with pytest.raises(ValueError):
            parse_page_control_command(command.model_dump())
    monkeypatch.setattr(outbox, "finish_effect", finish)
    consumer.clock = lambda: _AS_OF + timedelta(minutes=10)
    backend.reference = backend.reference.model_copy(update={"sha256": "0" * 64})
    recovered = service._resume_trusted_factor_run(request, authenticated_actor_id="alice")
    assert recovered.status.value == "succeeded"
    assert recovered.result["job_id"] == original.job_id
    assert len(ledger.list_recent()) == 1
    assert outbox.receipt(unrelated.command_id).status.value == "pending"
    Path(owned.ledger_identity.path).rename(tmp_path / "original-ledger.sqlite")
    Path(owned.ledger_identity.path).touch(mode=0o600)
    with pytest.raises(RuntimeError):
        service._resume_trusted_factor_run(request, authenticated_actor_id="alice")
