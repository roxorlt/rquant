"""Private decoder tests; no socket or different-OS-UID claim is made."""

from pathlib import Path

import pytest

from rquant.paper_operator_commands import SetPaperAccountPaused
from rquant.paper_portfolio_admission import PaperPrivateRequest, PaperPortfolioAdmission, PaperPortfolioAdmissionClient, PaperPortfolioAdmissionUnavailableError
from tests.unit.test_paper_research_submission import fixture


def test_private_original_request_has_exact_typed_union_and_rejects_extra_owner_or_path(tmp_path):
    _, _, runtime, request, _ = fixture(tmp_path)
    body={"authenticated_actor_id":"alice","command":request.model_dump(mode="json"),"verified_metadata_identity":runtime.state.identity().model_dump(mode="json")}
    assert PaperPrivateRequest.model_validate(body).command == request
    for changed in ({**body,"path":"/caller/ledger"},{**body,"command":{**body["command"],"owner_id":"bob"}}):
        with pytest.raises(ValueError):
            PaperPrivateRequest.model_validate(changed)


def test_private_source_default_closed_and_malformed_confirmation_is_unknown(tmp_path, monkeypatch):
    page, backend, runtime, request, _ = fixture(tmp_path)
    admission=PaperPortfolioAdmission(page,backend=backend)
    with pytest.raises(PermissionError):
        admission.submit(request,authenticated_actor_id="alice",verified_metadata_identity=runtime.state.identity())
    assert not backend.research_backend.facade.spool.pending()
    current=runtime.operator.current()
    pause=SetPaperAccountPaused(command_id=request.command_id,requested_at=request.requested_at,generation_id=request.generation_id,account_id=request.account_id,
        configuration_fingerprint=request.configuration_fingerprint,expected_sequence=current.sequence,expected_paused=current.paused,paused=not current.paused)
    client=PaperPortfolioAdmissionClient(Path("/private/tmp/paper-decoder-only.sock"),expected_service_uid=1002,shared_gid=1003,client_uid=lambda:1001)
    monkeypatch.setattr(client,"_call",lambda *_a,**_k:{"owner_id":"bob"})
    with pytest.raises(PaperPortfolioAdmissionUnavailableError):
        client.prepare(pause,authenticated_actor_id="alice",verified_metadata_identity=runtime.state.identity())
