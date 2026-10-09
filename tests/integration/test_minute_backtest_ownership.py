from __future__ import annotations

import hashlib
import os
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import pytest

from rquant.page_control import PageControlService
from rquant.sealed_result_ownership import SealedArtifactFact, require_owned_sealed_result
from rquant.web.collaboration_gateway import CollaborationGateway
from tests.integration.test_minute_backtest_web_submission import sealed_web_minute, web_minute
from tests.support.minute_backtest_installed import installed_minute


def owner_gateway(control: PageControlService) -> CollaborationGateway:
    def transport(message: object) -> bytes:
        return control.collaboration_request(message).model_dump_json().encode()

    return CollaborationGateway(Path("/private/tmp") / ("c6-unused-owner-" + uuid4().hex + ".sock"),
        expected_service_uid=0 if os.geteuid() != 0 else 1, shared_gid=os.getegid(), transport=transport)


def test_actual_minute_owner_uses_original_submission_and_complete_physical_result(
    sealed_web_minute: SimpleNamespace,
) -> None:
    case = sealed_web_minute
    journal = case.web.control.outbox.path
    before = hashlib.sha256(journal.read_bytes()).hexdigest()
    gateway = owner_gateway(case.web.control)
    full = case.service.read_result(case.job_id, owner_id=case.web.owner)
    assert full == case.full
    proof = gateway.result_owner(case.web.owner, domain="minute", job_id=str(case.job_id), spec_hash=full.spec_hash)
    assert proof.owner_id == full.owner_id == case.web.owner
    assert proof.worker_owner_id != proof.owner_id
    facts = SealedArtifactFact(domain="minute", job_id=str(full.job_id), spec_hash=full.spec_hash,
        manifest_hash=full.manifest_hash, complete_result_hash=full.complete_result_hash,
        full_artifact_hash=full.manifest.complete_result_hash, result_payload_hash=full.result_hash,
        input_hash=full.full_input_hash, complete=True, private_owner=full.owner_id)
    binding = gateway.bind_sealed_artifact(case.web.owner, facts)
    assert binding.command_kind == "submit_minute_replay"
    assert binding.command_id == proof.command_id
    assert binding.command_sha256 == proof.command_sha256
    assert require_owned_sealed_result(binding, requester=case.web.owner, current_artifact=facts) == binding
    for actor in ("other-owner", "viewer", "admin"):
        with pytest.raises((LookupError, PermissionError)):
            gateway.result_owner(actor, domain="minute", job_id=str(case.job_id), spec_hash=full.spec_hash)
    with pytest.raises(LookupError):
        gateway.result_owner(case.web.owner, domain="minute", job_id=str(case.job_id), spec_hash="0" * 64)
    with pytest.raises(LookupError):
        gateway.result_owner(case.web.owner, domain="portfolio", job_id=str(case.job_id), spec_hash=full.spec_hash)
    assert hashlib.sha256(journal.read_bytes()).hexdigest() == before
