from __future__ import annotations

from uuid import UUID

import pytest

from rquant.sealed_result_ownership import OriginalSubmissionFact, OriginalEffectFact, SealedJobFact, SealedArtifactFact, bind_sealed_owner, require_owned_sealed_result
from rquant.web.models.collaboration import ResultOwnerQuery

JOB = "f2123047-de88-51b6-93b1-29c6a4c0c76b"
COMMAND = "0d66a786-d60c-4932-9121-6e859f9c7b40"

def minute_facts():
    submit = OriginalSubmissionFact(domain="minute", command_id=COMMAND, command_kind="submit_minute_replay", command_sha256="1"*64, actor_id="fixture-owner", job_id=JOB, spec_hash="2"*64, origin_verified=True)
    effect = OriginalEffectFact(command_id=COMMAND, command_kind="submit_minute_replay", command_sha256="1"*64, status="succeeded", submitted_job_id=JOB, submitted_spec_hash="2"*64, worker_owner_id="page-worker")
    job = SealedJobFact(domain="minute", job_id=JOB, spec_hash="2"*64, status="succeeded", manifest_hash="3"*64, complete_result_hash="4"*64)
    artifact = SealedArtifactFact(domain="minute", job_id=JOB, spec_hash="2"*64, manifest_hash="3"*64, complete_result_hash="4"*64, full_artifact_hash="4"*64, result_payload_hash="5"*64, input_hash="6"*64, complete=True, private_owner="fixture-owner")
    return submit,effect,job,artifact

def test_minute_domain_requires_its_exact_original_submit_kind_and_full_owner() -> None:
    submit,effect,job,artifact=minute_facts()
    owner=bind_sealed_owner(submit,effect,job,artifact)
    assert owner is not None and owner.domain=="minute" and owner.command_kind=="submit_minute_replay"
    assert require_owned_sealed_result(owner,requester="fixture-owner",current_artifact=artifact)==owner
    with pytest.raises(PermissionError):require_owned_sealed_result(owner,requester="page-worker",current_artifact=artifact)
    with pytest.raises(ValueError):OriginalSubmissionFact.model_validate(submit.model_dump(mode="python")|{"command_kind":"submit_portfolio_backtest"})
    with pytest.raises(ValueError):bind_sealed_owner(submit,effect,job,SealedArtifactFact.model_validate(artifact.model_dump(mode="python")|{"private_owner":"other-owner"}))
    with pytest.raises(PermissionError):require_owned_sealed_result(owner,requester="fixture-owner",current_artifact=SealedArtifactFact.model_validate(artifact.model_dump(mode="python")|{"input_hash":"7"*64}))

def test_minute_owner_query_uses_original_canonical_uuid_and_closed_domains() -> None:
    assert ResultOwnerQuery(domain="minute",job_id=JOB,spec_hash="2"*64).domain=="minute"
    with pytest.raises(ValueError):ResultOwnerQuery(domain="minute",job_id=UUID(JOB).hex,spec_hash="2"*64)
    with pytest.raises(ValueError):ResultOwnerQuery(domain="unknown",job_id=JOB,spec_hash="2"*64)
