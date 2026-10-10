"""Original binding facts do not themselves attest a private ingress or UID."""

from __future__ import annotations

from importlib import import_module
from types import ModuleType
from typing import TYPE_CHECKING
from uuid import UUID

import pytest

if TYPE_CHECKING:
    from rquant.sealed_result_ownership import (
        OriginalEffectFact,
        OriginalSubmissionFact,
        SealedArtifactFact,
        SealedJobFact,
    )

JOB = str(UUID(int=11))
COMMAND = str(UUID(int=10))


def core() -> ModuleType:
    try:
        return import_module("rquant.sealed_result_ownership")
    except ModuleNotFoundError:
        pytest.fail("required sealed ownership core is not implemented", pytrace=False)


def facts(
    c: ModuleType,
) -> tuple[
    OriginalSubmissionFact, OriginalEffectFact, SealedJobFact, SealedArtifactFact
]:
    submit = c.OriginalSubmissionFact(
        domain="factor",
        command_id=COMMAND,
        command_kind="submit_factor_run",
        command_sha256="a" * 64,
        actor_id="alice",
        job_id=JOB,
        spec_hash="b" * 64,
        origin_verified=True,
    )
    effect = c.OriginalEffectFact(
        command_id=COMMAND,
        command_kind="submit_factor_run",
        command_sha256="a" * 64,
        status="succeeded",
        submitted_job_id=JOB,
        submitted_spec_hash="b" * 64,
        worker_owner_id="worker-1",
    )
    job = c.SealedJobFact(
        domain="factor",
        job_id=JOB,
        spec_hash="b" * 64,
        status="succeeded",
        manifest_hash="c" * 64,
        complete_result_hash="d" * 64,
    )
    artifact = c.SealedArtifactFact(
        domain="factor",
        job_id=JOB,
        spec_hash="b" * 64,
        manifest_hash="c" * 64,
        complete_result_hash="d" * 64,
        full_artifact_hash="e" * 64,
        result_payload_hash="f" * 64,
        input_hash="1" * 64,
        display_hash="2" * 64,
        complete=True,
    )
    return submit, effect, job, artifact


def test_complete_original_chain_has_current_owner() -> None:
    c = core()
    *before, artifact = facts(c)
    binding = c.bind_sealed_owner(*before, artifact)
    assert (
        binding.owner_id == "alice" and binding.manifest_hash == artifact.manifest_hash
    )
    assert (
        c.require_owned_sealed_result(
            binding, requester="alice", current_artifact=artifact
        )
        == binding
    )


def test_unknown_old_owner_or_unverified_origin_never_becomes_worker_or_current_user() -> (
    None
):
    c = core()
    submit, effect, job, artifact = facts(c)
    assert c.bind_sealed_owner(None, effect, job, artifact) is None
    assert (
        c.bind_sealed_owner(
            submit.model_copy(update={"actor_id": None}),
            effect.model_copy(update={"worker_owner_id": "alice"}),
            job,
            artifact,
        )
        is None
    )
    assert (
        c.bind_sealed_owner(
            submit.model_copy(update={"origin_verified": False}), effect, job, artifact
        )
        is None
    )
    with pytest.raises(PermissionError):
        c.require_owned_sealed_result(
            None, requester="alice", current_artifact=artifact
        )


@pytest.mark.parametrize(
    "field",
    [
        "command_id",
        "command_kind",
        "command_sha256",
        "submitted_job_id",
        "submitted_spec_hash",
        "status",
    ],
)
def test_effect_must_match_original_submit_and_acceptance(field: str) -> None:
    c = core()
    submit, effect, job, artifact = facts(c)
    value = (
        "failed"
        if field == "status"
        else "submit_portfolio_backtest"
        if field == "command_kind"
        else str(UUID(int=99))
        if field in ("command_id", "submitted_job_id")
        else "9" * 64
    )
    with pytest.raises(ValueError):
        c.bind_sealed_owner(
            submit, effect.model_copy(update={field: value}), job, artifact
        )


@pytest.mark.parametrize(
    "field",
    [
        "domain",
        "job_id",
        "spec_hash",
        "manifest_hash",
        "complete_result_hash",
        "complete",
    ],
)
def test_artifact_must_bind_completed_job_and_full_seal(field: str) -> None:
    c = core()
    submit, effect, job, artifact = facts(c)
    value = (
        False
        if field == "complete"
        else "portfolio"
        if field == "domain"
        else str(UUID(int=99))
        if field == "job_id"
        else "9" * 64
    )
    with pytest.raises(ValueError):
        c.bind_sealed_owner(
            submit, effect, job, artifact.model_copy(update={field: value})
        )


def test_current_owner_and_all_artifact_hashes_are_rechecked() -> None:
    c = core()
    submit, effect, job, artifact = facts(c)
    binding = c.bind_sealed_owner(submit, effect, job, artifact)
    for user in ("bob", "admin"):
        with pytest.raises(PermissionError):
            c.require_owned_sealed_result(
                binding, requester=user, current_artifact=artifact
            )
    for field in (
        "full_artifact_hash",
        "result_payload_hash",
        "input_hash",
        "display_hash",
        "html_sha256",
    ):
        with pytest.raises(PermissionError):
            c.require_owned_sealed_result(
                binding,
                requester="alice",
                current_artifact=artifact.model_copy(update={field: "9" * 64}),
            )
    with pytest.raises(ValueError):
        c.bind_sealed_owner(
            submit, effect, job, artifact.model_copy(update={"private_owner": "bob"})
        )


def test_pending_job_and_unsafe_identifiers_or_hashes_reject() -> None:
    c = core()
    submit, effect, job, artifact = facts(c)
    with pytest.raises(ValueError):
        c.bind_sealed_owner(
            submit, effect, job.model_copy(update={"status": "processing"}), artifact
        )
    for value in ("../job", "/private/secret", "file:///secret"):
        with pytest.raises(ValueError):
            c.SealedJobFact(**(job.model_dump() | {"job_id": value}))
    with pytest.raises(ValueError):
        c.SealedArtifactFact(
            **(artifact.model_dump() | {"result_payload_hash": "not-a-hash"})
        )


def test_new_fact_version_and_non_boolean_origin_are_not_legacy() -> None:
    c = core()
    submit, _, _, artifact = facts(c)
    with pytest.raises(ValueError):
        c.SealedArtifactFact(**(artifact.model_dump() | {"schema_version": True}))
    with pytest.raises(ValueError):
        c.OriginalSubmissionFact(**(submit.model_dump() | {"origin_verified": 1}))
