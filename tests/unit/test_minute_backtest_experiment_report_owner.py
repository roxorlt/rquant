"""Exact private query and the original family role fence; no worker replay."""
from __future__ import annotations

import importlib
from pathlib import Path
from uuid import UUID

import pytest

from rquant.sealed_result_ownership import SealedArtifactFact
from rquant.web.models import collaboration as models

JOB = UUID('be75c2f4-ac29-54fa-b021-d425a0b1a779')
SPEC = 'dc011d7eb16740b5bc8f43759597c06d5427cabf0a9898702de23577849be787'


def query_type() -> type:
    value = getattr(models, 'MinuteReportOwnerQuery', None)
    assert value is not None, 'exact minute report private query is absent'
    return value


def fact(**changes: object) -> SealedArtifactFact:
    return SealedArtifactFact.model_validate({
        'domain': 'minute', 'job_id': str(JOB), 'spec_hash': SPEC,
        'manifest_hash': '1' * 64, 'complete_result_hash': '2' * 64,
        'full_artifact_hash': '2' * 64, 'complete': True,
        'result_payload_hash': '3' * 64, 'input_hash': '4' * 64,
        'private_owner': 'fixture-owner', **changes,
    })


def test_private_report_query_keeps_original_job_and_complete_artifact() -> None:
    query = query_type()(job_id=str(JOB), spec_hash=SPEC, artifact=fact())
    request = models.CollaborationPrivateRequest(schema_version=1,
        operation='minute_report_owner', authenticated_actor_id='fixture-owner',
        minute_owner_query=query)
    assert models.CollaborationPrivateRequest.model_validate_json(request.model_dump_json()) == request


@pytest.mark.parametrize('extra', [{'path': '/ignored'}, {'trusted': True}, {'owner_id': 'foreign'}])
def test_private_report_query_does_not_accept_paths_or_authority(extra: dict[str, object]) -> None:
    cls = query_type()
    with pytest.raises(ValueError):
        cls(job_id=str(JOB), spec_hash=SPEC, **extra)


@pytest.mark.parametrize('changes', [
    {'job_id': str(UUID(int=1))}, {'spec_hash': '0' * 64}, {'domain': 'portfolio'},
])
def test_private_report_query_refuses_another_complete_artifact(changes: dict[str, object]) -> None:
    cls = query_type()
    with pytest.raises(ValueError):
        cls(job_id=str(JOB), spec_hash=SPEC, artifact=fact(**changes))


def test_family_role_read_can_nest_in_original_minute_execution_sh(tmp_path: Path) -> None:
    # Reuse the original accepted roles/private registry/Lab helper. No substitute owner.
    original = importlib.import_module('tests.unit.test_minute_experiment_result_owner')
    chain = original.build_chain(tmp_path / 'roles')
    with chain.roles.locked(read_only=True):
        values = chain.owner()._role('fixture-owner')
        assert values['role'] == 'admin'
        assert values['outbox_identity'] == chain.roles.require_outbox_identity()


def test_private_owner_permission_failure_never_falls_back(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    from tests.unit.test_minute_backtest_study_control import control

    service = control(tmp_path)

    def original_refusal(*args: object, **kwargs: object) -> object:
        raise PermissionError("original submission owner refused")

    monkeypatch.setattr(service, "_trusted_result_owner", original_refusal)
    with pytest.raises(PermissionError) as error:
        service._trusted_minute_report_owner(
            query_type()(job_id=str(UUID(int=77)), spec_hash="1" * 64),
            authenticated_actor_id="researcher")
    assert str(error.value.__cause__) == "original submission owner refused"
    assert service.outbox.effect(str(UUID(int=77))) is None


def test_missing_old_submission_cannot_create_an_uninstalled_family_owner(tmp_path: Path) -> None:
    from tests.unit.test_minute_backtest_study_control import control

    service = control(tmp_path)
    with pytest.raises(PermissionError) as error:
        service._trusted_minute_report_owner(
            query_type()(job_id=str(UUID(int=78)), spec_hash="1" * 64),
            authenticated_actor_id="researcher")
    assert "same original installed backend" in str(error.value.__cause__)
    assert service.outbox.effect(str(UUID(int=78))) is None
