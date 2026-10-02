"""Trusted source capabilities do not broaden either independent actor allowlist."""

from __future__ import annotations

import os
import tempfile
from pathlib import Path
from threading import Thread
from uuid import uuid4

import pytest

from tests.unit.test_factor_neutralization_jobs import _ready, _service
from tests.unit.test_factor_save_page_control import _draft


@pytest.mark.parametrize("expression", ["industry_neutralize(close)", "size_neutralize(close)"])
def test_draft_context_capabilities_are_server_only(expression: str) -> None:
    from rquant.factor.capability import historical_daily_capabilities
    from rquant.factor.draft import build_draft_definition

    with pytest.raises(ValueError):
        build_draft_definition(_draft(expression=expression), authenticated_actor_id="editor")
    definition = build_draft_definition(
        _draft(expression=expression),
        authenticated_actor_id="editor",
        capabilities=historical_daily_capabilities(
            industry_available=True, market_cap_available=True
        ),
    )
    assert definition.expression == expression


def test_actual_private_save_run_worker_and_separate_permissions(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from rquant.factor.page_control_backend import FactorDefinitionPageControlBackend
    from rquant.factor.registry import FactorDefinitionRegistry, FactorHeadRef
    from rquant.factor.run_configuration import run_configured_factor_worker
    from rquant.factor_definition_admission import (
        FactorDefinitionAdmission,
        FactorDefinitionAdmissionClient,
        FactorDefinitionAdmissionRejectedError,
        build_factor_definition_admission_server,
    )
    from rquant.factor_run_admission import (
        FactorRunAdmission,
        FactorRunAdmissionClient,
        FactorRunAdmissionRejectedError,
        build_factor_run_admission_server,
    )

    root, reference, request, _, config, _, backend = _ready(tmp_path, monkeypatch)
    service, _ = _service(tmp_path, backend, request)
    service.consumer.factor_definition_backend = FactorDefinitionPageControlBackend(
        FactorDefinitionRegistry(Path(config.registry_identity.path))
    )
    uid, gid = os.geteuid() + 1, os.getegid()
    directory = Path(tempfile.mkdtemp(prefix="fneu-", dir="/private/tmp"))
    os.chown(directory, os.geteuid(), gid)
    directory.chmod(0o710)
    definition_server = build_factor_definition_admission_server(
        FactorDefinitionAdmission(
            service, editor_users=frozenset({"editor-only"}), save_enabled=True
        ),
        socket_path=directory / "save.sock",
        trusted_web_uid=uid,
        shared_gid=gid,
        peer_uid=lambda _: uid,
    )
    run_server = build_factor_run_admission_server(
        FactorRunAdmission(service, run_users=frozenset({"alice"}), enabled=True),
        socket_path=directory / "run.sock",
        trusted_web_uid=uid,
        shared_gid=gid,
        peer_uid=lambda _: uid,
    )
    threads = [
        Thread(target=server.serve_forever, name="neutralization-synthetic-unix", daemon=True)
        for server in (definition_server, run_server)
    ]
    for thread in threads:
        thread.start()
    try:
        save = FactorDefinitionAdmissionClient(
            directory / "save.sock",
            expected_service_uid=os.geteuid(),
            shared_gid=gid,
            client_uid=lambda: uid,
        )
        run = FactorRunAdmissionClient(
            directory / "run.sock",
            expected_service_uid=os.geteuid(),
            shared_gid=gid,
            client_uid=lambda: uid,
        )
        capability = save.capabilities(authenticated_actor_id="editor-only")
        assert {"industry_neutralize", "size_neutralize"} <= set(capability.runnable_operators)
        with pytest.raises(FactorDefinitionAdmissionRejectedError):
            save.capabilities(authenticated_actor_id="alice")
        draft = _draft(
            command_id="new-neutralized-dsl",
            requested_at=request.requested_at,
            expression="size_neutralize(industry_neutralize(close))",
        )
        saved = save.submit_save(
            draft,
            authenticated_actor_id="editor-only",
            verified_registry_instance_id=config.registry_identity.instance_id,
        )
        effect = saved.receipt.result
        parameters = {
            **request.parameters.model_dump(),
            "factor_id": effect["factor_id"],
            "expected_head": FactorHeadRef(
                version=effect["version"], content_sha256=effect["content_sha256"]
            ),
            "neutralization": "industry_size",
        }
        request = type(request).model_validate(
            {**request.model_dump(), "command_id": str(uuid4()), "parameters": parameters}
        )
        with pytest.raises(FactorRunAdmissionRejectedError):
            run.submit(
                request,
                authenticated_actor_id="editor-only",
                verified_registry_instance_id=config.registry_identity.instance_id,
            )
        submitted = run.submit(
            request,
            authenticated_actor_id="alice",
            verified_registry_instance_id=config.registry_identity.instance_id,
        )
        result = run_configured_factor_worker(root, reference, clock=lambda: request.requested_at)
        assert result.status == "succeeded", result
        assert result.record.job_id == submitted.job_id
        assert result.record.spec.adapter_request.formula.definition.expression == draft.expression
        assert result.record.completion.neutralization == "industry_size"
        assert run.resume(request, authenticated_actor_id="alice").job_id == submitted.job_id
    finally:
        for server in (run_server, definition_server):
            server.shutdown()
        for thread in threads:
            thread.join(timeout=3)
        for server in (run_server, definition_server):
            server.server_close()
        directory.rmdir()
    assert not any(thread.is_alive() for thread in threads)
