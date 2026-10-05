"""The two real run routes accept only an original bounded private request."""

from datetime import timedelta

from rquant.strategy_authoring_admission import StrategyAuthoringAdmission
from rquant.web.app import create_app
from rquant.web.settings import WebSettings
from tests.support.web_proxy_identity import ProofTestClient, with_test_proxy_identity
from tests.unit.test_strategy_authoring_web import PREFIX, WRITE, publish
from tests.unit.test_strategy_template_submission import run_service


def test_real_run_web_routes_bind_original_job_and_resume_before_new_generation(
    tmp_path, monkeypatch
) -> None:
    target, service, backend, original, _ = run_service(tmp_path)
    root = tmp_path / "serving"
    manifest = publish(root, target, sequence=2)
    admission = StrategyAuthoringAdmission(
        service,
        source_catalog_provider=lambda *_args: (_ for _ in ()).throw(
            AssertionError("run reads duplicate source catalog")
        ),
    )
    settings = with_test_proxy_identity(WebSettings(serving_root=root))
    settings = WebSettings.model_validate(
        {
            **settings.model_dump(),
            "strategy_authoring_enabled": True,
            "strategy_authoring_users": {"alice"},
        }
    )
    app = create_app(
        settings,
        strategy_authoring_gateway=admission,
        clock=lambda: manifest.built_at + timedelta(seconds=1),
        background=False,
    )
    original = original.model_copy(update={"generation_id": manifest.generation_id})
    with ProofTestClient(app, headers=WRITE) as client:
        detail = client.get(f"{PREFIX}/{original.strategy_id}").json()["data"]
        assert detail["can_run"] is True
        url = f"{PREFIX}/{original.strategy_id}/runs"
        response = client.post(url, json=original.model_dump(mode="json"))
        assert response.status_code == 200 and response.json()["data"]["status"] == "submitted"
        assert response.json()["data"]["job_id"] == original.command_id
        assert len(backend.facade.spool.pending()) == 1
        (root / "current.json").unlink()
        monkeypatch.setattr(
            backend.preparer,
            "prepare",
            lambda *_args, **_kwargs: (_ for _ in ()).throw(
                AssertionError("resume read new source")
            ),
        )
        resumed = client.post(url + "/resume", json=original.model_dump(mode="json"))
        assert resumed.status_code == 200 and resumed.json()["data"]["status"] == "submitted"
        assert (
            client.post(
                url, headers={**WRITE, "content-type": "application/json"}, content=b" " * 4097
            ).status_code
            == 413
        )
        assert (
            client.post(
                url, json={**original.model_dump(mode="json"), "owner_id": "bob"}
            ).status_code
            == 422
        )
        assert (
            client.post(
                url,
                headers={**WRITE, "x-rquant-user": "bob"},
                json=original.model_dump(mode="json"),
            ).status_code
            == 403
        )


def test_archive_physical_whitespace_over_4k_is_rejected(tmp_path) -> None:
    import json
    from uuid import uuid4

    from rquant.strategy_authoring_commands import ArchiveStrategyTemplate

    target, service, backend, original, _ = run_service(tmp_path)
    root = tmp_path / "serving"
    manifest = publish(root, target, sequence=2)
    admission = StrategyAuthoringAdmission(service, source_catalog_provider=lambda *_args: None)
    settings = with_test_proxy_identity(WebSettings(serving_root=root))
    settings = WebSettings.model_validate(
        {
            **settings.model_dump(),
            "strategy_authoring_enabled": True,
            "strategy_authoring_users": {"alice"},
        }
    )
    app = create_app(
        settings,
        strategy_authoring_gateway=admission,
        clock=lambda: manifest.built_at + timedelta(seconds=1),
        background=False,
    )
    archive = ArchiveStrategyTemplate(
        command_id=str(uuid4()),
        requested_at=original.requested_at,
        generation_id=manifest.generation_id,
        strategy_id=original.strategy_id,
        expected_head=original.head,
    )
    with ProofTestClient(app, headers=WRITE) as client:
        body = json.dumps(archive.model_dump(mode="json")).encode() + b" " * 4096
        assert (
            client.post(
                PREFIX + "/commands",
                headers={**WRITE, "content-type": "application/json"},
                content=body,
            ).status_code
            == 413
        )
    assert service.outbox.receipt(archive.command_id) is None
