"""Actual private Web, journal and published metadata boundaries for templates."""

from __future__ import annotations

from datetime import timedelta
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient

from rquant.screen.loader import FUNDAMENTAL_COLS_MAP
from rquant.serving_publisher import ServingPublisher
from rquant.serving_read_models import (
    SERVING_TABLE_SPECS,
    ServingProjectionInput,
    ServingReadModelInput,
    build_serving_read_models,
)
from rquant.strategy_authoring_admission import StrategyAuthoringAdmission
from rquant.strategy_authoring_projection import (
    build_strategy_authoring_snapshot,
    project_strategy_authoring,
)
from rquant.strategy_template import TemplateCondition
from rquant.web.app import create_app
from rquant.web.screen_catalog import screen_blocks
from rquant.web.settings import WebSettings
from rquant.web.strategy_authoring_reader import read_strategy_authoring
from tests.support.web_proxy_identity import (
    PROXY_HEADERS,
    ProofTestClient,
    with_test_proxy_identity,
)
from tests.support.web_serving_fixture import _generation_ids, _watermarks
from tests.unit.test_strategy_authoring import NOW, catalog, draft, store
from tests.unit.test_strategy_authoring_page_control import service_for

PREFIX = "/api/v1/strategy-templates"
WRITE = {"x-rquant-csrf": "1", "x-rquant-user": "alice", **PROXY_HEADERS}


def publish(root, target, *, sequence: int = 0):
    at = NOW + timedelta(minutes=sequence)
    generations = _generation_ids("baseline", sequence)
    snapshot = build_strategy_authoring_snapshot(
        target, available_at=at, source_catalogs=(catalog(),)
    )
    payloads = project_strategy_authoring(snapshot).serving_payloads()
    projections = tuple(
        ServingProjectionInput.bind(
            p, owner_dataset_id="lab_jobs", owner_generation_id=generations["lab_jobs"]
        )
        for p in payloads
    )
    tables = build_serving_read_models(
        ServingReadModelInput(observed_at=at, projections=projections)
    )
    return ServingPublisher(
        root, producer_commit="0" * 40, schema_version=3, table_specs=SERVING_TABLE_SPECS
    ).publish(
        tables,
        watermarks=_watermarks("baseline", built_at=at, generations=generations, sequence=sequence),
        source_generations=generations,
        built_at=at,
    )


def setup(tmp_path):
    target = store(tmp_path)
    service = service_for(tmp_path, target)
    private = StrategyAuthoringAdmission(
        service, source_catalog_provider=lambda owner, generation: catalog(generation=generation)
    )
    root = tmp_path / "serving"
    manifest = publish(root, target)
    settings = with_test_proxy_identity(WebSettings(serving_root=root))
    settings = WebSettings.model_validate(
        {
            **settings.model_dump(),
            "strategy_authoring_enabled": True,
            "strategy_authoring_users": {"alice"},
        }
    )
    clock = [NOW + timedelta(minutes=1)]
    app = create_app(
        settings, strategy_authoring_gateway=private, clock=lambda: clock[0], background=False
    )
    return SimpleNamespace(
        target=target,
        service=service,
        private=private,
        root=root,
        manifest=manifest,
        app=app,
        clock=clock,
    )


def test_template_settings_default_closed_and_require_private_identity(tmp_path) -> None:
    settings = WebSettings(serving_root=tmp_path)
    assert (
        settings.strategy_authoring_enabled is False
        and settings.strategy_authoring_users == frozenset()
    )
    with pytest.raises(ValueError, match="private"):
        WebSettings(
            serving_root=tmp_path,
            strategy_authoring_enabled=True,
            strategy_authoring_users={"alice"},
        )


def test_owner_bound_reads_do_not_leak_metadata_identity_or_another_owner(tmp_path) -> None:
    ctx = setup(tmp_path)
    saved = ctx.target.save(draft(), owner_id="alice", catalog=catalog())
    publish(ctx.root, ctx.target, sequence=2)
    ctx.clock[0] = NOW + timedelta(minutes=3)
    ctx.app.state.web.tracker.refresh()
    with ctx.app.state.web.tracker.borrow() as borrowed:
        assert read_strategy_authoring(borrowed).for_owner("alice")[0].metadata.head == saved.head
    with ProofTestClient(ctx.app, headers={"x-rquant-user": "alice"}) as client:
        response = client.get(PREFIX)
        assert response.status_code == 200
        data = response.json()["data"]
        assert (
            data["availability"] == "populated"
            and data["templates"][0]["strategy_id"] == saved.strategy_id
        )
        assert (
            "metadata.sqlite" not in response.text
            and "inode" not in response.text
            and "instance_id" not in response.text
        )
        detail = client.get(f"{PREFIX}/{saved.strategy_id}")
        assert (
            detail.status_code == 200
            and detail.json()["data"]["rules"]["entry"]["kind"] == "conditions"
        )
        history = client.get(f"{PREFIX}/{saved.strategy_id}/versions")
        assert history.status_code == 200 and len(history.json()["data"]["versions"]) == 1
        sources = client.get(f"{PREFIX}/sources")
        assert sources.status_code == 200 and len(sources.json()["data"]["conditions"]) == 26
        assert client.get(PREFIX, params={"generation_id": "a" * 64}).status_code == 409
        assert (
            client.get(PREFIX, headers={"x-rquant-user": "bob"}).json()["data"]["templates"] == []
        )
        assert (
            client.get(
                f"{PREFIX}/{saved.strategy_id}", headers={"x-rquant-user": "bob"}
            ).status_code
            == 404
        )


def test_sources_offer_original_chinese_controls_and_typed_rule_arguments(tmp_path) -> None:
    ctx = setup(tmp_path)
    with ProofTestClient(ctx.app, headers={"x-rquant-user": "alice"}) as client:
        response = client.get(PREFIX + "/sources")
    assert response.status_code == 200
    choices = response.json()["data"]["conditions"]
    originals = screen_blocks(fundamental_fields=FUNDAMENTAL_COLS_MAP)
    assert {choice["key"] for choice in choices} == {block.key for block in originals}
    for choice, block in zip(choices, originals, strict=True):
        assert choice["label"] == block.label
        assert choice["block"] == block.model_dump(mode="json")
        assert not any(parameter["custom_ma"] for parameter in choice["block"]["parameters"])
    schema = TemplateCondition.model_json_schema(mode="serialization")
    assert schema["properties"]["args"]["type"] == "object"
    assert "additionalProperties" in schema["properties"]["args"]
    condition = TemplateCondition(key="gt", args={"left": "CLOSE[0]", "right": 10.0})
    assert condition.model_dump(mode="json")["args"] == {"left": "CLOSE[0]", "right": 10.0}


def test_save_is_actual_journal_then_waits_for_exact_publication(tmp_path) -> None:
    ctx = setup(tmp_path)
    original = draft().model_copy(update={"generation_id": ctx.manifest.generation_id})
    with ProofTestClient(ctx.app, headers=WRITE) as client:
        response = client.post(PREFIX + "/commands", json=original.model_dump(mode="json"))
        assert response.status_code == 200
        data = response.json()["data"]
        assert data["status"] == "succeeded_waiting_publication"
        saved = ctx.target.lookup_command(original, owner_id="alice")
        assert saved.strategy_id == data["strategy_id"] and ctx.service.outbox.receipt(
            original.command_id
        ).result == saved.model_dump(mode="json")
        publish(ctx.root, ctx.target, sequence=2)
        ctx.clock[0] = NOW + timedelta(minutes=3)
        response = client.post(PREFIX + "/commands/resume", json=original.model_dump(mode="json"))
        assert response.status_code == 200 and response.json()["data"]["status"] == "published"
        assert response.json()["data"]["head"] == saved.head.model_dump(mode="json")


def test_original_retry_precedes_new_serving_preflight_and_changed_body_rejected(
    tmp_path, monkeypatch
) -> None:
    ctx = setup(tmp_path)
    original = draft().model_copy(update={"generation_id": ctx.manifest.generation_id})
    with ProofTestClient(ctx.app, headers=WRITE) as client:
        assert (
            client.post(PREFIX + "/commands", json=original.model_dump(mode="json")).status_code
            == 200
        )

        def no_new_source(*args, **kwargs):
            raise AssertionError("original retry must not preflight a new source or head")

        monkeypatch.setattr(ctx.private, "source_catalog_provider", no_new_source)
        (ctx.root / "current.json").unlink()
        retry = client.post(PREFIX + "/commands", json=original.model_dump(mode="json"))
        assert (
            retry.status_code == 200
            and retry.json()["data"]["status"] == "succeeded_waiting_publication"
        )
        changed = original.model_copy(update={"name": "新正文"})
        rejected = client.post(PREFIX + "/commands", json=changed.model_dump(mode="json"))
        assert rejected.status_code in {200, 409} and (
            rejected.status_code == 409 or rejected.json()["data"]["status"] == "rejected"
        )


def test_auth_csrf_and_body_caps_reject_without_command_effect(tmp_path) -> None:
    ctx = setup(tmp_path)
    original = draft().model_copy(update={"generation_id": ctx.manifest.generation_id})
    body = original.model_dump(mode="json")
    with TestClient(ctx.app) as client:
        assert client.get(PREFIX, headers={"x-rquant-user": "alice"}).status_code == 401
        assert (
            client.post(
                PREFIX + "/commands",
                headers={"x-rquant-user": "alice", "x-rquant-csrf": "1"},
                json=body,
            ).status_code
            == 401
        )
        assert (
            client.post(
                PREFIX + "/commands", headers={"x-rquant-user": "alice", **PROXY_HEADERS}, json=body
            ).status_code
            == 403
        )
        assert (
            client.post(
                PREFIX + "/commands",
                headers={**WRITE, "origin": "https://other.example"},
                json=body,
            ).status_code
            == 403
        )
        assert (
            client.post(
                PREFIX + "/commands",
                headers={**WRITE, "content-type": "application/json"},
                content=b" " * (32 * 1024 + 1),
            ).status_code
            == 413
        )
    assert ctx.service.outbox.receipt(original.command_id) is None
