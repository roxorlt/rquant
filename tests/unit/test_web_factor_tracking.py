"""Only original authenticated tracking intents cross the Web/private boundary."""

import os
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

import pytest
from fastapi import FastAPI

from rquant.web.settings import WebSettings
from tests.support.web_proxy_identity import TEST_PROXY_PROOF, ResearcherTestClient


@contextmanager
def _app(settings: WebSettings, **kwargs: Any) -> Iterator[FastAPI]:
    from rquant.web.app import create_app

    # /private/tmp has a writable ancestor: keep the synthetic ingress proof in this tree.
    with TemporaryDirectory(
        prefix=".tracking-web-fixture-", dir=Path(__file__).resolve().parents[2]
    ) as directory:
        proof = Path(directory) / "proof"
        proof.write_text(TEST_PROXY_PROOF, encoding="ascii")
        proof.chmod(0o400)
        yield create_app(settings.model_copy(update={"proxy_proof_file": proof}), **kwargs)


def _settings(tmp_path: Path, *, enabled: bool = False) -> WebSettings:
    values = {
        "serving_root": tmp_path / "serving",
        "ingress_socket_path": tmp_path / "web" / "app.sock",
    }
    if enabled:
        values.update(
            factor_tracking_enabled=True,
            factor_tracking_users=frozenset({"alice"}),
            factor_tracking_admission_socket_path=tmp_path / "tracking-private" / "tracking.sock",
            factor_tracking_admission_service_uid=os.geteuid() + 1,
            factor_tracking_admission_shared_gid=os.getegid(),
        )
    return WebSettings(**values)


def test_web_tracking_defaults_closed_and_missing_serving_is_unavailable(tmp_path: Path) -> None:
    with _app(_settings(tmp_path), background=False) as app, ResearcherTestClient(app) as client:
        response = client.get("/api/v1/factors/example/tracking")
        assert response.status_code == 200
        panel = response.json()["data"]
        assert panel["availability"] == "unavailable" and not panel["can_set_tracked"]
        assert panel["summary"] is None


def test_web_tracking_original_recovery_precedes_new_generation_and_requires_csrf(
    tmp_path: Path,
) -> None:
    from rquant.factor.tracking import FactorTrackingOperationResult, FactorTrackingReceipt
    from tests.unit.test_factor_tracking import _registry, _request

    _, _, head = _registry(tmp_path)
    body = _request(head)
    calls = []
    original = FactorTrackingOperationResult(
        original_request=body,
        status="applied",
        receipt=FactorTrackingReceipt(
            command_id=body.command_id,
            factor_id=body.factor_id,
            tracked=True,
            tracking_generation="c" * 32,
            segment_id="c" * 32,
            definition_head=head,
        ),
    )

    class Client:
        def lookup(self, request: object, *, authenticated_actor_id: str) -> object:
            assert request == body and authenticated_actor_id == "alice"
            calls.append("lookup")
            return original

        def resume(self, request: object, *, authenticated_actor_id: str) -> object:
            calls.append("resume")
            return original

        def submit(self, *args: object, **kwargs: object) -> object:
            raise AssertionError("original recovery must not preflight or submit")

    with (
        _app(
            _settings(tmp_path, enabled=True),
            factor_tracking_admission_client=Client(),
            background=False,
        ) as app,
        ResearcherTestClient(app, headers={"x-rquant-user": "alice"}) as client,
    ):
        assert (
            client.post(
                "/api/v1/factors/tracking/commands", json=body.model_dump(mode="json")
            ).status_code
            == 403
        )
        for suffix in ("", "/resume", "/retry"):
            response = client.post(
                "/api/v1/factors/tracking/commands" + suffix,
                json=body.model_dump(mode="json"),
                headers={"x-rquant-csrf": "1"},
            )
            assert response.status_code == 200, response.text
            assert response.json()["data"] == original.model_dump(mode="json")
        assert calls == ["lookup", "resume"] * 3
        with ResearcherTestClient(app, headers={"x-rquant-user": "bob"}) as other:
            assert (
                other.post(
                    "/api/v1/factors/tracking/commands",
                    json=body.model_dump(mode="json"),
                    headers={"x-rquant-csrf": "1"},
                ).status_code
                == 403
            )


def test_tracking_projection_validates_entire_pair_and_keeps_empty_distinct(tmp_path: Path) -> None:
    from rquant.factor.tracking import FactorTrackingStore
    from rquant.factor.tracking_serving import (
        project_factor_tracking_projections,
        project_factor_tracking_snapshot,
        validate_factor_tracking_projections,
    )
    from tests.unit.test_factor_tracking import _registry, _request

    _, registry_identity, head = _registry(tmp_path)
    store = FactorTrackingStore(tmp_path / "tracking.sqlite")
    identity = store.initialize()
    at = _request(head).requested_at
    empty = project_factor_tracking_snapshot(
        identity, registry_identity=registry_identity, available_at=at
    )
    pair = project_factor_tracking_projections(empty)
    assert validate_factor_tracking_projections({p.table_name: p for p in pair}) == empty
    assert not empty.panels
    with pytest.raises(ValueError):
        validate_factor_tracking_projections({pair[0].table_name: pair[0]})
    store.set_tracked(
        _request(head),
        actor_id="alice",
        expected_identity=identity,
        registry_identity=registry_identity,
    )
    populated = project_factor_tracking_snapshot(
        identity, registry_identity=registry_identity, available_at=at
    )
    panel = populated.panels[0]
    assert panel.tracked and panel.status == "waiting" and panel.summary is None
    pair = project_factor_tracking_projections(populated)
    broken = pair[1].model_copy(update={"rows": ()})
    with pytest.raises(ValueError):
        validate_factor_tracking_projections(
            {pair[0].table_name: pair[0], broken.table_name: broken}
        )
