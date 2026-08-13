from __future__ import annotations

import socket
from datetime import UTC, datetime
from http.server import ThreadingHTTPServer
from pathlib import Path

import pytest

import rquant.page_control_service as page_control_service
import rquant.runtime_deployment_profile as deployment_profile_module
from rquant.canvas_publication_receipt import CanvasPublicationReceipt
from rquant.page_control import PageControlStatus, SaveCanvas
from rquant.page_control_service import build_page_control_service
from rquant.runtime_deployment_profile import (
    LINUX_PRODUCTION_RUNTIME_ROOT,
    PageControlRuntimeProfile,
    RuntimeDeploymentProfile,
    install_runtime_deployment_profile,
)
from rquant.runtime_service_control import RuntimeServicePlane
from rquant.runtime_service_entrypoint import RuntimeServiceKind, RuntimeServiceManifest
from tests.canvas_ed25519_support import create_canvas_ed25519_test_authority

NOW = datetime(2026, 8, 3, 1, 30, tzinfo=UTC)
COMMIT = "a" * 40


def _runtime_manifest(runtime_root: Path) -> RuntimeServiceManifest:
    return RuntimeServiceManifest(
        service_id="lab-jobs.serving.v1",
        service_kind=RuntimeServiceKind.LAB_JOBS_PUBLISHER,
        plane=RuntimeServicePlane.RESEARCH,
        interval_seconds=30,
        stale_after_seconds=120,
        producer_commit=COMMIT,
        settings={
            "lab_jobs_path": str(runtime_root / "research" / "lab_jobs.sqlite3"),
            "authority_root": str(runtime_root / "research" / "serving-authorities" / "lab-jobs"),
        },
    )


def _install_page_control_profile(
    runtime_root: Path,
    page_control: PageControlRuntimeProfile,
) -> None:
    manifest = _runtime_manifest(runtime_root)
    profile = RuntimeDeploymentProfile(
        producer_commit=COMMIT,
        manifests=(manifest,),
        capability_environment={manifest.service_id: ()},
        page_control=page_control,
    )
    install_runtime_deployment_profile(
        profile,
        runtime_root=runtime_root,
        environ={},
        schema_bootstrap_reason="page-control main test",
    )


def test_page_control_runtime_profile_binds_public_only_canvas_authority(
    tmp_path: Path,
) -> None:
    profile = PageControlRuntimeProfile.model_validate(
        {
            "endpoint": "http://127.0.0.1:8767/v1/commands",
            "outbox_path": tmp_path / "control" / "page-control.sqlite3",
            "data_dir": tmp_path / "page-data",
            "log_dir": tmp_path / "page-logs",
            "canvas_publication": {
                "schema_version": 1,
                "active_key_id": "canvas-v2",
                "active_public_key_pem": "active-public-pem",
                "previous_public_key_pems": {"canvas-v1": "previous-public-pem"},
                "signer_command": ("/test/rquant-canvas-publication-signer",),
                "consumer_service_id": "page-control.production.v1",
                "consumer_instance_id": "page-control-primary",
                "timeout_seconds": 5,
            },
        }
    )

    payload = profile.model_dump(mode="json")
    assert payload["canvas_publication"]["active_key_id"] == "canvas-v2"
    canvas_fields = payload["canvas_publication"]
    assert all("private" not in field.lower() for field in canvas_fields)


def test_main_rejects_missing_canvas_authority_before_binding_http(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime_root = tmp_path / "runtime"
    _install_page_control_profile(
        runtime_root,
        PageControlRuntimeProfile(
            endpoint="http://127.0.0.1:8767/v1/commands",
            outbox_path=runtime_root / "control" / "page-control.sqlite3",
        ),
    )
    monkeypatch.setattr(
        ThreadingHTTPServer,
        "__init__",
        lambda *_args, **_kwargs: pytest.fail("HTTP server must not be constructed"),
    )

    with pytest.raises(ValueError, match="Canvas.*authority|canvas.*authority"):
        page_control_service.main(runtime_root=runtime_root, expected_commit=COMMIT)


def test_main_rejects_unavailable_canvas_signer_before_binding_http(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    authority = create_canvas_ed25519_test_authority(tmp_path / "keys")
    public_key = (tmp_path / "keys" / "canvas-test-v1.public.pem").read_text()
    runtime_root = tmp_path / "runtime"
    _install_page_control_profile(
        runtime_root,
        PageControlRuntimeProfile.model_validate(
            {
                "endpoint": "http://127.0.0.1:8767/v1/commands",
                "outbox_path": runtime_root / "control" / "page-control.sqlite3",
                "data_dir": runtime_root / "serving" / "page-control",
                "log_dir": runtime_root / "control" / "page-control-logs",
                "page_projection_canvas_catalog_root": (
                    runtime_root / "serving" / "page-control" / "canvases"
                ),
                "canvas_publication": {
                    "active_key_id": authority.keyring.active_key_id,
                    "active_public_key_pem": public_key,
                    "signer_command": ("/missing/rquant-canvas-publication-signer",),
                    "consumer_service_id": "page-control.production.v1",
                    "consumer_instance_id": "page-control-primary",
                },
            }
        ),
    )
    monkeypatch.setattr(
        ThreadingHTTPServer,
        "__init__",
        lambda *_args, **_kwargs: pytest.fail("HTTP server must not be constructed"),
    )

    with pytest.raises(RuntimeError, match="signer|capability|unavailable"):
        page_control_service.main(runtime_root=runtime_root, expected_commit=COMMIT)


def test_default_entrypoint_rejects_local_test_profile_before_http_binding(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime_root = tmp_path / "runtime"
    authority = create_canvas_ed25519_test_authority(tmp_path / "keys")
    public_key = (tmp_path / "keys" / "canvas-test-v1.public.pem").read_text()
    profile = RuntimeDeploymentProfile(
        producer_commit=COMMIT,
        manifests=(_runtime_manifest(runtime_root),),
        capability_environment={"lab-jobs.serving.v1": ()},
        page_control=PageControlRuntimeProfile.model_validate(
            {
                "endpoint": "http://127.0.0.1:8767/v1/commands",
                "outbox_path": runtime_root / "control" / "page-control.sqlite3",
                "data_dir": runtime_root / "serving" / "page-control",
                "log_dir": runtime_root / "control" / "page-control-logs",
                "page_projection_canvas_catalog_root": (
                    runtime_root / "serving" / "page-control" / "canvases"
                ),
                "canvas_publication": {
                    "active_key_id": authority.keyring.active_key_id,
                    "active_public_key_pem": public_key,
                    "signer_command": ("/runner-controlled-helper",),
                    "consumer_service_id": "page-control.production.v1",
                    "consumer_instance_id": "page-control-primary",
                },
            }
        ),
    )
    monkeypatch.setattr(
        deployment_profile_module,
        "load_current_runtime_deployment_profile",
        lambda _root: profile,
    )
    monkeypatch.setattr(
        ThreadingHTTPServer,
        "__init__",
        lambda *_args, **_kwargs: pytest.fail("HTTP server must not be constructed"),
    )

    with pytest.raises(ValueError, match="linux-production"):
        page_control_service.main(expected_commit=COMMIT)


def test_explicit_production_root_rejects_local_test_profile_before_signer_probe(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime_root = tmp_path / "runtime"
    authority = create_canvas_ed25519_test_authority(tmp_path / "keys")
    public_key = (tmp_path / "keys" / "canvas-test-v1.public.pem").read_text()
    profile = RuntimeDeploymentProfile(
        producer_commit=COMMIT,
        manifests=(_runtime_manifest(runtime_root),),
        capability_environment={"lab-jobs.serving.v1": ()},
        page_control=PageControlRuntimeProfile.model_validate(
            {
                "endpoint": "http://127.0.0.1:8767/v1/commands",
                "outbox_path": runtime_root / "control" / "page-control.sqlite3",
                "data_dir": runtime_root / "serving" / "page-control",
                "log_dir": runtime_root / "control" / "page-control-logs",
                "page_projection_canvas_catalog_root": (
                    runtime_root / "serving" / "page-control" / "canvases"
                ),
                "canvas_publication": {
                    "active_key_id": authority.keyring.active_key_id,
                    "active_public_key_pem": public_key,
                    "signer_command": ("/runner-controlled-helper",),
                    "consumer_service_id": "page-control.production.v1",
                    "consumer_instance_id": "page-control-primary",
                },
            }
        ),
    )
    monkeypatch.setattr(
        deployment_profile_module,
        "load_current_runtime_deployment_profile",
        lambda root: (
            profile
            if root == LINUX_PRODUCTION_RUNTIME_ROOT
            else pytest.fail("unexpected runtime root")
        ),
    )
    monkeypatch.setattr(
        page_control_service.SecureCanvasPublicationSigningClient,
        "sign",
        lambda *_args, **_kwargs: pytest.fail("signer probe must not run"),
    )
    monkeypatch.setattr(
        ThreadingHTTPServer,
        "__init__",
        lambda *_args, **_kwargs: pytest.fail("HTTP server must not be constructed"),
    )

    with pytest.raises(ValueError, match="linux-production"):
        page_control_service.main(
            runtime_root=LINUX_PRODUCTION_RUNTIME_ROOT,
            expected_commit=COMMIT,
        )


@pytest.mark.parametrize("host", ["127.0.0.1", "localhost"])
def test_main_preserves_ipv4_loopback_binding(host: str) -> None:
    server_class = page_control_service._server_class_for_host(host)

    assert server_class.address_family == socket.AF_INET


@pytest.mark.skipif(not socket.has_ipv6, reason="OS has no IPv6 support")
def test_main_binds_ipv6_loopback_with_ipv6_server() -> None:
    server_class = page_control_service._server_class_for_host("::1")

    assert server_class.address_family == socket.AF_INET6


def test_page_control_service_factory_accepts_canvas_authority_and_restarts_once(
    tmp_path: Path,
) -> None:
    authority = create_canvas_ed25519_test_authority(tmp_path / "keys")
    outbox_path = tmp_path / "page-control.sqlite3"
    data_dir = tmp_path / "data"
    log_dir = tmp_path / "logs"
    service = build_page_control_service(
        outbox_path=outbox_path,
        data_dir=data_dir,
        log_dir=log_dir,
        allowed_lab_export_roots=(tmp_path / "exports",),
        load_default_lab_backend=False,
        canvas_publication_signer=authority.signer,
        canvas_publication_keyring=authority.keyring,
        consumer_service_id="page-control-service-test",
        consumer_instance_id="page-control-service-instance",
        clock=lambda: NOW,
        lease_seconds=1,
    )
    command = SaveCanvas(
        command_id="factory-save-canvas",
        requested_at=NOW,
        name="breakout",
        description="factory injected authority",
        pool_refs=("n-shape-pool1",),
        source="canvas_page",
    )

    first = service.submit(command)
    first_receipt_files = tuple((data_dir / "canvas-publication-receipts").glob("*.json"))
    first_receipt_bytes = first_receipt_files[0].read_bytes()
    restarted = build_page_control_service(
        outbox_path=outbox_path,
        data_dir=data_dir,
        log_dir=log_dir,
        allowed_lab_export_roots=(tmp_path / "exports",),
        load_default_lab_backend=False,
        canvas_publication_signer=authority.signer,
        canvas_publication_keyring=authority.keyring,
        consumer_service_id="page-control-service-test",
        consumer_instance_id="page-control-service-instance",
        clock=lambda: NOW,
        lease_seconds=1,
    )
    duplicate = restarted.submit(command)

    assert first.status is PageControlStatus.SUCCEEDED
    assert duplicate.result == first.result
    assert tuple((data_dir / "canvas-publication-receipts").glob("*.json")) == (
        first_receipt_files[0],
    )
    assert first_receipt_files[0].read_bytes() == first_receipt_bytes
    publication = CanvasPublicationReceipt.model_validate_json(
        first_receipt_files[0].read_text(encoding="utf-8")
    )
    assert publication.claims.consumer_service_id == "page-control-service-test"
    assert publication.claims.consumer_instance_id == "page-control-service-instance"
