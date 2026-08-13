"""Loopback HTTP service for page control commands."""

from __future__ import annotations

import json
import os
import socket
from collections.abc import Callable
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlsplit

from rquant.canvas_publication_receipt import (
    CANVAS_PUBLICATION_PROBE_NAMESPACE,
    CanvasPublicationKeyring,
    CanvasPublicationSigner,
    Ed25519CanvasPublicationKeyring,
    Ed25519CanvasPublicationSigner,
    SecureCanvasPublicationSigningClient,
)
from rquant.config import settings
from rquant.job_center_authority import resolve_current_job_center_authority_binding
from rquant.lab_daemon import load_lab_job_center_authority_manifest
from rquant.lab_page_control import build_lab_page_control_writer
from rquant.page_control import (
    DEFAULT_PAGE_CONTROL_SERVICE_ID,
    LabPageControlBackend,
    PageControlConsumer,
    PageControlOutbox,
    PageControlService,
    parse_page_control_command,
)
from rquant.research_manifest import detect_verified_code_commit
from rquant.runtime_shadow_validation import _ed25519_signing_payload
from rquant.strict_json import canonical_json_bytes

PRODUCTION_CANVAS_SIGNER_COMMAND = (
    "/usr/bin/sudo",
    "-n",
    "/usr/local/libexec/rquant-canvas-publication-signer",
)


class _IPv6ThreadingHTTPServer(ThreadingHTTPServer):
    address_family = socket.AF_INET6


def _server_class_for_host(host: str) -> type[ThreadingHTTPServer]:
    if host == "::1":
        return _IPv6ThreadingHTTPServer
    if host in {"127.0.0.1", "localhost"}:
        return ThreadingHTTPServer
    raise ValueError("page control service must bind to loopback")


def _build_lab_backend() -> object | None:
    code_sha = detect_verified_code_commit(
        trusted_git_path=settings.lab_trusted_git_path,
    )
    deployment_root = os.environ.get("RQUANT_RUNTIME_ROOT", "")
    if code_sha is None or not deployment_root:
        return None
    binding = resolve_current_job_center_authority_binding(
        Path(deployment_root),
        expected_code_sha=code_sha,
        runtime_root=settings.lab_runtime_dir_resolved,
        lab_jobs_path=settings.lab_jobs_path_resolved,
        command_spool_path=settings.lab_job_command_dir_resolved,
        final_artifact_root=settings.lab_final_artifact_dir_resolved,
    )
    manifest = load_lab_job_center_authority_manifest(
        binding.runtime_root / "job-center-authority.json",
        expected_code_sha=code_sha,
        expected_research_root=binding.runtime_root,
        expected_lab_jobs_path=binding.lab_jobs_path,
        expected_command_spool_path=binding.command_spool_path,
        expected_final_artifact_root=binding.final_artifact_root,
        expected_runtime_deployment_root=binding.runtime_deployment_root,
        expected_deployment_profile_id=binding.deployment_profile_id,
        expected_deployment_generation_hash=binding.deployment_generation_hash,
    )
    return build_lab_page_control_writer(manifest)


def build_page_control_service(
    *,
    outbox_path: Path | None = None,
    data_dir: Path | None = None,
    log_dir: Path | None = None,
    allowed_lab_export_roots: tuple[Path, ...] | None = None,
    lab_backend: LabPageControlBackend | None = None,
    load_default_lab_backend: bool = True,
    clock: Callable[[], datetime] | None = None,
    lease_seconds: int = 30,
    consumer_service_id: str = DEFAULT_PAGE_CONTROL_SERVICE_ID,
    consumer_instance_id: str | None = None,
    canvas_publication_signer: CanvasPublicationSigner | None = None,
    canvas_publication_keyring: CanvasPublicationKeyring | None = None,
) -> PageControlService:
    return build_page_control_service_with_dependencies(
        outbox_path=outbox_path,
        data_dir=data_dir,
        log_dir=log_dir,
        allowed_lab_export_roots=allowed_lab_export_roots,
        lab_backend=lab_backend,
        load_default_lab_backend=load_default_lab_backend,
        clock=clock,
        lease_seconds=lease_seconds,
        consumer_service_id=consumer_service_id,
        consumer_instance_id=consumer_instance_id,
        canvas_publication_signer=canvas_publication_signer,
        canvas_publication_keyring=canvas_publication_keyring,
    )


def build_page_control_service_with_dependencies(
    *,
    outbox_path: Path | None = None,
    data_dir: Path | None = None,
    log_dir: Path | None = None,
    allowed_lab_export_roots: tuple[Path, ...] | None = None,
    lab_backend: LabPageControlBackend | None = None,
    load_default_lab_backend: bool = True,
    clock: Callable[[], datetime] | None = None,
    lease_seconds: int = 30,
    consumer_service_id: str = DEFAULT_PAGE_CONTROL_SERVICE_ID,
    consumer_instance_id: str | None = None,
    canvas_publication_signer: CanvasPublicationSigner | None = None,
    canvas_publication_keyring: CanvasPublicationKeyring | None = None,
) -> PageControlService:
    if (canvas_publication_signer is None) != (canvas_publication_keyring is None):
        raise ValueError(
            "CanvasPublicationReceipt signer and public keyring must be provided together"
        )
    if allowed_lab_export_roots is None:
        configured_roots = os.environ.get("RQUANT_PAGE_CONTROL_ALLOWED_EXPORT_ROOTS", "")
        allowed_roots = tuple(
            Path(value.strip()) for value in configured_roots.split(os.pathsep) if value.strip()
        ) or (settings.lab_runtime_dir_resolved / "exports",)
    else:
        allowed_roots = allowed_lab_export_roots
    outbox = PageControlOutbox(
        Path(
            outbox_path
            or os.environ.get(
                "RQUANT_PAGE_CONTROL_OUTBOX",
                settings.data_dir / "page-control.sqlite3",
            )
        )
    )
    return PageControlService(
        outbox=outbox,
        consumer=PageControlConsumer(
            outbox=outbox,
            data_dir=settings.data_dir if data_dir is None else data_dir,
            log_dir=settings.log_dir if log_dir is None else log_dir,
            allowed_lab_export_roots=allowed_roots,
            lab_backend=(
                lab_backend
                if lab_backend is not None or not load_default_lab_backend
                else _build_lab_backend()
            ),
            clock=clock,
            lease_seconds=lease_seconds,
            consumer_id=consumer_instance_id,
            consumer_service_id=consumer_service_id,
            canvas_publication_signer=canvas_publication_signer,
            canvas_publication_keyring=canvas_publication_keyring,
        ),
    )


def handler_for(service: PageControlService) -> type[BaseHTTPRequestHandler]:
    class PageControlHandler(BaseHTTPRequestHandler):
        def do_POST(self) -> None:  # noqa: N802
            if self.path != "/v1/commands":
                self.send_error(404)
                return
            try:
                content_length = int(self.headers.get("Content-Length", "0"))
                if not 1 <= content_length <= 1024 * 1024:
                    raise ValueError("request body must be between 1 byte and 1 MiB")
                payload = json.loads(self.rfile.read(content_length))
                command = parse_page_control_command(payload)
                response = service.submit(command).model_dump(mode="json")
            except Exception as exc:
                self._write_json(
                    400,
                    {"error": f"{type(exc).__name__}: {exc}"},
                )
                return
            self._write_json(200, response)

        def log_message(self, format: str, *args: object) -> None:
            return

        def _write_json(self, status: int, payload: object) -> None:
            body = json.dumps(payload, ensure_ascii=True).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    return PageControlHandler


def main(
    *,
    runtime_root: Path | None = None,
    expected_commit: str | None = None,
) -> None:
    from rquant.runtime_deployment_profile import (
        LINUX_PRODUCTION_RUNTIME_ROOT,
        PRODUCTION_CANVAS_SIGNER_COMMAND,
        load_current_runtime_deployment_profile,
    )

    resolved_runtime_root = runtime_root or LINUX_PRODUCTION_RUNTIME_ROOT
    is_production_root = (
        Path(os.path.abspath(resolved_runtime_root)) == LINUX_PRODUCTION_RUNTIME_ROOT
    )
    profile = load_current_runtime_deployment_profile(resolved_runtime_root)
    resolved_commit = expected_commit or detect_verified_code_commit(
        trusted_git_path=settings.lab_trusted_git_path,
    )
    if resolved_commit is None or profile.producer_commit != resolved_commit:
        raise ValueError("PageControl runtime profile commit is not the running commit")
    if is_production_root and profile.runtime_mode != "linux-production":
        raise ValueError("production PageControl entrypoint requires a linux-production profile")
    page_profile = profile.page_control
    if page_profile is None or page_profile.canvas_publication is None:
        raise ValueError("Canvas publication authority is missing from runtime profile")
    if page_profile.data_dir is None or page_profile.log_dir is None:
        raise ValueError("Canvas publication storage authority is missing from runtime profile")
    if page_profile.page_projection_canvas_catalog_root != page_profile.data_dir / "canvases":
        raise ValueError(
            "Canvas catalog root must remain derived from the PageControl data directory"
        )
    canvas_profile = page_profile.canvas_publication
    if profile.runtime_mode == "linux-production" and (
        canvas_profile.signer_command != PRODUCTION_CANVAS_SIGNER_COMMAND
    ):
        raise ValueError("production Canvas signer must use the fixed protected capability")
    keyring = Ed25519CanvasPublicationKeyring(
        active_key_id=canvas_profile.active_key_id,
        active_public_key=canvas_profile.active_public_key_pem.encode("utf-8"),
        previous_public_keys={
            key_id: public_key.encode("utf-8")
            for key_id, public_key in canvas_profile.previous_public_key_pems.items()
        },
    )
    signing_client = SecureCanvasPublicationSigningClient(
        command=canvas_profile.signer_command,
        key_id=canvas_profile.active_key_id,
        timeout_seconds=canvas_profile.timeout_seconds,
    )
    probe_body = canonical_json_bytes(
        {
            "contract": "serving-canvas-publication-startup-probe/v1",
            "profile_id": profile.profile_id,
            "producer_commit": profile.producer_commit,
            "consumer_service_id": canvas_profile.consumer_service_id,
            "consumer_instance_id": canvas_profile.consumer_instance_id,
        }
    )
    probe_payload = _ed25519_signing_payload(
        namespace=CANVAS_PUBLICATION_PROBE_NAMESPACE,
        payload=probe_body,
    )
    probe_signature = signing_client.sign(
        namespace=CANVAS_PUBLICATION_PROBE_NAMESPACE,
        payload=probe_payload,
    )
    if not keyring.verify_detached_payload(
        key_id=canvas_profile.active_key_id,
        payload=probe_payload,
        signature=probe_signature,
        require_active=True,
    ):
        raise RuntimeError("Canvas publication signer capability is not the active key")
    endpoint = urlsplit(page_profile.endpoint)
    host = endpoint.hostname or ""
    port = endpoint.port or 0
    if (
        endpoint.scheme != "http"
        or host not in {"127.0.0.1", "::1", "localhost"}
        or endpoint.path != "/v1/commands"
        or endpoint.query
        or endpoint.fragment
        or port <= 0
    ):
        raise ValueError("page control endpoint must be an explicit loopback command URL")
    service = build_page_control_service(
        outbox_path=page_profile.outbox_path,
        data_dir=page_profile.data_dir,
        log_dir=page_profile.log_dir,
        allowed_lab_export_roots=(page_profile.data_dir / "exports",),
        load_default_lab_backend=False,
        consumer_service_id=canvas_profile.consumer_service_id,
        consumer_instance_id=canvas_profile.consumer_instance_id,
        canvas_publication_signer=Ed25519CanvasPublicationSigner(
            key_id=canvas_profile.active_key_id,
            client=signing_client,
        ),
        canvas_publication_keyring=keyring,
    )
    server_class = _server_class_for_host(host)
    server = server_class((host, port), handler_for(service))
    server.serve_forever()


if __name__ == "__main__":
    main()
