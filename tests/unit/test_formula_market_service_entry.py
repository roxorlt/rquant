"""The real PageControl entrypoint enables formula admission only from private local config."""

from __future__ import annotations

import os
from pathlib import Path

import pytest

import rquant.page_control_service as page_control_service
from rquant.formula_market_page_backend import FormulaMarketPageBackendConfig
from rquant.page_control import PageControlService, PageControlStatus
from rquant.runtime_deployment_profile import PageControlRuntimeProfile
from rquant.strict_json import canonical_json_bytes
from tests.unit.test_formula_market_admission import _command
from tests.unit.test_formula_market_run import _history, _market
from tests.unit.test_page_control_service import COMMIT, _install_page_control_profile


def _entry_profile(tmp_path: Path) -> Path:
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
                    "active_key_id": "canvas-local-v1",
                    "active_public_key_pem": "local-test-public-key",
                    "signer_command": ("/test/canvas-signer",),
                    "consumer_service_id": "page-control.local-test.v1",
                    "consumer_instance_id": "page-control-local-test",
                },
            }
        ),
    )
    return runtime_root


def _private_config(tmp_path: Path) -> Path:
    market, history = _market(tmp_path), _history(tmp_path)
    config = FormulaMarketPageBackendConfig(
        universe_root=market[0],
        projection_root=history[0],
        state_path=tmp_path / "state" / "formula-jobs.sqlite",
        artifact_directory=tmp_path / "results",
    )
    path = tmp_path / "formula-market-config.json"
    path.write_bytes(canonical_json_bytes(config.model_dump(mode="json")))
    os.chmod(path, 0o600)
    return path


def _isolate_entrypoint(
    monkeypatch: pytest.MonkeyPatch,
    *,
    command_id: str,
) -> list[object]:
    observed: list[object] = []

    class TestSigningClient:
        def __init__(
            self,
            *,
            command: tuple[str, ...],
            key_id: str,
            timeout_seconds: float,
        ) -> None:
            self.key_id = key_id

        def sign(self, *, namespace: str, payload: bytes) -> str:
            return "local-test-signature"

    class TestKeyring:
        def __init__(
            self,
            *,
            active_key_id: str,
            active_public_key: bytes,
            previous_public_keys: dict[str, bytes],
        ) -> None:
            self.active_key_id = active_key_id

        def verify_detached_payload(
            self,
            *,
            key_id: str,
            payload: bytes,
            signature: str,
            require_active: bool,
        ) -> bool:
            return True

    class NoSocketServer:
        def __init__(self, address: tuple[str, int], handler: type) -> None:
            observed.append(("bind", address))

        def serve_forever(self) -> None:
            service = next(item for item in observed if isinstance(item, PageControlService))
            observed.append(service.submit(_command(command_id)))

        def server_close(self) -> None:
            observed.append("closed")

    def capture_handler(service: PageControlService) -> type:
        observed.append(service)
        return object

    def server_class_for_host(host: str) -> type[NoSocketServer]:
        assert host == "127.0.0.1"
        return NoSocketServer

    monkeypatch.setattr(
        page_control_service, "SecureCanvasPublicationSigningClient", TestSigningClient
    )
    monkeypatch.setattr(page_control_service, "Ed25519CanvasPublicationKeyring", TestKeyring)
    monkeypatch.setattr(page_control_service, "handler_for", capture_handler)
    monkeypatch.setattr(page_control_service, "_server_class_for_host", server_class_for_host)
    return observed


def test_real_entrypoint_is_disabled_by_default_and_opt_in_queues(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime_root = _entry_profile(tmp_path)
    config_path = _private_config(tmp_path)
    observed = _isolate_entrypoint(monkeypatch, command_id="formula-entry-disabled")

    page_control_service.main(runtime_root=runtime_root, expected_commit=COMMIT)
    default_receipt = next(
        item for item in observed if getattr(item, "command_id", None) == "formula-entry-disabled"
    )
    assert default_receipt.status is PageControlStatus.FAILED
    assert default_receipt.error == "RuntimeError: formula market backend is unavailable"
    assert observed[-1] == "closed"

    observed = _isolate_entrypoint(monkeypatch, command_id="formula-entry-enabled")
    page_control_service.main(
        argv=[
            "--manifest",
            str(tmp_path / "wrapper-manifest.json"),
            "--control-root",
            str(tmp_path / "control"),
            "--expected-commit",
            COMMIT,
            "--expected-generation",
            "b" * 64,
            "--formula-market-config",
            str(config_path),
        ],
        runtime_root=runtime_root,
    )
    enabled_receipt = next(
        item for item in observed if getattr(item, "command_id", None) == "formula-entry-enabled"
    )
    assert enabled_receipt.status is PageControlStatus.SUCCEEDED
    assert enabled_receipt.result is not None
    assert enabled_receipt.result["outcome"] == "task_queued"
    service = next(item for item in observed if isinstance(item, PageControlService))
    assert service.consumer.formula_market_backend is not None
    assert (
        service.consumer.formula_market_backend.store.status(
            enabled_receipt.result["task_id"]
        ).status
        == "queued"
    )


def test_loose_private_config_fails_before_service_binds(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime_root = _entry_profile(tmp_path)
    config_path = _private_config(tmp_path)
    os.chmod(config_path, 0o644)

    def forbidden_server_class(_host: str) -> type:
        pytest.fail("unsafe config reached HTTP binding")

    _isolate_entrypoint(monkeypatch, command_id="formula-unsafe-config")
    monkeypatch.setattr(page_control_service, "_server_class_for_host", forbidden_server_class)
    with pytest.raises(ValueError, match="config|private|unsafe"):
        page_control_service.main(
            runtime_root=runtime_root,
            expected_commit=COMMIT,
            formula_market_config_path=config_path,
        )
