from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

from rquant.cli import build_parser, cmd_runtime_code, main
from rquant.runtime_code_attestation import CodeTrustEvidence
from rquant.runtime_code_generation import RuntimeCodeInstallReceipt
from rquant.runtime_code_operations import (
    RUNTIME_CODE_EXIT_CONFLICT,
    RuntimeCodeInspectResult,
    RuntimeCodeInstallPlan,
    RuntimeCodeInstallResult,
    RuntimeCodeMigrationRequest,
    RuntimeCodeOperationError,
    RuntimeCodePackageCeremonyRequest,
    RuntimeCodePackageResult,
    RuntimeCodeRotateCeremonyRequest,
)


@pytest.mark.parametrize("action", ("package", "install", "rotate", "inspect", "dry-run"))
def test_runtime_code_cli_registers_explicit_operator_actions(action: str) -> None:
    argv = [
        "runtime-code",
        action,
        "--runtime-code-config",
        "/etc/rquant/runtime-code-bootstrap.json",
        "--runtime-code-trusted-base",
        "/etc/rquant",
        "--runtime-code-authority-uid",
        "0",
        "--runtime-code-authority-gid",
        "0",
        "--format",
        "json",
    ]
    if action != "inspect":
        argv.extend(("--request", "/etc/rquant/runtime-code-operation.json"))
    parsed = build_parser().parse_args(argv)

    assert parsed.command == "runtime-code"
    assert parsed.action == action
    assert parsed.runtime_code_config == Path("/etc/rquant/runtime-code-bootstrap.json")
    assert parsed.format == "json"


@pytest.mark.parametrize(
    "legacy",
    (
        ("--expected-checkout-root", "/srv/rquant"),
        ("--checkout-root", "/srv/rquant"),
        ("--trusted-git-path", "/usr/bin/git"),
    ),
)
def test_runtime_code_cli_rejects_legacy_runtime_git_arguments(
    legacy: tuple[str, str],
) -> None:
    with pytest.raises(SystemExit):
        build_parser().parse_args(
            [
                "runtime-code",
                "inspect",
                "--runtime-code-config",
                "/etc/rquant/runtime-code-bootstrap.json",
                "--runtime-code-trusted-base",
                "/etc/rquant",
                "--runtime-code-authority-uid",
                "0",
                "--runtime-code-authority-gid",
                "0",
                *legacy,
            ]
        )


def test_runtime_code_real_handler_emits_structured_success_and_stable_error(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    from rquant import runtime_code_operations

    args = argparse.Namespace(
        action="inspect",
        format="json",
        runtime_code_config=Path("/etc/rquant/runtime-code-bootstrap.json"),
        runtime_code_trusted_base=Path("/etc/rquant"),
        runtime_code_authority_uid=0,
        runtime_code_authority_gid=0,
    )
    configuration = SimpleNamespace()
    result = RuntimeCodeInspectResult(
        runtime_root=Path("/etc/rquant/runtime-code"),
        generation_id="1" * 64,
        promotion_sequence=7,
        provenance_commit="2" * 40,
        attestation_sha256="3" * 64,
        content_root_sha256="4" * 64,
    )
    monkeypatch.setattr(
        runtime_code_operations,
        "load_runtime_code_bootstrap_configuration",
        lambda *_args, **_kwargs: configuration,
    )
    monkeypatch.setattr(
        runtime_code_operations,
        "compose_runtime_code_generation_operator",
        lambda _configuration, **_kwargs: SimpleNamespace(inspect=lambda: result),
    )

    assert cmd_runtime_code(args) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "ok"
    assert payload["exit_code"] == 0
    assert payload["result"]["generation_id"] == "1" * 64

    monkeypatch.setattr(
        runtime_code_operations,
        "load_runtime_code_bootstrap_configuration",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            RuntimeCodeOperationError(
                "legacy runtime residue blocks migration",
                exit_code=RUNTIME_CODE_EXIT_CONFLICT,
            )
        ),
    )
    assert cmd_runtime_code(args) == RUNTIME_CODE_EXIT_CONFLICT
    failure = json.loads(capsys.readouterr().out)
    assert failure == {
        "action": "inspect",
        "exit_code": RUNTIME_CODE_EXIT_CONFLICT,
        "message": "legacy runtime residue blocks migration",
        "status": "error",
    }


def _cli_result(action: str) -> object:
    evidence = CodeTrustEvidence(
        generation_id="1" * 64,
        attestation_sha256="3" * 64,
        content_root_sha256="4" * 64,
        promotion_sequence=7,
        provenance_commit="2" * 40,
    )
    if action in {"package", "rotate"}:
        return RuntimeCodePackageResult(
            status="packaged" if action == "package" else "rotated",
            output_root=Path("/etc/rquant/package"),
            generation_id=evidence.generation_id,
            attestation_sha256=evidence.attestation_sha256,
            bundle_sha256="5" * 64,
            content_root_sha256=evidence.content_root_sha256,
            promotion_sequence=evidence.promotion_sequence,
            previous_receipt_sha256="6" * 64,
            external_promotion_required=True,
        )
    if action == "install":
        return RuntimeCodeInstallResult(
            receipt=RuntimeCodeInstallReceipt(
                generation_id=evidence.generation_id,
                previous_generation_id=None,
                write_performed=True,
                evidence=evidence,
            ),
            checks=("formal-service-artifacts-and-argv-verified",),
        )
    if action == "dry-run":
        return RuntimeCodeInstallPlan(
            generation_id=evidence.generation_id,
            previous_generation_id=None,
            promotion_sequence=evidence.promotion_sequence,
            checks=("formal-service-artifacts-and-argv-verified",),
        )
    return RuntimeCodeInspectResult(
        runtime_root=Path("/etc/rquant/runtime-code"),
        generation_id=evidence.generation_id,
        promotion_sequence=evidence.promotion_sequence,
        provenance_commit=evidence.provenance_commit,
        attestation_sha256=evidence.attestation_sha256,
        content_root_sha256=evidence.content_root_sha256,
    )


def _cli_request(action: str) -> object:
    if action == "package":
        return RuntimeCodePackageCeremonyRequest.model_construct(
            package=object(),
            runtime_key_id="runtime-key",
            runtime_private_key_path=Path("/etc/rquant/runtime.private.pem"),
            promotion_private_key_path=Path("/etc/rquant/promotion.private.pem"),
        )
    if action == "rotate":
        return RuntimeCodeRotateCeremonyRequest.model_construct(
            rotation=object(),
            promotion_private_key_path=Path("/etc/rquant/promotion.private.pem"),
        )
    return RuntimeCodeMigrationRequest.model_construct(
        install=object(),
        formal_services=(),
        expected_configuration_path=Path("/etc/rquant/runtime-code-bootstrap.json"),
        expected_trusted_base=Path("/etc/rquant"),
        expected_authority_uid=0,
        expected_authority_gid=0,
        legacy_paths=(),
    )


@pytest.mark.parametrize("action", ("package", "install", "rotate", "inspect", "dry-run"))
@pytest.mark.parametrize("output_format", ("json", "text"))
def test_runtime_code_real_cli_success_contract_for_every_action(
    action: str,
    output_format: str,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    from rquant import runtime_code_operations

    result = _cli_result(action)
    operator = SimpleNamespace(
        package=lambda *_args, **_kwargs: result,
        install=lambda *_args, **_kwargs: result,
        rotate=lambda *_args, **_kwargs: result,
        inspect=lambda *_args, **_kwargs: result,
        dry_run=lambda *_args, **_kwargs: result,
    )
    configuration = SimpleNamespace(
        runtime_keys=(SimpleNamespace(key_id="runtime-key"),),
        promotion_key=object(),
    )
    monkeypatch.setattr(
        runtime_code_operations,
        "load_runtime_code_bootstrap_configuration",
        lambda *_args, **_kwargs: configuration,
    )
    monkeypatch.setattr(
        runtime_code_operations,
        "compose_runtime_code_generation_operator",
        lambda *_args, **_kwargs: operator,
    )
    monkeypatch.setattr(
        runtime_code_operations,
        "load_runtime_code_operation_request",
        lambda *_args, **_kwargs: _cli_request(action),
    )
    monkeypatch.setattr(
        runtime_code_operations,
        "offline_contract_signer",
        lambda *_args, **_kwargs: object(),
    )
    argv = [
        "rquant",
        "runtime-code",
        action,
        "--runtime-code-config",
        "/etc/rquant/runtime-code-bootstrap.json",
        "--runtime-code-trusted-base",
        "/etc/rquant",
        "--runtime-code-authority-uid",
        "0",
        "--runtime-code-authority-gid",
        "0",
        "--format",
        output_format,
    ]
    if action != "inspect":
        argv.extend(("--request", "/etc/rquant/runtime-code-operation.json"))
    monkeypatch.setattr(sys, "argv", argv)

    assert main() == 0
    captured = capsys.readouterr()
    if output_format == "json":
        payload = json.loads(captured.out)
        assert payload["action"] == action
        assert payload["status"] == "ok"
        assert payload["exit_code"] == 0
        if action == "install":
            assert payload["result"]["receipt"]["generation_id"] == "1" * 64
        else:
            assert payload["result"]["generation_id"] == "1" * 64
    else:
        assert captured.out.splitlines()[0] == f"runtime-code {action}: ok"
        assert captured.err == ""


@pytest.mark.parametrize("action", ("package", "install", "rotate", "inspect", "dry-run"))
def test_runtime_code_real_cli_error_contract_for_every_action(
    action: str,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    from rquant import runtime_code_operations

    monkeypatch.setattr(
        runtime_code_operations,
        "load_runtime_code_bootstrap_configuration",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            RuntimeCodeOperationError(
                "runtime code bootstrap configuration is invalid",
                exit_code=RUNTIME_CODE_EXIT_CONFLICT,
            )
        ),
    )
    argv = [
        "rquant",
        "runtime-code",
        action,
        "--runtime-code-config",
        "/etc/rquant/runtime-code-bootstrap.json",
        "--runtime-code-trusted-base",
        "/etc/rquant",
        "--runtime-code-authority-uid",
        "0",
        "--runtime-code-authority-gid",
        "0",
        "--format",
        "json",
    ]
    if action != "inspect":
        argv.extend(("--request", "/etc/rquant/runtime-code-operation.json"))
    monkeypatch.setattr(sys, "argv", argv)

    assert main() == RUNTIME_CODE_EXIT_CONFLICT
    assert json.loads(capsys.readouterr().out) == {
        "action": action,
        "exit_code": RUNTIME_CODE_EXIT_CONFLICT,
        "message": "runtime code bootstrap configuration is invalid",
        "status": "error",
    }
