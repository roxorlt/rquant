from __future__ import annotations

import argparse
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from rquant.cli import build_parser, cmd_runtime_code
from rquant.runtime_code_operations import (
    RUNTIME_CODE_EXIT_CONFLICT,
    RuntimeCodeInspectResult,
    RuntimeCodeOperationError,
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
