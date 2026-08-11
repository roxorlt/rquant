from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def test_dedicated_finalizer_unit_is_daemon_only_and_minimally_authorized() -> None:
    from rquant.lab_claim_finalizer_runtime import verify_lab_claim_finalizer_unit

    result = verify_lab_claim_finalizer_unit(ROOT / "deploy" / "systemd")

    assert result.status == "ok", result.details
    unit = (ROOT / "deploy" / "systemd" / "rquant-lab-claim-finalizer.service").read_text(
        encoding="utf-8"
    )
    assert "Type=simple" in unit
    assert "lab-claim-finalizer" in unit
    assert "ExecStartPre=" in unit
    assert "runtime-code dry-run" in unit
    assert "scripts/run-lab-daemon.py" in unit
    assert "--expected-checkout-root" not in unit
    assert "--trusted-git-path" not in unit
    assert "--runtime-code-config /etc/rquant/runtime-code-bootstrap.json" in unit
    assert "--deployment-lock-path /run/rquant-lab-claim-finalizer/deployment.lock" in unit
    assert "WatchdogSec" not in unit
    assert "rquant-runtime-lab-jobs" not in unit


def test_finalizer_unit_uses_the_formal_wrapper_argument_contract() -> None:
    import importlib.util
    import sys

    wrapper_path = ROOT / "scripts" / "run-lab-daemon.py"
    spec = importlib.util.spec_from_file_location("test_finalizer_wrapper", wrapper_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    unit = (ROOT / "deploy" / "systemd" / "rquant-lab-claim-finalizer.service").read_text(
        encoding="utf-8"
    )
    assert module.FORMAL_RUNTIME_WRAPPER_CONTRACT == "rquant-formal-runtime-wrapper/v1"
    assert " formal --runtime-code-config" in unit.split("ExecStart=", maxsplit=1)[1]


def test_offline_trust_cli_keeps_issue_rotate_separate_from_runtime_command() -> None:
    from rquant.cli import build_parser

    parser = build_parser()
    inspected = parser.parse_args(
        ["lab-claim-finalizer-trust", "inspect", "--certificate", "/tmp/certificate.json"]
    )
    assert inspected.command == "lab-claim-finalizer-trust"
    assert inspected.action == "inspect"
    issued = parser.parse_args(
        [
            "lab-claim-finalizer-trust",
            "issue",
            "--root-private-key",
            "/tmp/root.private.pem",
            "--root-public-key",
            "/tmp/root.public.pem",
            "--finalizer-public-key",
            "/tmp/finalizer.public.pem",
            "--store-id",
            "a" * 64,
            "--database-device",
            "1",
            "--database-inode",
            "2",
            "--not-before",
            "2026-08-11T00:00:00+00:00",
            "--expires-at",
            "2026-08-12T00:00:00+00:00",
        ]
    )
    assert issued.action == "issue"
    runtime = parser.parse_args(
        [
            "lab-claim-finalizer-runtime",
            "install",
            "--runtime-root",
            "/tmp/finalizer-runtime",
            "--request",
            "/tmp/install-request.json",
            "--root-public-key",
            "/tmp/root.public.pem",
            "--root-issuer",
            "offline-root",
            "--root-key-id",
            "root-v1",
            "--service-user",
            "root",
            "--service-group",
            "wheel",
            "--dry-run",
        ]
    )
    assert runtime.action == "install"
    assert not hasattr(runtime, "root_private_key")
