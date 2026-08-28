from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


UNIT_PATH = ROOT / "deploy" / "systemd" / "rquant-lab-claim-finalizer.service"


def test_dedicated_finalizer_unit_is_daemon_only_and_minimally_authorized() -> None:
    """Amended per Codex round-3 verdict 2026-08-28, item RQ-WI-R2-P1-02.

    Three of the assertions here used to *require* the checkout: an `ExecStartPre`, a
    `runtime-code dry-run` on the unit line, and `scripts/run-lab-daemon.py` in the
    `ExecStart`. All three named lighthouse-writable code, and the dry-run in particular
    validated the deployment using the tree being validated. They are inverted rather than
    dropped, so a regression puts them back at the cost of a red test. What the unit is
    still held to — one daemon, no watchdog, no lab-jobs authority — is unchanged.
    """

    from rquant.lab_claim_finalizer_runtime import verify_lab_claim_finalizer_unit

    result = verify_lab_claim_finalizer_unit(ROOT / "deploy" / "systemd")

    assert result.status == "ok", result.details
    unit = UNIT_PATH.read_text(encoding="utf-8")
    assert "Type=simple" in unit
    assert "lab_claim_finalizer" in unit
    assert "ExecStartPre=" not in unit
    assert "runtime-code dry-run" not in unit
    assert "scripts/run-lab-daemon.py" not in unit
    assert "--expected-checkout-root" not in unit
    assert "--trusted-git-path" not in unit
    assert "--runtime-code-config" not in unit
    assert "--deployment-lock-path" not in unit
    assert "WatchdogSec" not in unit
    assert "rquant-runtime-lab-jobs" not in unit


def test_finalizer_unit_uses_the_formal_wrapper_argument_contract() -> None:
    """The contract is now the wrapper's role argument, not the checkout script's source.

    This used to load `scripts/run-lab-daemon.py` from the checkout and read a constant out
    of it, which is the same circularity the verdict is about. The unit's start command is
    now compared token by token against the fixed root-owned wrapper, and the argument
    contract it satisfies is the one `rquant.formal_runtime_command` inspects.
    """

    import configparser

    from rquant.formal_runtime_command import (
        FINALIZER_ROLE,
        FORMAL_RUNTIME_WRAPPER_CONTRACT,
        inspect_formal_systemd_service,
    )

    parser = configparser.ConfigParser(interpolation=None, strict=True)
    parser.optionxform = str
    parser.read_string(UNIT_PATH.read_text(encoding="utf-8"))
    inspected = inspect_formal_systemd_service(unit_path=UNIT_PATH)

    assert FORMAL_RUNTIME_WRAPPER_CONTRACT == "rquant-formal-runtime-wrapper/v1"
    assert inspected.role == FINALIZER_ROLE
    assert parser["Service"]["ExecStart"].split() == [
        "/usr/local/libexec/rquant-workload-arbiter",
        "research",
        "--",
        "/usr/bin/python3.11",
        "-I",
        "-S",
        "/usr/local/libexec/rquant-runtime-exec.pyz",
        "--role",
        FINALIZER_ROLE,
    ]


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
