from __future__ import annotations

from pathlib import Path

import pytest

from rquant.cli import build_parser
from rquant.formal_runtime_command import (
    FormalRuntimeCommandError,
    compose_formal_daemon_argv,
    inspect_formal_systemd_service,
)

ROOT = Path(__file__).resolve().parents[2]
UNIT = ROOT / "deploy/systemd/rquant-lab-claim-finalizer.service"
WRAPPER = ROOT / "scripts/run-lab-daemon.py"


def test_checked_in_finalizer_unit_composes_real_parser_argv_without_legacy() -> None:
    inspected = inspect_formal_systemd_service(
        unit_path=UNIT,
        wrapper_source_path=WRAPPER,
    )

    preflight = build_parser().parse_args(list(inspected.preflight_argv))
    daemon_argv = compose_formal_daemon_argv(
        inspected.wrapper,
        deployment_generation="a" * 40,
        deployment_generation_fd=17,
        startup_deadline_monotonic=9_999_999_999.0,
    )
    daemon = build_parser().parse_args(list(daemon_argv))

    assert preflight.command == "runtime-code"
    assert preflight.action == "dry-run"
    assert daemon.command == "lab-claim-finalizer"
    assert daemon.runtime_code_config == Path("/etc/rquant/runtime-code-bootstrap.json")
    assert daemon.runtime_code_trusted_base == Path("/etc/rquant")
    assert daemon.runtime_code_authority_uid == 0
    assert daemon.runtime_code_authority_gid == 0
    assert daemon.deployment_generation == "a" * 40
    assert daemon.deployment_lock_path == "/run/rquant-lab-claim-finalizer/deployment.lock"
    assert daemon.deployment_generation_fd == 17
    assert daemon.startup_deadline_monotonic == 9_999_999_999.0
    unit_text = UNIT.read_text(encoding="utf-8")
    assert "--expected-checkout-root" not in unit_text
    assert "--trusted-git-path" not in unit_text


@pytest.mark.parametrize(
    ("old", "new", "match"),
    (
        (
            "--runtime-code-authority-gid 0",
            "--trusted-git-path /usr/bin/git",
            "legacy",
        ),
        (
            "--runtime-code-trusted-base /etc/rquant",
            "",
            "required immutable binding",
        ),
        (
            "-- lab-claim-finalizer",
            "-- lab-claim-finalizer --unknown-drift",
            "unknown",
        ),
    ),
)
def test_static_unit_inspection_fails_closed_on_drift(
    tmp_path: Path,
    old: str,
    new: str,
    match: str,
) -> None:
    unit = tmp_path / UNIT.name
    raw = UNIT.read_text(encoding="utf-8")
    assert old in raw
    unit.write_text(raw.replace(old, new, 1), encoding="utf-8")

    with pytest.raises(FormalRuntimeCommandError, match=match):
        inspect_formal_systemd_service(unit_path=unit, wrapper_source_path=WRAPPER)


def test_static_wrapper_inspection_fails_closed_when_typed_composition_drifts(
    tmp_path: Path,
) -> None:
    wrapper = tmp_path / WRAPPER.name
    raw = WRAPPER.read_text(encoding="utf-8")
    assert "compose_formal_daemon_argv" in raw
    wrapper.write_text(
        raw.replace("compose_formal_daemon_argv", "compose_untrusted_argv"),
        encoding="utf-8",
    )

    with pytest.raises(FormalRuntimeCommandError, match="wrapper"):
        inspect_formal_systemd_service(unit_path=UNIT, wrapper_source_path=wrapper)
