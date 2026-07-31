"""Static contracts for isolated runtime systemd service templates."""

from __future__ import annotations

import configparser
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
SYSTEMD = ROOT / "deploy" / "systemd"
PLANES = ("live", "serving", "research")
RUNTIME_ROOT = "/home/lighthouse/rquant/data/runtime"
CONTROL_ROOT = f"{RUNTIME_ROOT}/control"
ENVIRONMENT_FILE = f"{RUNTIME_ROOT}/runtime.env"
EXPECTED_EXECUTABLE = "/home/lighthouse/rquant/.venv/bin/python"
EXPECTED_MODULE = "rquant.runtime_service_main"


def _path(plane: str) -> Path:
    return SYSTEMD / f"rquant-runtime-{plane}@.service"


def _load(plane: str) -> configparser.ConfigParser:
    parser = configparser.ConfigParser(interpolation=None, strict=True)
    parser.optionxform = str
    with _path(plane).open(encoding="utf-8") as stream:
        parser.read_file(stream)
    return parser


@pytest.mark.parametrize("plane", PLANES)
def test_runtime_template_has_fixed_identity_entrypoint_and_manifest(
    plane: str,
) -> None:
    parser = _load(plane)
    unit = parser["Unit"]
    service = parser["Service"]

    assert unit["OnFailure"] == "rquant-alert@%n.service"
    assert service["Type"] == "simple"
    assert service["User"] == "lighthouse"
    assert service["Group"] == "lighthouse"
    assert service["WorkingDirectory"] == "/home/lighthouse/rquant"
    expected_environment_file = (
        f"{RUNTIME_ROOT}/secrets/%i.env" if plane == "live" else ENVIRONMENT_FILE
    )
    assert service["EnvironmentFile"] == expected_environment_file
    assert service["Environment"] == "RQUANT_DISABLE_DOTENV=1"
    assert service["Slice"] == f"rquant-{plane}.slice"

    command = service["ExecStart"]
    assert command.startswith(f"{EXPECTED_EXECUTABLE} -m {EXPECTED_MODULE} ")
    assert (
        f"--manifest {RUNTIME_ROOT}/manifests/%i.json" in command
    )
    assert f"--control-root {CONTROL_ROOT}" in command
    assert "--expected-commit ${RQUANT_RUNTIME_COMMIT}" in command


@pytest.mark.parametrize("plane", PLANES)
def test_runtime_template_has_bounded_restart_and_shutdown(plane: str) -> None:
    parser = _load(plane)
    unit = parser["Unit"]
    service = parser["Service"]

    assert 1 <= int(unit["StartLimitBurst"]) <= 5
    assert 60 <= int(unit["StartLimitIntervalSec"].removesuffix("s")) <= 3600
    assert service["Restart"] == "on-failure"
    assert 5 <= int(service["RestartSec"].removesuffix("s")) <= 60
    assert 30 <= int(service["TimeoutStartSec"].removesuffix("s")) <= 300
    assert 15 <= int(service["TimeoutStopSec"].removesuffix("s")) <= 120
    assert service["SuccessExitStatus"] == "0"


@pytest.mark.parametrize("plane", PLANES)
def test_runtime_template_is_hardened_without_shell_or_manifest_secrets(
    plane: str,
) -> None:
    parser = _load(plane)
    service = parser["Service"]
    raw = _path(plane).read_text(encoding="utf-8")

    assert service["NoNewPrivileges"] == "true"
    assert service["PrivateTmp"] == "true"
    assert service["PrivateDevices"] == "true"
    assert service["ProtectSystem"] == "strict"
    assert service["ProtectHome"] == "read-only"
    assert service["InaccessiblePaths"] == "/home/lighthouse/rquant/.env"
    assert service["ProtectKernelTunables"] == "true"
    assert service["ProtectKernelModules"] == "true"
    assert service["ProtectControlGroups"] == "true"
    assert service["RestrictSUIDSGID"] == "true"
    assert service["LockPersonality"] == "true"
    assert service["CapabilityBoundingSet"] == ""
    assert service["AmbientCapabilities"] == ""
    assert service["UMask"] == "0077"

    lowered = raw.lower()
    assert "/bin/sh" not in lowered
    assert "/bin/bash" not in lowered
    assert "execstart=/usr/bin/env" not in lowered
    assert " -c " not in lowered
    assert "import_path" not in lowered
    assert "dynamic_import" not in lowered
    assert "EnvironmentFile=/home/lighthouse/rquant/.env" not in raw
    for secret_name in ("TUSHARE_TOKEN", "PUSHDEER_KEYS", "PASSWORD", "API_KEY"):
        assert secret_name not in raw


def test_only_live_instances_may_load_one_scoped_secret_file() -> None:
    live = _load("live")["Service"]
    serving = _load("serving")["Service"]
    research = _load("research")["Service"]

    assert live["EnvironmentFile"] == f"{RUNTIME_ROOT}/secrets/%i.env"
    assert serving["EnvironmentFile"] == ENVIRONMENT_FILE
    assert research["EnvironmentFile"] == ENVIRONMENT_FILE


def test_runtime_templates_only_write_their_plane_and_shared_control_root() -> None:
    expected = {
        plane: {CONTROL_ROOT, f"{RUNTIME_ROOT}/{plane}"} for plane in PLANES
    }

    for plane in PLANES:
        parser = _load(plane)
        writable = set(parser["Service"]["ReadWritePaths"].split())
        assert writable == expected[plane]
        assert f"{RUNTIME_ROOT}/rquant.duckdb" not in writable
        assert "/home/lighthouse/rquant/data/rquant.duckdb" not in writable

    research_writable = set(
        _load("research")["Service"]["ReadWritePaths"].split()
    )
    assert f"{RUNTIME_ROOT}/live" not in research_writable
    assert f"{RUNTIME_ROOT}/serving" not in research_writable


@pytest.mark.parametrize("plane", PLANES)
def test_runtime_templates_are_install_only_not_auto_enabled(plane: str) -> None:
    parser = _load(plane)
    assert "Install" not in parser
