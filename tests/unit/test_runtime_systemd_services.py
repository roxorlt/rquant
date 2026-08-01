"""Static contracts for isolated runtime systemd service templates."""

from __future__ import annotations

import configparser
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
SYSTEMD = ROOT / "deploy" / "systemd"
PLANES = ("live", "serving", "research")
TEMPLATES = (*PLANES, "candidate", "strategy")
RUNTIME_ROOT = "/home/lighthouse/rquant/data/runtime"
CURRENT_ROOT = f"{RUNTIME_ROOT}/current"
CONTROL_ROOT = f"{RUNTIME_ROOT}/control"
ENVIRONMENT_FILE = f"{CURRENT_ROOT}/runtime.env"
CREDENTIAL_FILE = "/etc/credstore.encrypted/rquant-runtime/instances/%i/current.cred"
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


@pytest.mark.parametrize("plane", TEMPLATES)
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
    assert service["EnvironmentFile"] == ENVIRONMENT_FILE
    assert service["Environment"] == "RQUANT_DISABLE_DOTENV=1"
    expected_plane = "live" if plane in {"candidate", "strategy"} else plane
    assert service["Slice"] == f"rquant-{expected_plane}.slice"

    command = service["ExecStart"]
    assert command.startswith(f"{EXPECTED_EXECUTABLE} -m {EXPECTED_MODULE} ")
    assert f"--manifest {CURRENT_ROOT}/manifests/%i.json" in command
    expected_control = {
        "candidate": f"{CONTROL_ROOT}/candidates/%i",
        "strategy": f"{CONTROL_ROOT}/strategies/%i",
    }.get(plane, CONTROL_ROOT)
    assert f"--control-root {expected_control}" in command
    assert "--expected-commit ${RQUANT_RUNTIME_COMMIT}" in command
    assert "--expected-generation ${RQUANT_RUNTIME_GENERATION}" in command
    expected_kind = {
        "candidate": "candidate_publisher",
        "strategy": "strategy_live",
    }.get(plane)
    if expected_kind is not None:
        assert f"--expected-kind {expected_kind}" in command


@pytest.mark.parametrize("plane", TEMPLATES)
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


@pytest.mark.parametrize("plane", TEMPLATES)
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
    inaccessible = set(service["InaccessiblePaths"].split())
    assert inaccessible == {
        "/home/lighthouse/rquant/.env",
        f"-{CURRENT_ROOT}/secrets",
        f"-{CURRENT_ROOT}/credentials",
    }
    assert service["ProtectProc"] == "invisible"
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


def test_only_capability_live_instances_load_one_encrypted_systemd_credential() -> None:
    live = _load("live")["Service"]
    candidate = _load("candidate")["Service"]
    strategy = _load("strategy")["Service"]
    serving = _load("serving")["Service"]
    research = _load("research")["Service"]

    assert live["EnvironmentFile"] == ENVIRONMENT_FILE
    assert live["LoadCredentialEncrypted"] == f"capabilities.json:{CREDENTIAL_FILE}"
    assert candidate["EnvironmentFile"] == ENVIRONMENT_FILE
    assert strategy["EnvironmentFile"] == ENVIRONMENT_FILE
    assert serving["EnvironmentFile"] == ENVIRONMENT_FILE
    assert research["EnvironmentFile"] == ENVIRONMENT_FILE
    for service in (candidate, strategy, serving, research):
        assert "LoadCredential" not in service
        assert "LoadCredentialEncrypted" not in service


def test_runtime_templates_only_write_their_plane_and_shared_control_root() -> None:
    expected = {plane: {CONTROL_ROOT, f"{RUNTIME_ROOT}/{plane}"} for plane in PLANES}

    for plane in PLANES:
        parser = _load(plane)
        writable = set(parser["Service"]["ReadWritePaths"].split())
        assert writable == expected[plane]
        assert f"{RUNTIME_ROOT}/rquant.duckdb" not in writable
        assert "/home/lighthouse/rquant/data/rquant.duckdb" not in writable

    research_writable = set(_load("research")["Service"]["ReadWritePaths"].split())
    assert f"{RUNTIME_ROOT}/live" not in research_writable
    assert f"{RUNTIME_ROOT}/serving" not in research_writable

    candidate_writable = set(_load("candidate")["Service"]["ReadWritePaths"].split())
    assert candidate_writable == {
        f"{CONTROL_ROOT}/candidates/%i",
        f"{RUNTIME_ROOT}/live/candidates/%i",
    }
    assert f"{RUNTIME_ROOT}/live" not in candidate_writable

    strategy_writable = set(_load("strategy")["Service"]["ReadWritePaths"].split())
    assert strategy_writable == {
        f"{CONTROL_ROOT}/strategies/%i",
        f"{RUNTIME_ROOT}/live/strategies/%i",
    }
    assert f"{RUNTIME_ROOT}/live" not in strategy_writable


def test_generic_live_instances_cannot_modify_candidate_authority() -> None:
    live = _load("live")["Service"]
    readonly = {path.lstrip("-") for path in live["ReadOnlyPaths"].split()}

    assert readonly == {
        f"{CONTROL_ROOT}/candidates",
        f"{CONTROL_ROOT}/strategies",
        f"{RUNTIME_ROOT}/live/candidates",
        f"{RUNTIME_ROOT}/live/strategies",
    }
    assert set(_load("candidate")["Service"]["ReadWritePaths"].split()) == {
        f"{CONTROL_ROOT}/candidates/%i",
        f"{RUNTIME_ROOT}/live/candidates/%i",
    }


@pytest.mark.parametrize("plane", PLANES)
def test_non_candidate_instances_cannot_modify_candidate_heartbeats(
    plane: str,
) -> None:
    readonly = {path.lstrip("-") for path in _load(plane)["Service"]["ReadOnlyPaths"].split()}
    assert f"{CONTROL_ROOT}/candidates" in readonly


@pytest.mark.parametrize("plane", PLANES)
def test_non_strategy_instances_cannot_modify_strategy_state_or_heartbeats(
    plane: str,
) -> None:
    readonly = {path.lstrip("-") for path in _load(plane)["Service"]["ReadOnlyPaths"].split()}
    assert f"{CONTROL_ROOT}/strategies" in readonly
    if plane == "live":
        assert f"{RUNTIME_ROOT}/live/strategies" in readonly


@pytest.mark.parametrize("plane", TEMPLATES)
def test_runtime_templates_are_install_only_not_auto_enabled(plane: str) -> None:
    parser = _load(plane)
    assert "Install" not in parser


@pytest.mark.parametrize("plane", TEMPLATES)
def test_optional_runtime_masks_do_not_block_unit_start(plane: str) -> None:
    service = _load(plane)["Service"]
    assert all(path.startswith("-") for path in service["InaccessiblePaths"].split()[1:])
    for path in service.get("ReadOnlyPaths", "").split():
        if path.endswith(("/candidates", "/strategies")):
            assert path.startswith("-")


def test_manual_infrastructure_deployer_installs_root_owned_credential_sealer() -> None:
    deployer = (ROOT / "scripts" / "install-runtime-credential-infra.sh").read_text(
        encoding="utf-8"
    )

    assert "deploy/libexec/rquant-runtime-credential-sealer" in deployer
    assert 'HELPER_DIR="${PREFIX}/usr/local/libexec"' in deployer
    assert 'HELPER_TARGET="${HELPER_DIR}/rquant-runtime-credential-sealer"' in deployer
    assert "/usr/bin/install -o root -g root" in deployer
    assert "-m 0755" in deployer
    assert "VISUDO_BIN" in deployer
    assert "install_file 0440" in deployer
