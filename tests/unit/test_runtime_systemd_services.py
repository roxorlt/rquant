"""Static contracts for isolated runtime systemd service templates."""

from __future__ import annotations

import configparser
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
SYSTEMD = ROOT / "deploy" / "systemd"
PLANES = ("serving",)
DEDICATED_LIVE_TEMPLATES = (
    "reference-slow-source",
    "reference-slow-publisher",
    "auction-universe",
    "auction-match",
    "market-minute",
    "watchlist-quote",
    "feature",
    "candidate",
    "strategy",
    "signal-router",
    "notifier",
    "paper-constraint",
    "paper-broker",
    "daily-close",
)
DEDICATED_SERVING_TEMPLATES = ("runtime-health",)
DEDICATED_RESEARCH_TEMPLATES = ("shadow", "lab-jobs", "artifact-catalog", "promotions")
TEMPLATES = (
    *PLANES,
    *DEDICATED_LIVE_TEMPLATES,
    *DEDICATED_SERVING_TEMPLATES,
    *DEDICATED_RESEARCH_TEMPLATES,
)
RUNTIME_ROOT = "/home/lighthouse/rquant/data/runtime"
CURRENT_ROOT = f"{RUNTIME_ROOT}/current"
CONTROL_ROOT = f"{RUNTIME_ROOT}/control"
CREDENTIAL_FILE = "/etc/credstore.encrypted/rquant-runtime/instances/%i/current.cred"
#: Codex round-2 P1-3: every protected runtime unit now executes the fixed root-owned
#: wrapper instead of a checkout interpreter, and takes nothing from the unit but a role
#: literal. `%i` may still appear in systemd's own sandbox directives — those are path
#: grants systemd applies, not authority values the wrapper reads (ruling D-2).
WRAPPER_COMMAND = (
    "/usr/bin/python3.11 -I -S /usr/local/libexec/rquant-runtime-exec.pyz --role"
)
ARBITER_PREFIX = "/usr/local/libexec/rquant-workload-arbiter research -- "
FORBIDDEN_EXECUTABLE = "/home/lighthouse/rquant/.venv/bin/python"
RETIRED_MODULE = "rquant.runtime_service_main"
TEMPLATE_ROLES = {
    "reference-slow-source": "reference_slow_source",
    "reference-slow-publisher": "reference_slow_publisher",
    "auction-universe": "auction_universe_publisher",
    "auction-match": "auction_match_source",
    "market-minute": "market_minute_source",
    "watchlist-quote": "watchlist_quote_source",
    "feature": "feature_live",
    "candidate": "candidate_publisher",
    "strategy": "strategy_live",
    "signal-router": "signal_router",
    "notifier": "notifier",
    "paper-constraint": "paper_constraint_publisher",
    "paper-broker": "paper_broker",
    "daily-close": "daily_close_source",
    "runtime-health": "runtime_health_publisher",
    "shadow": "shadow_session",
    "lab-jobs": "lab_jobs_publisher",
    "artifact-catalog": "lab_artifact_catalog",
    "promotions": "promotions_publisher",
    "serving": "serving_publisher",
}
#: Every unit this branch adds that a protected role covers, and the role it names.
PROTECTED_UNIT_ROLES = {
    **{f"rquant-runtime-{name}@.service": role for name, role in TEMPLATE_ROLES.items()},
    "rquant-artifact-retention.service": "artifact_retention",
    "rquant-runtime-daily-orchestrator@.service": "daily_pipeline_orchestrator",
    "rquant-runtime-recovery@.service": "runtime_recovery",
    "rquant-runtime-recovery-rehearsal@.service": "runtime_recovery_rehearsal",
    "rquant-page-control.service": "page_control",
}
#: `rquant-page-control.service` is not a template, so its authorised instance label is a
#: unit-owned literal rather than `%i` — the same shape `rquant-artifact-retention` uses.
PAGE_CONTROL_INSTANCE = "svc-981cb38218dd899500ee1592a504790a57d459c946bbc53c8e210f299cf1980b"
RETENTION_INSTANCE = "svc-248ba9b29fdc243fcd4f7d09641fbdedd61871ffeea693ea4eb26f36f264b349"
REQUIRED_INACCESSIBLE_PATHS = {
    "lab-jobs": frozenset({"/etc/rquant/lab-claim-finalizer-runtime"}),
}


def test_generic_write_plane_runtime_templates_are_retired() -> None:
    assert not (SYSTEMD / "rquant-runtime-live@.service").exists()
    assert not (SYSTEMD / "rquant-runtime-research@.service").exists()


def test_artifact_retention_oneshot_and_timer_are_static_production_contracts() -> None:
    service_path = SYSTEMD / "rquant-artifact-retention.service"
    timer_path = SYSTEMD / "rquant-artifact-retention.timer"
    service = configparser.ConfigParser(interpolation=None, strict=True)
    service.optionxform = str
    service.read_string(service_path.read_text(encoding="utf-8"))
    timer = configparser.ConfigParser(interpolation=None, strict=True)
    timer.optionxform = str
    timer.read_string(timer_path.read_text(encoding="utf-8"))

    unit = service["Service"]
    assert unit["Type"] == "oneshot"
    assert unit["LoadCredentialEncrypted"] == (
        "capabilities.json:/etc/credstore.encrypted/rquant-runtime/instances/"
        f"{RETENTION_INSTANCE}/current.cred"
    )
    assert unit["ExecStart"] == (
        f"{ARBITER_PREFIX}{WRAPPER_COMMAND} artifact_retention --instance {RETENTION_INSTANCE}"
    )
    assert f"manifests/{RETENTION_INSTANCE}.json" not in unit["ExecStart"]
    assert "EnvironmentFile" not in unit
    assert set(unit["ReadWritePaths"].split()) == {
        f"{CONTROL_ROOT}/artifact-retention/{RETENTION_INSTANCE}",
        f"{RUNTIME_ROOT}/research/artifact-retention/{RETENTION_INSTANCE}",
        f"{RUNTIME_ROOT}/research/final-artifacts",
    }
    assert timer["Timer"]["OnCalendar"] == "*-*-* *:0/5"
    assert timer["Timer"]["Unit"] == "rquant-artifact-retention.service"
    assert timer["Timer"]["Persistent"] == "true"
    assert "systemd-analyze verify rquant-artifact-retention.service" in (
        service_path.read_text(encoding="utf-8")
    )
    assert "systemd-analyze calendar '*-*-* *:0/5' --iterations 5" in (
        timer_path.read_text(encoding="utf-8")
    )


def test_page_control_has_a_loopback_production_unit() -> None:
    path = SYSTEMD / "rquant-page-control.service"
    parser = configparser.ConfigParser(interpolation=None, strict=True)
    parser.optionxform = str
    parser.read_string(path.read_text(encoding="utf-8"))
    service = parser["Service"]
    assert "APP_ENV=prod" in service["Environment"]
    assert "RQUANT_DISABLE_DOTENV=1" in service["Environment"]
    assert "EnvironmentFile" not in service
    assert service["ExecStart"] == (
        f"{WRAPPER_COMMAND} page_control --instance {PAGE_CONTROL_INSTANCE}"
    )
    assert "RQUANT_PAGE_CONTROL_HOST=127.0.0.1" in service["Environment"]
    assert "RQUANT_PAGE_CONTROL_PORT=8767" in service["Environment"]
    assert service["Restart"] == "on-failure"
    assert "/home/lighthouse/rquant/data/runtime/serving" in service["ReadWritePaths"]
    assert "/home/lighthouse/rquant/data/canvases" not in service["ReadWritePaths"]
    assert "systemd-analyze verify rquant-page-control.service" in path.read_text(encoding="utf-8")


def test_daily_close_source_is_a_dedicated_least_privilege_live_unit() -> None:
    path = _path("daily-close")
    parser = configparser.ConfigParser(interpolation=None, strict=True)
    parser.optionxform = str
    parser.read_string(path.read_text(encoding="utf-8"))
    service = parser["Service"]
    assert service["Type"] == "simple"
    assert service["Slice"] == "rquant-live.slice"
    assert service["ExecStart"].endswith(f"{WRAPPER_COMMAND} daily_close_source --instance %i")
    assert set(service["ReadWritePaths"].split()) == {
        f"{CONTROL_ROOT}/daily-close-sources/%i",
        f"{RUNTIME_ROOT}/live/daily-close",
    }
    assert "TUSHARE_TOKEN_MAIN" not in path.read_text(encoding="utf-8")
    assert "systemd-analyze verify rquant-runtime-daily-close@.service" in (
        path.read_text(encoding="utf-8")
    )


def test_watchlist_quote_has_an_independent_least_privilege_live_unit() -> None:
    path = _path("watchlist-quote")
    parser = configparser.ConfigParser(interpolation=None, strict=True)
    parser.optionxform = str
    parser.read_string(path.read_text(encoding="utf-8"))
    service = parser["Service"]

    assert service["Slice"] == "rquant-live.slice"
    assert service["ExecStart"].endswith(f"{WRAPPER_COMMAND} watchlist_quote_source --instance %i")
    assert "LoadCredentialEncrypted" not in service
    assert set(service["ReadWritePaths"].split()) == {
        f"{CONTROL_ROOT}/watchlist-quote-sources/%i",
        f"{RUNTIME_ROOT}/live/watchlist-quote",
    }
    assert set(service["ReadOnlyPaths"].split()) == {
        f"-{RUNTIME_ROOT}/authorities/market-calendar",
        f"-{RUNTIME_ROOT}/live/candidates",
    }
    assert "systemd-analyze verify rquant-runtime-watchlist-quote@.service" in path.read_text(
        encoding="utf-8"
    )


def test_shadow_session_is_a_readonly_legacy_consumer_with_its_own_report_root() -> None:
    path = _path("shadow")
    parser = configparser.ConfigParser(interpolation=None, strict=True)
    parser.optionxform = str
    parser.read_string(path.read_text(encoding="utf-8"))
    service = parser["Service"]
    assert service["Type"] == "simple"
    assert service["Slice"] == "rquant-research.slice"
    assert service["ExecStart"].endswith(f"{WRAPPER_COMMAND} shadow_session --instance %i")
    assert set(service["ReadWritePaths"].split()) == {
        f"{CONTROL_ROOT}/shadow-sessions/%i",
        f"{RUNTIME_ROOT}/research/shadow-reports",
    }
    assert set(service["ReadOnlyPaths"].split()) == {
        f"-{RUNTIME_ROOT}/authorities/market-calendar",
        "-/home/lighthouse/rquant/data/legacy-shadow",
    }
    assert "LoadCredential" not in service
    assert "systemd-analyze verify rquant-runtime-shadow@.service" in path.read_text(
        encoding="utf-8"
    )


def test_daily_orchestrator_is_an_unenabled_shadow_fan_in_oneshot() -> None:
    service_path = SYSTEMD / "rquant-runtime-daily-orchestrator@.service"
    timer_path = SYSTEMD / "rquant-runtime-daily-orchestrator@.timer"
    service = configparser.ConfigParser(interpolation=None, strict=True)
    service.optionxform = str
    service.read_string(service_path.read_text(encoding="utf-8"))
    timer = configparser.ConfigParser(interpolation=None, strict=True)
    timer.optionxform = str
    timer.read_string(timer_path.read_text(encoding="utf-8"))

    unit = service["Service"]
    assert unit["Type"] == "oneshot"
    assert unit["Slice"] == "rquant-research.slice"
    assert unit["ExecStart"].endswith(
        f"{WRAPPER_COMMAND} daily_pipeline_orchestrator --instance %i"
    )
    assert unit["Restart"] == "no"
    assert unit["NoNewPrivileges"] == "true"
    assert unit["PrivateTmp"] == "true"
    assert unit["PrivateDevices"] == "true"
    assert unit["ProtectSystem"] == "strict"
    assert unit["ProtectHome"] == "read-only"
    assert unit["CapabilityBoundingSet"] == ""
    assert unit["AmbientCapabilities"] == ""
    assert unit["RestrictSUIDSGID"] == "true"
    assert unit["UMask"] == "0077"
    assert set(unit["ReadWritePaths"].split()) == {
        f"{CONTROL_ROOT}/daily-orchestrators/%i",
        f"{RUNTIME_ROOT}/research/daily-pipeline",
    }
    assert set(unit["ReadOnlyPaths"].split()) == {
        "-/etc/rquant/daily-receipt-trusted-keys.json",
        f"-{RUNTIME_ROOT}/live/daily-close",
        f"-{CURRENT_ROOT}",
    }
    assert "Install" not in service
    assert timer["Timer"]["OnCalendar"] == "Mon..Fri *-*-* 17:15:00"
    assert timer["Timer"]["Unit"] == "rquant-runtime-daily-orchestrator@%i.service"
    assert timer["Timer"]["Persistent"] == "true"
    assert "systemd-analyze verify rquant-runtime-daily-orchestrator@.service" in (
        service_path.read_text(encoding="utf-8")
    )
    assert (
        "systemd-analyze calendar 'Mon..Fri *-*-* 17:15:00' --iterations 5"
        in timer_path.read_text(encoding="utf-8")
    )


def test_daily_receipt_signing_is_not_a_sudoers_capability() -> None:
    sudoers = (ROOT / "deploy" / "sudoers" / "rquant-production-deploy").read_text(encoding="utf-8")

    assert "RQUANT_DAILY_RECEIPT_SIGNER" not in sudoers
    assert "/usr/local/libexec/rquant-daily-receipt-signer" not in sudoers


def test_daily_receipt_signer_socket_units_are_root_owned_static_contracts() -> None:
    socket_path = SYSTEMD / "rquant-daily-receipt-signer.socket"
    service_path = SYSTEMD / "rquant-daily-receipt-signer.service"
    socket_unit = configparser.ConfigParser(interpolation=None, strict=True)
    socket_unit.optionxform = str
    socket_unit.read_string(socket_path.read_text(encoding="utf-8"))
    service_unit = configparser.ConfigParser(interpolation=None, strict=True)
    service_unit.optionxform = str
    service_unit.read_string(service_path.read_text(encoding="utf-8"))

    socket = socket_unit["Socket"]
    service = service_unit["Service"]
    assert socket["ListenStream"] == "/run/rquant/daily-receipt-signer.sock"
    assert socket["SocketUser"] == "root"
    assert socket["SocketGroup"] == "lighthouse"
    assert socket["SocketMode"] == "0660"
    assert socket["DirectoryMode"] == "0755"
    assert socket["RemoveOnStop"] == "true"
    assert socket["Service"] == "rquant-daily-receipt-signer.service"

    assert service["Type"] == "simple"
    assert service["User"] == "root"
    assert service["Group"] == "root"
    assert service["ReadWritePaths"] == "/var/lib/rquant/daily-receipt-signer"
    assert service["ExecStart"] == (
        "/usr/bin/python3 -I -S "
        "/usr/local/libexec/rquant-daily-receipt-authority/current/authority.pyz"
    )
    assert "/home/lighthouse" not in service["ExecStart"]
    assert service["NoNewPrivileges"] == "true"
    assert service["CapabilityBoundingSet"] == ""
    assert service["AmbientCapabilities"] == ""
    assert service["PrivateTmp"] == "true"
    assert service["ProtectSystem"] == "strict"
    assert "/etc/rquant/daily-receipt-keys.json" in service["ReadOnlyPaths"]
    assert "/etc/rquant/daily-receipt-trusted-keys.json" in service["ReadOnlyPaths"]
    assert "daily-receipt-signer.sock" not in service["ExecStart"]
    assert "systemd-analyze verify rquant-daily-receipt-signer.socket" in (
        socket_path.read_text(encoding="utf-8")
    )


def test_daily_receipt_signer_is_installed_and_release_controlled_as_a_socket_unit() -> None:
    from rquant import release_generation

    installer = (ROOT / "scripts" / "install-runtime-credential-infra.sh").read_text(
        encoding="utf-8"
    )

    assert "deploy/systemd/rquant-daily-receipt-signer.socket" in installer
    assert "deploy/systemd/rquant-daily-receipt-signer.service" in installer
    assert "systemctl_run enable --now" in installer
    assert "rquant-daily-receipt-signer.socket" in installer
    assert "systemctl enable --now rquant-daily-receipt-signer.service" not in installer

    assert "rquant-daily-receipt-signer.socket" in release_generation.ALL_LONG_RUNNING_SERVICES
    assert release_generation.SERVICE_PATTERNS["rquant-daily-receipt-signer.socket"] == (
        "deploy/root-runtime/daily_receipt_authority.py",
        "deploy/libexec/rquant-daily-receipt-signer",
        "scripts/install-runtime-credential-infra.sh",
        "deploy/systemd/rquant-daily-receipt-signer.socket",
        "deploy/systemd/rquant-daily-receipt-signer.service",
    )


def test_installed_daily_libexec_wrapper_rejects_receipt_signing() -> None:
    """The compatibility wrapper must direct signing callers to the socket authority."""

    import subprocess
    import sys

    helper = ROOT / "deploy" / "libexec" / "rquant-daily-receipt-signer"
    result = subprocess.run(
        [sys.executable, str(helper)],
        input=b'{"operation":"sign","schema_version":1}',
        capture_output=True,
        timeout=5,
    )

    assert result.returncode == 2
    assert b"only available through the root socket authority" in result.stderr


def _path(plane: str) -> Path:
    return SYSTEMD / f"rquant-runtime-{plane}@.service"


def _load(plane: str) -> configparser.ConfigParser:
    parser = configparser.ConfigParser(interpolation=None, strict=True)
    parser.optionxform = str
    with _path(plane).open(encoding="utf-8") as stream:
        parser.read_file(stream)
    return parser


def _is_readonly_path_covered(readonly: set[str], required: str) -> bool:
    return any(
        required == candidate or required.startswith(f"{candidate}/") for candidate in readonly
    )


@pytest.mark.parametrize("plane", TEMPLATES)
def test_runtime_template_runs_the_fixed_root_owned_wrapper(plane: str) -> None:
    """Codex round-2 P1-3: no checkout interpreter, no mutable env file, no `%i` authority."""

    parser = _load(plane)
    unit = parser["Unit"]
    service = parser["Service"]

    assert unit["OnFailure"] == "rquant-alert@%n.service"
    assert service["Type"] == "simple"
    assert service["User"] == "lighthouse"
    assert service["Group"] == "lighthouse"
    assert service["WorkingDirectory"] == "/home/lighthouse/rquant"
    assert "EnvironmentFile" not in service
    assert service["Environment"] == "APP_ENV=prod RQUANT_DISABLE_DOTENV=1"
    expected_plane = (
        "live"
        if plane in DEDICATED_LIVE_TEMPLATES
        else "serving"
        if plane in DEDICATED_SERVING_TEMPLATES
        else "research"
        if plane in DEDICATED_RESEARCH_TEMPLATES
        else plane
    )
    assert service["Slice"] == f"rquant-{expected_plane}.slice"

    command = service["ExecStart"]
    if plane in DEDICATED_RESEARCH_TEMPLATES:
        assert command.startswith(ARBITER_PREFIX)
        command = command.removeprefix(ARBITER_PREFIX)
    assert command == f"{WRAPPER_COMMAND} {TEMPLATE_ROLES[plane]} --instance %i"


@pytest.mark.parametrize("plane", TEMPLATES)
def test_runtime_template_carries_no_retired_authority_input(plane: str) -> None:
    """The three things P1-3 names, absent from the command line entirely."""

    command = _load(plane)["Service"]["ExecStart"]

    assert FORBIDDEN_EXECUTABLE not in command
    assert RETIRED_MODULE not in command
    assert CURRENT_ROOT not in command
    assert "${" not in command
    for retired in ("--manifest", "--control-root", "--expected-commit", "--expected-generation"):
        assert retired not in command
    # `%i` survives only as the instance label, and the wrapper accepts it only if the
    # root-owned profile already lists it (ruling D-2: a lookup key, not an authority value).
    assert command.count("%i") == 1
    assert command.endswith("--instance %i")


def test_every_protected_unit_names_a_wrapper_allowlisted_role() -> None:
    """The unit's literal and the wrapper's frozen allowlist are the same closed set."""

    from rquant.runtime_exec_wrapper._verify import PROTECTED_ROLES

    for name, role in sorted(PROTECTED_UNIT_ROLES.items()):
        parser = configparser.ConfigParser(interpolation=None, strict=True)
        parser.optionxform = str
        parser.read_string((SYSTEMD / name).read_text(encoding="utf-8"))
        command = parser["Service"]["ExecStart"]

        assert role in PROTECTED_ROLES, role
        expected = f"{WRAPPER_COMMAND} {role}"
        assert command.startswith(ARBITER_PREFIX) or command.startswith(expected), name
        assert expected in command, name


def test_no_new_unit_still_reads_the_application_written_runtime_environment() -> None:
    """`data/runtime/current/runtime.env` is written by the application it configures."""

    offenders = [
        path.name
        for path in sorted(SYSTEMD.glob("*.service"))
        if "data/runtime/current/runtime.env" in path.read_text(encoding="utf-8")
    ]

    assert offenders == []


def test_no_protected_unit_executes_a_checkout_interpreter() -> None:
    offenders = [
        name
        for name in sorted(PROTECTED_UNIT_ROLES)
        if FORBIDDEN_EXECUTABLE in (SYSTEMD / name).read_text(encoding="utf-8")
    ]

    assert offenders == []


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
    assert set(service["SuccessExitStatus"].split()) == (
        {"0", "75"} if plane in DEDICATED_RESEARCH_TEMPLATES else {"0"}
    )


def test_reference_slow_source_has_exact_process_resource_budgets() -> None:
    service = _load("reference-slow-source")["Service"]

    assert service["MemoryHigh"] == "512M"
    assert service["MemoryMax"] == "768M"
    assert service["TasksMax"] == "64"
    assert service["LimitNOFILE"] == "1024"


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
        *REQUIRED_INACCESSIBLE_PATHS.get(plane, ()),
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
    reference_slow_source = _load("reference-slow-source")["Service"]
    reference_slow_publisher = _load("reference-slow-publisher")["Service"]
    auction_universe = _load("auction-universe")["Service"]
    auction_match = _load("auction-match")["Service"]
    market_minute = _load("market-minute")["Service"]
    watchlist_quote = _load("watchlist-quote")["Service"]
    feature = _load("feature")["Service"]
    candidate = _load("candidate")["Service"]
    strategy = _load("strategy")["Service"]
    signal_router = _load("signal-router")["Service"]
    notifier = _load("notifier")["Service"]
    paper_broker = _load("paper-broker")["Service"]
    paper_constraint = _load("paper-constraint")["Service"]
    runtime_health = _load("runtime-health")["Service"]
    lab_jobs = _load("lab-jobs")["Service"]
    artifact_catalog = _load("artifact-catalog")["Service"]
    promotions = _load("promotions")["Service"]
    serving = _load("serving")["Service"]

    assert reference_slow_source["LoadCredentialEncrypted"] == (
        f"capabilities.json:{CREDENTIAL_FILE}"
    )
    assert reference_slow_publisher["LoadCredentialEncrypted"] == (
        f"capabilities.json:{CREDENTIAL_FILE}"
    )
    assert auction_match["LoadCredentialEncrypted"] == (f"capabilities.json:{CREDENTIAL_FILE}")
    assert market_minute["LoadCredentialEncrypted"] == (f"capabilities.json:{CREDENTIAL_FILE}")
    assert notifier["LoadCredentialEncrypted"] == f"capabilities.json:{CREDENTIAL_FILE}"
    for service in (
        auction_universe,
        watchlist_quote,
        candidate,
        feature,
        strategy,
        signal_router,
        paper_constraint,
        paper_broker,
        runtime_health,
        lab_jobs,
        artifact_catalog,
        promotions,
        serving,
    ):
        assert "LoadCredential" not in service
        assert "LoadCredentialEncrypted" not in service


def test_runtime_templates_only_write_their_plane_and_shared_control_root() -> None:
    expected = {
        "serving": {
            f"{CONTROL_ROOT}/serving-publishers/%i",
            f"{RUNTIME_ROOT}/serving",
        },
    }

    for plane in PLANES:
        parser = _load(plane)
        writable = set(parser["Service"]["ReadWritePaths"].split())
        assert writable == expected[plane]
        assert f"{RUNTIME_ROOT}/rquant.duckdb" not in writable
        assert "/home/lighthouse/rquant/data/rquant.duckdb" not in writable

    assert set(_load("market-minute")["Service"]["ReadWritePaths"].split()) == {
        f"{CONTROL_ROOT}/market-minute-sources/%i",
        f"{RUNTIME_ROOT}/live/market-minute",
    }
    assert set(_load("watchlist-quote")["Service"]["ReadWritePaths"].split()) == {
        f"{CONTROL_ROOT}/watchlist-quote-sources/%i",
        f"{RUNTIME_ROOT}/live/watchlist-quote",
    }
    assert set(_load("auction-match")["Service"]["ReadWritePaths"].split()) == {
        f"{CONTROL_ROOT}/auction-match-sources/%i",
        f"{RUNTIME_ROOT}/live/auction-match",
    }
    assert set(_load("auction-universe")["Service"]["ReadWritePaths"].split()) == {
        f"{CONTROL_ROOT}/auction-universe-publishers/%i",
        f"{RUNTIME_ROOT}/authorities/auction-universe",
    }
    assert set(_load("reference-slow-source")["Service"]["ReadWritePaths"].split()) == {
        f"{CONTROL_ROOT}/reference-slow-sources/%i",
        f"{RUNTIME_ROOT}/live/reference-slow",
    }
    assert set(_load("reference-slow-source")["Service"]["ReadOnlyPaths"].split()) == {
        "-/home/lighthouse/rquant/data/rquant_ro.duckdb",
        f"-{RUNTIME_ROOT}/authorities/market-calendar",
        f"-{CONTROL_ROOT}/reference-slow-publishers/"
        "svc-62c9061740150340b1f1e3a8a54323e26794caf9616d34047546383cdc027abd/cursors",
    }
    assert set(_load("reference-slow-publisher")["Service"]["ReadWritePaths"].split()) == {
        f"{CONTROL_ROOT}/reference-slow-publishers/%i",
        f"{RUNTIME_ROOT}/authorities/reference-slow",
    }
    assert set(_load("reference-slow-publisher")["Service"]["ReadOnlyPaths"].split()) == {
        f"-{RUNTIME_ROOT}/authorities/market-calendar",
        f"-{RUNTIME_ROOT}/live/reference-slow",
    }
    assert set(_load("feature")["Service"]["ReadWritePaths"].split()) == {
        f"{CONTROL_ROOT}/features/%i",
        f"{RUNTIME_ROOT}/live/features",
    }

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

    dedicated_writable = {
        "signal-router": {
            f"{CONTROL_ROOT}/signal-routers/%i",
            f"{RUNTIME_ROOT}/live/signal-bus",
        },
        "notifier": {
            f"{CONTROL_ROOT}/notifiers/%i",
            f"{RUNTIME_ROOT}/live/notifications/%i",
        },
        "paper-broker": {
            f"{CONTROL_ROOT}/paper-brokers/%i",
            f"{RUNTIME_ROOT}/live/paper-brokers/%i",
        },
        "paper-constraint": {
            f"{CONTROL_ROOT}/paper-constraints/%i",
            f"{RUNTIME_ROOT}/authorities/paper-execution",
        },
        "runtime-health": {
            f"{CONTROL_ROOT}/runtime-health-publishers/%i",
            f"{CONTROL_ROOT}/authority-runtime-health",
        },
        "lab-jobs": {
            f"{CONTROL_ROOT}/lab-jobs-publishers/%i",
            f"{RUNTIME_ROOT}/research/serving-authorities/lab-jobs",
        },
        "artifact-catalog": {
            f"{CONTROL_ROOT}/artifact-catalogs/%i",
            f"{RUNTIME_ROOT}/research/artifact-catalogs/%i",
            (
                f"{RUNTIME_ROOT}/research/artifact-retention/{RETENTION_INSTANCE}"
                "/catalog-registration-outbox"
            ),
        },
        "promotions": {
            f"{CONTROL_ROOT}/promotions-publishers/%i",
            f"{RUNTIME_ROOT}/research/serving-authorities/promotions",
        },
        "shadow": {
            f"{CONTROL_ROOT}/shadow-sessions/%i",
            f"{RUNTIME_ROOT}/research/shadow-reports",
        },
    }
    for template, expected_paths in dedicated_writable.items():
        writable = set(_load(template)["Service"]["ReadWritePaths"].split())
        assert writable == expected_paths
        assert f"{RUNTIME_ROOT}/live" not in writable


@pytest.mark.parametrize(
    ("name", "action"),
    (
        ("rquant-runtime-recovery@.service", "execute"),
        ("rquant-runtime-recovery-rehearsal@.service", "rehearse"),
    ),
)
def test_recovery_oneshot_is_isolated_bounded_and_does_not_control_live(
    name: str,
    action: str,
) -> None:
    parser = configparser.ConfigParser(interpolation=None, strict=True)
    parser.optionxform = str
    path = SYSTEMD / name
    parser.read_string(path.read_text(encoding="utf-8"))
    unit = parser["Unit"]
    service = parser["Service"]

    assert unit["OnFailure"] == "rquant-alert@%n.service"
    assert "rquant-monitor" not in path.read_text(encoding="utf-8")
    assert service["Type"] == "oneshot"
    assert service["Slice"] == "rquant-research.slice"
    assert service["RuntimeDirectory"].startswith("rquant/runtime-recovery/")
    assert "StateDirectory" not in service
    assert "LoadCredentialEncrypted" not in service
    assert service["Restart"] == "no"
    assert service["TimeoutStartSec"] == "infinity"
    assert service["RuntimeMaxSec"] == "infinity"
    assert service["MemoryHigh"] == "512M"
    assert service["MemoryMax"] == "768M"
    assert service["TasksMax"] == "64"
    assert service["CPUWeight"] == "20"
    assert service["IOWeight"] == "20"
    assert service["NoNewPrivileges"] == "true"
    assert service["PrivateTmp"] == "true"
    assert service["ProtectSystem"] == "strict"
    assert service["ReadOnlyPaths"] == (
        "-/home/lighthouse/rquant/data -/var/lib/rquant/runtime-recovery/backups"
    )
    assert service["ReadWritePaths"] == (
        "-/home/lighthouse/rquant/data/runtime/control/recovery "
        "-/var/lib/rquant/runtime-recovery/restores"
    )
    command = service["ExecStart"]
    assert command.startswith(ARBITER_PREFIX)
    # P1-3 / ruling D-2: the generation is no longer a `%i` argument the caller chooses.
    # The wrapper reads it out of the root-owned `current.json` and refuses anything else.
    role = "runtime_recovery" if action == "execute" else "runtime_recovery_rehearsal"
    assert command.removeprefix(ARBITER_PREFIX) == f"{WRAPPER_COMMAND} {role} --instance %i"
    assert "--expected-profile-generation" not in command
    assert set(service["SuccessExitStatus"].split()) == {"0", "75"}
    for duplicated in (
        "--publication-root",
        "--state-path",
        "--receipt-root",
        "--restore-root",
        "--credential-file",
        "--deadline-seconds",
        "--lease-seconds",
        "--max-attempts",
        "--retry-delay-seconds",
        "--schedule-cycle-seconds",
    ):
        assert duplicated not in command
    assert "/bin/sh" not in command and "/bin/bash" not in command and " -c " not in command


def test_recovery_rehearsal_timer_is_the_only_high_frequency_scheduler() -> None:
    path = SYSTEMD / "rquant-runtime-recovery-rehearsal@.timer"
    raw = path.read_text(encoding="utf-8")
    parser = configparser.ConfigParser(interpolation=None, strict=True)
    parser.optionxform = str
    parser.read_string(raw)

    assert parser["Timer"]["OnCalendar"] == "hourly"
    assert parser["Timer"]["Unit"] == "rquant-runtime-recovery-rehearsal@%i.service"
    assert parser["Timer"]["Persistent"] == "true"
    assert "systemd-analyze calendar 'hourly' --iterations 5" in raw
    timers = tuple(SYSTEMD.glob("rquant-runtime-recovery*.timer"))
    assert timers == (path,)
    service = configparser.ConfigParser(interpolation=None, strict=True)
    service.optionxform = str
    service.read_string(
        (SYSTEMD / "rquant-runtime-recovery-rehearsal@.service").read_text(encoding="utf-8")
    )
    assert "--rehearsal-interval-seconds" not in service["Service"]["ExecStart"]


def test_dedicated_signal_and_paper_units_receive_only_required_read_authorities() -> None:
    router_readonly = set(_load("signal-router")["Service"]["ReadOnlyPaths"].split())
    notifier_readonly = set(_load("notifier")["Service"]["ReadOnlyPaths"].split())
    broker_readonly = set(_load("paper-broker")["Service"]["ReadOnlyPaths"].split())
    constraint_readonly = set(_load("paper-constraint")["Service"]["ReadOnlyPaths"].split())

    assert router_readonly == {f"-{RUNTIME_ROOT}/live/strategies"}
    assert notifier_readonly == {
        f"-{RUNTIME_ROOT}/live/signal-bus/spool",
        f"-{RUNTIME_ROOT}/serving/page-control",
        f"-{CONTROL_ROOT}/page-control.sqlite3",
    }
    assert broker_readonly == {
        f"-{RUNTIME_ROOT}/authorities/paper-execution",
        f"-{RUNTIME_ROOT}/live/market-minute",
        f"-{RUNTIME_ROOT}/live/signal-bus/spool",
    }
    assert constraint_readonly == {
        f"-{RUNTIME_ROOT}/live/market-minute",
        f"-{RUNTIME_ROOT}/authorities/reference-slow",
    }


def test_authority_publishers_have_exact_read_scopes() -> None:
    assert set(_load("runtime-health")["Service"]["ReadOnlyPaths"].split()) == {
        f"-{CONTROL_ROOT}/reference-slow-sources",
        f"-{CONTROL_ROOT}/reference-slow-publishers",
        f"-{CONTROL_ROOT}/auction-universe-publishers",
        f"-{CONTROL_ROOT}/auction-match-sources",
        f"-{CONTROL_ROOT}/artifact-catalogs",
        f"-{CONTROL_ROOT}/candidates",
        f"-{CONTROL_ROOT}/features",
        f"-{CONTROL_ROOT}/market-minute-sources",
        f"-{CONTROL_ROOT}/watchlist-quote-sources",
        f"-{CONTROL_ROOT}/daily-close-sources",
        f"-{CONTROL_ROOT}/daily-orchestrators",
        f"-{CONTROL_ROOT}/shadow-sessions",
        f"-{CONTROL_ROOT}/notifiers",
        f"-{CONTROL_ROOT}/paper-brokers",
        f"-{CONTROL_ROOT}/paper-constraints",
        f"-{CONTROL_ROOT}/promotions-publishers",
        f"-{CONTROL_ROOT}/lab-jobs-publishers",
        f"-{CONTROL_ROOT}/signal-routers",
        f"-{CONTROL_ROOT}/strategies",
    }
    assert set(_load("lab-jobs")["Service"]["ReadOnlyPaths"].split()) == {
        f"-{RUNTIME_ROOT}/research/lab_jobs.sqlite3",
    }
    assert set(_load("promotions")["Service"]["ReadOnlyPaths"].split()) == {
        f"-{RUNTIME_ROOT}/research/experiment_registry.sqlite3",
    }


def test_artifact_catalog_has_exact_read_scopes() -> None:
    assert set(_load("artifact-catalog")["Service"]["ReadOnlyPaths"].split()) == {
        f"-{RUNTIME_ROOT}/research/final-artifacts",
        f"-{RUNTIME_ROOT}/research/lab_jobs.sqlite3",
        f"-{RUNTIME_ROOT}/research/research_ro.duckdb",
        f"-{RUNTIME_ROOT}/research/experiment_registry.sqlite3",
    }


def test_quote_and_market_minute_units_cannot_modify_other_live_owners() -> None:
    universe_writable = set(_load("auction-universe")["Service"]["ReadWritePaths"].split())
    auction_writable = set(_load("auction-match")["Service"]["ReadWritePaths"].split())
    source_writable = set(_load("market-minute")["Service"]["ReadWritePaths"].split())
    quote_writable = set(_load("watchlist-quote")["Service"]["ReadWritePaths"].split())
    feature_writable = set(_load("feature")["Service"]["ReadWritePaths"].split())
    forbidden = {
        f"{RUNTIME_ROOT}/live/signal-bus",
        f"{RUNTIME_ROOT}/live/notifications",
        f"{RUNTIME_ROOT}/live/paper-brokers",
        f"{RUNTIME_ROOT}/live/candidates",
        f"{RUNTIME_ROOT}/live/strategies",
    }

    assert source_writable.isdisjoint(forbidden)
    assert quote_writable.isdisjoint(forbidden | {f"{RUNTIME_ROOT}/live/market-minute"})
    assert universe_writable.isdisjoint(forbidden)
    assert auction_writable.isdisjoint(forbidden)
    assert feature_writable.isdisjoint(forbidden)
    assert set(_load("feature")["Service"]["ReadOnlyPaths"].split()) == {
        f"-{RUNTIME_ROOT}/live/market-minute",
        f"-{RUNTIME_ROOT}/research",
    }
    assert set(_load("auction-match")["Service"]["ReadOnlyPaths"].split()) == {
        f"-{RUNTIME_ROOT}/authorities/market-calendar",
        f"-{RUNTIME_ROOT}/authorities/auction-universe",
    }
    assert set(_load("auction-universe")["Service"]["ReadOnlyPaths"].split()) == {
        "-/home/lighthouse/rquant/data/rquant_ro.duckdb",
        f"-{RUNTIME_ROOT}/authorities/market-calendar",
    }


def test_serving_publisher_reads_owner_authorities_without_writing_owner_state() -> None:
    service = _load("serving")["Service"]
    readonly = set(service["ReadOnlyPaths"].split())

    assert readonly == {
        f"-{CONTROL_ROOT}",
        f"-{RUNTIME_ROOT}/live/notifications",
        f"-{RUNTIME_ROOT}/live/paper-brokers",
        f"-{RUNTIME_ROOT}/live/signal-bus",
        f"-{RUNTIME_ROOT}/research",
    }
    assert set(service["ReadWritePaths"].split()) == {
        f"{CONTROL_ROOT}/serving-publishers/%i",
        f"{RUNTIME_ROOT}/serving",
    }


@pytest.mark.parametrize("plane", PLANES)
def test_non_candidate_instances_cannot_modify_candidate_heartbeats(
    plane: str,
) -> None:
    readonly = {path.lstrip("-") for path in _load(plane)["Service"]["ReadOnlyPaths"].split()}
    assert _is_readonly_path_covered(readonly, f"{CONTROL_ROOT}/candidates")


@pytest.mark.parametrize("plane", PLANES)
def test_non_strategy_instances_cannot_modify_strategy_state_or_heartbeats(
    plane: str,
) -> None:
    readonly = {path.lstrip("-") for path in _load(plane)["Service"]["ReadOnlyPaths"].split()}
    assert _is_readonly_path_covered(readonly, f"{CONTROL_ROOT}/strategies")
    if plane == "live":
        assert f"{RUNTIME_ROOT}/live/strategies" in readonly


@pytest.mark.parametrize("plane", TEMPLATES)
def test_runtime_templates_are_install_only_not_auto_enabled(plane: str) -> None:
    parser = _load(plane)
    assert "Install" not in parser


@pytest.mark.parametrize("plane", TEMPLATES)
def test_optional_runtime_masks_do_not_block_unit_start(plane: str) -> None:
    service = _load(plane)["Service"]
    inaccessible = service["InaccessiblePaths"].split()
    required = REQUIRED_INACCESSIBLE_PATHS.get(plane, frozenset())
    assert inaccessible[0] == "/home/lighthouse/rquant/.env"
    assert set(inaccessible[1:]) == {
        f"-{CURRENT_ROOT}/secrets",
        f"-{CURRENT_ROOT}/credentials",
        *required,
    }
    assert all(path.startswith("-") for path in inaccessible[1:] if path not in required)
    assert all(not path.startswith("-") for path in required)
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
