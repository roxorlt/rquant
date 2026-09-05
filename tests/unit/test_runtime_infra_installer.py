from __future__ import annotations

import hashlib
import json
import os
import shutil
import stat
import subprocess
import sys
import time
import zipfile
from pathlib import Path

import pytest

from rquant.lab_highwater_authority import load_highwater_trusted_keys
from rquant.runtime_contracts import canonical_sha256
from rquant.strict_json import canonical_json_bytes

ROOT = Path(__file__).resolve().parents[2]
INSTALLER = ROOT / "scripts" / "install-runtime-credential-infra.sh"


@pytest.mark.parametrize(
    "failure_step",
    (
        "libexec_dir",
        "helper_install",
        "helper_publish",
        "sudoers_install",
        "sudoers_validate_staging",
        "sudoers_publish",
        "sudoers_validate_final",
    ),
)
def test_failure_is_nonzero_and_same_head_rerun_recovers(
    tmp_path: Path,
    failure_step: str,
) -> None:
    _provision_highwater_key_material(tmp_path)
    failed = subprocess.run(
        [
            "/bin/bash",
            str(INSTALLER),
            "--test-root",
            str(tmp_path),
            "--fail-step",
            failure_step,
        ],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    assert failed.returncode != 0
    assert "installed" not in failed.stdout.lower()

    recovered = subprocess.run(
        ["/bin/bash", str(INSTALLER), "--test-root", str(tmp_path)],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    assert recovered.returncode == 0, recovered.stderr
    helper = tmp_path / "usr/local/libexec/rquant-runtime-credential-sealer"
    highwater_helper = tmp_path / "usr/local/libexec/rquant-lab-highwater-authority"
    canvas_helper = tmp_path / "usr/local/libexec/rquant-canvas-publication-signer"
    shadow_helper = tmp_path / "usr/local/libexec/rquant-shadow-report-signer"
    daily_helper = tmp_path / "usr/local/libexec/rquant-daily-receipt-signer"
    daily_socket_unit = tmp_path / "etc/systemd/system/rquant-daily-receipt-signer.socket"
    daily_service_unit = tmp_path / "etc/systemd/system/rquant-daily-receipt-signer.service"
    highwater_state = tmp_path / "var/lib/rquant/lab-highwater"
    daily_state = tmp_path / "var/lib/rquant/daily-receipt-signer"
    shadow_recovery_state = tmp_path / "var/lib/rquant/shadow-recovery"
    highwater_public_keys = tmp_path / "etc/rquant/lab-highwater-trusted-keys.json"
    canvas_public_keys = tmp_path / "etc/rquant/canvas-publication-trusted-keys.json"
    shadow_public_keys = tmp_path / "etc/rquant/shadow-report-trusted-keys.json"
    daily_public_keys = tmp_path / "etc/rquant/daily-receipt-trusted-keys.json"
    sudoers = tmp_path / "etc/sudoers.d/rquant-production-deploy"
    assert (
        helper.read_bytes()
        == (ROOT / "deploy/libexec/rquant-runtime-credential-sealer").read_bytes()
    )
    assert sudoers.read_bytes() == (ROOT / "deploy/sudoers/rquant-production-deploy").read_bytes()
    assert helper.stat().st_mode & 0o777 == 0o755
    assert highwater_helper.stat().st_mode & 0o777 == 0o755
    assert (
        canvas_helper.read_bytes()
        == (ROOT / "deploy/libexec/rquant-canvas-publication-signer").read_bytes()
    )
    assert canvas_helper.stat().st_mode & 0o777 == 0o755
    assert (
        shadow_helper.read_bytes()
        == (ROOT / "deploy/libexec/rquant-shadow-report-signer").read_bytes()
    )
    assert shadow_helper.stat().st_mode & 0o777 == 0o755
    assert (
        daily_helper.read_bytes()
        == (ROOT / "deploy/libexec/rquant-daily-receipt-signer").read_bytes()
    )
    assert daily_helper.stat().st_mode & 0o777 == 0o755
    assert (
        daily_socket_unit.read_bytes()
        == (ROOT / "deploy/systemd/rquant-daily-receipt-signer.socket").read_bytes()
    )
    assert (
        daily_service_unit.read_bytes()
        == (ROOT / "deploy/systemd/rquant-daily-receipt-signer.service").read_bytes()
    )
    assert daily_socket_unit.stat().st_mode & 0o777 == 0o644
    assert daily_service_unit.stat().st_mode & 0o777 == 0o644
    assert stat.S_IMODE(highwater_state.stat().st_mode) == 0o700
    assert stat.S_IMODE(daily_state.stat().st_mode) == 0o700
    assert stat.S_IMODE(shadow_recovery_state.stat().st_mode) == 0o700
    assert stat.S_IMODE(highwater_public_keys.stat().st_mode) == 0o444
    assert stat.S_IMODE(canvas_public_keys.stat().st_mode) == 0o444
    assert stat.S_IMODE(shadow_public_keys.stat().st_mode) == 0o444
    assert stat.S_IMODE(daily_public_keys.stat().st_mode) == 0o444
    assert load_highwater_trusted_keys(highwater_public_keys).active_key_id == "hw-v1"
    daily_keyring = json.loads(daily_public_keys.read_text(encoding="utf-8"))
    assert daily_keyring["active_key_id"] == "daily-v1"
    assert daily_keyring["previous_public_keys"] == {}
    assert "PRIVATE KEY" not in daily_public_keys.read_text(encoding="utf-8")
    assert sudoers.stat().st_mode & 0o777 == 0o440
    assert stat.S_IMODE(sudoers.parent.stat().st_mode) == 0o750


def test_installer_rejects_a_tampered_shadow_recovery_calendar(
    tmp_path: Path,
) -> None:
    _provision_highwater_key_material(tmp_path)
    calendar = tmp_path / "etc/rquant/shadow-report/legacy-recovery-calendar.json"
    document = json.loads(calendar.read_bytes())
    document["open_dates"] = []
    calendar.write_bytes(canonical_json_bytes(document))
    calendar.chmod(0o600)

    rejected = subprocess.run(
        ["/bin/bash", str(INSTALLER), "--test-root", str(tmp_path)],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )

    assert rejected.returncode != 0
    assert "calendar" in rejected.stderr.lower()


def test_installer_rejects_a_symlinked_shadow_recovery_state_directory(
    tmp_path: Path,
) -> None:
    _provision_highwater_key_material(tmp_path)
    state_parent = tmp_path / "var/lib/rquant"
    state_parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    redirected = tmp_path / "redirected-shadow-state"
    redirected.mkdir(mode=0o700)
    (state_parent / "shadow-recovery").symlink_to(redirected, target_is_directory=True)

    rejected = subprocess.run(
        ["/bin/bash", str(INSTALLER), "--test-root", str(tmp_path)],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )

    assert rejected.returncode != 0
    assert "symbolic link" in rejected.stderr.lower()


def test_shadow_recovery_helper_has_fixed_openat_authority_contract() -> None:
    helper = (ROOT / "deploy/libexec/rquant-shadow-report-signer").read_text(encoding="utf-8")
    installer = INSTALLER.read_text(encoding="utf-8")

    assert 'LEGACY_SHADOW_ROOT = Path("/home/lighthouse/rquant/data/legacy-shadow")' in helper
    assert 'RECOVERY_STATE_ROOT = Path("/var/lib/rquant/shadow-recovery")' in helper
    assert "_open_trusted_staging" in helper
    assert 'getattr(os, "O_NOFOLLOW", 0)' in helper
    assert 'SHADOW_RECOVERY_STATE_DIR="${PREFIX}/var/lib/rquant/shadow-recovery"' in installer


def test_installer_enables_daily_receipt_socket_not_service() -> None:
    installer = INSTALLER.read_text(encoding="utf-8")

    assert "systemctl_run daemon-reload" in installer
    assert "systemctl_run enable --now" in installer
    assert "rquant-daily-receipt-signer.socket" in installer
    assert "systemctl_run enable --now rquant-daily-receipt-signer.service" not in installer
    assert "RQuantDailyAuthorityRelease" not in installer
    assert "validate_running_daily_authority_identity" in installer
    assert "rquant-daily-receipt-authority.identity" in installer


@pytest.mark.parametrize("failure_mode", ("daemon_reload", "socket_enable", "post_start_health"))
def test_daily_authority_linux_like_faults_restore_old_runtime_identity(
    tmp_path: Path,
    failure_mode: str,
) -> None:
    (
        old_target,
        candidate_target,
        state,
        calls,
        fake_systemctl,
        identity_probe,
        identity_socket,
    ) = _prepare_daily_systemctl_fixture(tmp_path)
    try:
        failed = subprocess.run(
            [
                "/bin/bash",
                str(INSTALLER),
                "--test-root",
                str(tmp_path),
                "--test-daily-authority-source",
                str(tmp_path / "candidate-authority.py"),
                "--test-systemctl",
                str(fake_systemctl),
            ],
            cwd=ROOT,
            env={
                **os.environ,
                "FAKE_SYSTEMCTL_STATE": str(state),
                "FAKE_SYSTEMCTL_CALLS": str(calls),
                "FAKE_SYSTEMCTL_CURRENT": str(
                    tmp_path / "usr/local/libexec/rquant-daily-receipt-authority/current"
                ),
                "FAKE_SYSTEMCTL_FAIL_MODE": failure_mode,
                "RQUANT_TEST_DAILY_AUTHORITY_SOCKET": str(identity_socket),
            },
            check=False,
            capture_output=True,
            text=True,
        )
    finally:
        _stop_identity_probe(identity_probe)

    assert failed.returncode != 0, failed.stdout + failed.stderr
    assert (
        os.readlink(tmp_path / "usr/local/libexec/rquant-daily-receipt-authority/current")
        == old_target
    )
    assert _read_fake_state(state)["runtime_identity"] == old_target.removeprefix("releases/")
    assert _read_fake_state(state)["socket_active"] == "1"
    assert _read_fake_state(state)["service_active"] == "1"
    assert _read_fake_state(state)["socket_enabled"] == "1"
    calls_text = calls.read_text(encoding="utf-8")
    assert "systemctl restart rquant-daily-receipt-signer.socket" in calls_text
    assert "systemctl restart rquant-daily-receipt-signer.service" in calls_text
    assert calls_text.count("systemctl daemon-reload") >= 2
    assert candidate_target.removeprefix("releases/") != old_target.removeprefix("releases/")


def test_daily_authority_linux_like_success_runs_new_identity_and_restores_active_state(
    tmp_path: Path,
) -> None:
    (
        old_target,
        candidate_target,
        state,
        calls,
        fake_systemctl,
        identity_probe,
        identity_socket,
    ) = _prepare_daily_systemctl_fixture(tmp_path)
    try:
        installed = subprocess.run(
            [
                "/bin/bash",
                str(INSTALLER),
                "--test-root",
                str(tmp_path),
                "--test-daily-authority-source",
                str(tmp_path / "candidate-authority.py"),
                "--test-systemctl",
                str(fake_systemctl),
            ],
            cwd=ROOT,
            env={
                **os.environ,
                "FAKE_SYSTEMCTL_STATE": str(state),
                "FAKE_SYSTEMCTL_CALLS": str(calls),
                "FAKE_SYSTEMCTL_CURRENT": str(
                    tmp_path / "usr/local/libexec/rquant-daily-receipt-authority/current"
                ),
                "RQUANT_TEST_DAILY_AUTHORITY_SOCKET": str(identity_socket),
            },
            check=False,
            capture_output=True,
            text=True,
        )
    finally:
        _stop_identity_probe(identity_probe)

    assert installed.returncode == 0, installed.stdout + installed.stderr
    assert (
        os.readlink(tmp_path / "usr/local/libexec/rquant-daily-receipt-authority/current")
        == candidate_target
    )
    assert _read_fake_state(state)["runtime_identity"] == candidate_target.removeprefix("releases/")
    assert _read_fake_state(state)["socket_active"] == "1"
    assert _read_fake_state(state)["service_active"] == "1"
    calls_text = calls.read_text(encoding="utf-8")
    assert "systemctl daemon-reload" in calls_text
    assert "systemctl enable --now rquant-daily-receipt-signer.socket" in calls_text
    assert "systemctl start rquant-daily-receipt-signer.service" in calls_text
    assert "systemctl restart rquant-daily-receipt-signer.service" not in calls_text
    assert candidate_target != old_target


def test_daily_authority_rejects_old_process_when_current_pointer_moves(
    tmp_path: Path,
) -> None:
    """A stale process identity cannot pass merely because ``current`` moved."""

    old_target, _candidate_target, state, calls, fake_systemctl, probe, endpoint = (
        _prepare_daily_systemctl_fixture(tmp_path)
    )
    try:
        rejected = subprocess.run(
            [
                "/bin/bash",
                str(INSTALLER),
                "--test-root",
                str(tmp_path),
                "--test-daily-authority-source",
                str(tmp_path / "candidate-authority.py"),
                "--test-systemctl",
                str(fake_systemctl),
            ],
            cwd=ROOT,
            env={
                **os.environ,
                "FAKE_SYSTEMCTL_STATE": str(state),
                "FAKE_SYSTEMCTL_CALLS": str(calls),
                "FAKE_SYSTEMCTL_CURRENT": str(
                    tmp_path / "usr/local/libexec/rquant-daily-receipt-authority/current"
                ),
                "FAKE_SYSTEMCTL_KEEP_RUNTIME_IDENTITY": "1",
                "RQUANT_TEST_DAILY_AUTHORITY_SOCKET": str(endpoint),
            },
            check=False,
            capture_output=True,
            text=True,
        )
    finally:
        _stop_identity_probe(probe)

    assert rejected.returncode != 0
    assert "runtime identity mismatch" in rejected.stderr
    assert (
        os.readlink(tmp_path / "usr/local/libexec/rquant-daily-receipt-authority/current")
        == old_target
    )
    assert _read_fake_state(state)["runtime_identity"] == old_target.removeprefix("releases/")


def _prepare_daily_systemctl_fixture(
    tmp_path: Path,
) -> tuple[str, str, Path, Path, Path, subprocess.Popen[bytes], Path]:
    _provision_highwater_key_material(tmp_path)
    first = subprocess.run(
        ["/bin/bash", str(INSTALLER), "--test-root", str(tmp_path)],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    assert first.returncode == 0, first.stderr

    authority_root = tmp_path / "usr/local/libexec/rquant-daily-receipt-authority"
    current = authority_root / "current"
    old_target = os.readlink(current)
    candidate_source = tmp_path / "candidate-authority.py"
    candidate_source.write_bytes(
        (ROOT / "deploy/root-runtime/daily_receipt_authority.py").read_bytes()
        + b"\n# linux-like installer fault fixture\n"
    )
    candidate_sha = hashlib.sha256(candidate_source.read_bytes()).hexdigest()
    candidate_target = f"releases/{candidate_sha}"

    state = tmp_path / "fake-systemctl.state"
    _write_fake_state(
        state,
        {
            "socket_active": "1",
            "service_active": "1",
            "socket_enabled": "1",
            "service_enabled": "0",
            "runtime_identity": old_target.removeprefix("releases/"),
            "service_started": "0",
        },
    )
    calls = tmp_path / "fake-systemctl.calls"
    fake_systemctl = tmp_path / "fake-systemctl"
    _write_fake_systemctl(fake_systemctl)
    identity_probe, identity_socket = _start_identity_probe(tmp_path, state)
    return (
        old_target,
        candidate_target,
        state,
        calls,
        fake_systemctl,
        identity_probe,
        identity_socket,
    )


def _start_identity_probe(
    tmp_path: Path,
    state: Path,
    *,
    private_key: Path | None = None,
    key_id: str = "daily-v1",
    fault: str = "",
    fault_signing_key: Path | None = None,
    fault_key_id: str | None = None,
) -> tuple[subprocess.Popen[bytes], Path]:
    # Darwin caps AF_UNIX paths at roughly 104 bytes; keep the unique socket
    # below pytest's temporary root rather than inside the long test directory.
    endpoint = tmp_path.parents[2] / (
        "rqdi-" + hashlib.sha256(str(tmp_path).encode("utf-8")).hexdigest()[:16] + ".sock"
    )
    command = [
        sys.executable,
        str(ROOT / "tests/support/daily_authority_identity_probe.py"),
        "--endpoint",
        str(endpoint),
        "--state",
        str(state),
        "--private-key",
        str(private_key or tmp_path / "etc/rquant/daily-receipt/daily-v1.private.pem"),
        "--key-id",
        key_id,
        "--max-connections",
        "16",
    ]
    if fault:
        command.extend(("--fault", fault))
    if fault_signing_key is not None:
        command.extend(("--fault-signing-key", str(fault_signing_key)))
    if fault_key_id is not None:
        command.extend(("--fault-key-id", fault_key_id))
    process = subprocess.Popen(
        command,
        cwd=ROOT,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        if process.poll() is not None:
            stdout, stderr = process.communicate(timeout=1)
            raise AssertionError(
                f"identity probe exited early: {process.returncode}\n{stdout!r}\n{stderr!r}"
            )
        if endpoint.exists():
            return process, endpoint
        time.sleep(0.02)
    process.kill()
    stdout, stderr = process.communicate(timeout=1)
    raise AssertionError(f"identity probe did not start\n{stdout!r}\n{stderr!r}")


@pytest.mark.parametrize(
    "fault",
    ("bad-signature", "wrong-key-id", "nonce-tamper", "source-sha-tamper"),
)
def test_daily_authority_identity_faults_are_rejected_and_rollback_is_proven(
    tmp_path: Path,
    fault: str,
) -> None:
    old_target, _candidate_target, state, calls, fake_systemctl, probe, endpoint = (
        _prepare_daily_systemctl_fixture(tmp_path)
    )
    _stop_identity_probe(probe)
    fault_probe, fault_endpoint = _start_identity_probe(tmp_path, state, fault=fault)
    try:
        rejected = subprocess.run(
            [
                "/bin/bash",
                str(INSTALLER),
                "--test-root",
                str(tmp_path),
                "--test-daily-authority-source",
                str(tmp_path / "candidate-authority.py"),
                "--test-systemctl",
                str(fake_systemctl),
            ],
            cwd=ROOT,
            env={
                **os.environ,
                "FAKE_SYSTEMCTL_STATE": str(state),
                "FAKE_SYSTEMCTL_CALLS": str(calls),
                "FAKE_SYSTEMCTL_CURRENT": str(
                    tmp_path / "usr/local/libexec/rquant-daily-receipt-authority/current"
                ),
                "RQUANT_TEST_DAILY_AUTHORITY_SOCKET": str(fault_endpoint),
            },
            check=False,
            capture_output=True,
            text=True,
        )
    finally:
        _stop_identity_probe(fault_probe)

    assert rejected.returncode != 0, rejected.stdout + rejected.stderr
    assert (
        os.readlink(tmp_path / "usr/local/libexec/rquant-daily-receipt-authority/current")
        == old_target
    )
    assert _read_fake_state(state)["runtime_identity"] == old_target.removeprefix("releases/")


def test_daily_authority_identity_previous_key_is_rejected_even_with_a_real_signature(
    tmp_path: Path,
) -> None:
    old_target, _candidate_target, state, calls, fake_systemctl, probe, _endpoint = (
        _prepare_daily_systemctl_fixture(tmp_path)
    )
    _stop_identity_probe(probe)
    previous_key = tmp_path / "etc/rquant/daily-receipt/daily-v1.private.pem"
    daily_public_path = tmp_path / "etc/rquant/daily-receipt-trusted-keys.json"
    first_keyring = json.loads(daily_public_path.read_text(encoding="utf-8"))
    _rotate_daily_key_material(
        tmp_path,
        previous_manifest_hash=first_keyring["manifest_hash"],
        keep_previous_private=True,
    )
    previous_probe, previous_endpoint = _start_identity_probe(
        tmp_path,
        state,
        fault="previous-key",
        fault_signing_key=previous_key,
        private_key=tmp_path / "etc/rquant/daily-receipt/daily-v2.private.pem",
        key_id="daily-v2",
        fault_key_id="daily-v1",
    )
    try:
        rejected = subprocess.run(
            [
                "/bin/bash",
                str(INSTALLER),
                "--test-root",
                str(tmp_path),
                "--test-daily-authority-source",
                str(tmp_path / "candidate-authority.py"),
                "--test-systemctl",
                str(fake_systemctl),
            ],
            cwd=ROOT,
            env={
                **os.environ,
                "FAKE_SYSTEMCTL_STATE": str(state),
                "FAKE_SYSTEMCTL_CALLS": str(calls),
                "FAKE_SYSTEMCTL_CURRENT": str(
                    tmp_path / "usr/local/libexec/rquant-daily-receipt-authority/current"
                ),
                "RQUANT_TEST_DAILY_AUTHORITY_SOCKET": str(previous_endpoint),
            },
            check=False,
            capture_output=True,
            text=True,
        )
    finally:
        _stop_identity_probe(previous_probe)

    assert rejected.returncode != 0, rejected.stdout + rejected.stderr
    assert (
        os.readlink(tmp_path / "usr/local/libexec/rquant-daily-receipt-authority/current")
        == old_target
    )
    assert _read_fake_state(state)["runtime_identity"] == old_target.removeprefix("releases/")


def _stop_identity_probe(process: subprocess.Popen[bytes]) -> None:
    if process.poll() is not None:
        return
    process.terminate()
    try:
        process.communicate(timeout=2)
    except subprocess.TimeoutExpired:
        process.kill()
        process.communicate(timeout=2)


def _write_fake_state(path: Path, values: dict[str, str]) -> None:
    path.write_text(
        "".join(f"{key}={value}\n" for key, value in values.items()),
        encoding="utf-8",
    )


def _read_fake_state(path: Path) -> dict[str, str]:
    return dict(
        line.split("=", 1) for line in path.read_text(encoding="utf-8").splitlines() if line
    )


def _write_fake_systemctl(path: Path) -> None:
    path.write_text(
        r"""#!/usr/bin/env bash
set -u

state="${FAKE_SYSTEMCTL_STATE:?}"
calls="${FAKE_SYSTEMCTL_CALLS:?}"
current="${FAKE_SYSTEMCTL_CURRENT:?}"
printf 'systemctl %s\n' "$*" >>"${calls}"

load_state() {
    # The fixture owns this file and stores shell-safe scalar values only.
    source "${state}"
}

save_state() {
    cat >"${state}" <<EOF
socket_active=${socket_active}
service_active=${service_active}
socket_enabled=${socket_enabled}
service_enabled=${service_enabled}
runtime_identity=${runtime_identity}
service_started=${service_started}
EOF
}

fail_once() {
    local name="$1"
    local marker="${state}.${name}.once"
    if [[ "${FAKE_SYSTEMCTL_FAIL_MODE:-}" == "${name}" && ! -e "${marker}" ]]; then
        : >"${marker}"
        return 1
    fi
    return 0
}

current_identity() {
    local target
    target=$(readlink "${current}")
    printf '%s\n' "${target#releases/}"
}

load_state
case "${1:-}" in
  daemon-reload)
    fail_once daemon_reload
    exit $?
    ;;
  enable)
    unit="${@: -1}"
    if [[ "${2:-}" == "--now" && "${unit}" == "rquant-daily-receipt-signer.socket" ]]; then
        fail_once socket_enable || exit $?
        socket_enabled=1
        socket_active=1
    elif [[ "${unit}" == "rquant-daily-receipt-signer.socket" ]]; then
        socket_enabled=1
    elif [[ "${unit}" == "rquant-daily-receipt-signer.service" ]]; then
        service_enabled=1
    fi
    save_state
    exit 0
    ;;
  disable)
    unit="${@: -1}"
    if [[ "${unit}" == "rquant-daily-receipt-signer.socket" ]]; then socket_enabled=0; fi
    if [[ "${unit}" == "rquant-daily-receipt-signer.service" ]]; then service_enabled=0; fi
    save_state
    exit 0
    ;;
  start|restart)
    unit="${@: -1}"
    if [[ "${unit}" == "rquant-daily-receipt-signer.socket" ]]; then
        socket_active=1
    elif [[ "${unit}" == "rquant-daily-receipt-signer.service" ]]; then
        service_active=1
        service_started=1
        if [[ "${FAKE_SYSTEMCTL_KEEP_RUNTIME_IDENTITY:-0}" != "1" ]]; then
            runtime_identity=$(current_identity)
        fi
    fi
    save_state
    exit 0
    ;;
  stop)
    for unit in "${@:2}"; do
      if [[ "${unit}" == "rquant-daily-receipt-signer.socket" ]]; then socket_active=0; fi
      if [[ "${unit}" == "rquant-daily-receipt-signer.service" ]]; then
          service_active=0
          service_started=0
      fi
    done
    save_state
    exit 0
    ;;
  is-active)
    unit="${@: -1}"
    active=0
    if [[ "${unit}" == "rquant-daily-receipt-signer.socket" ]]; then active=${socket_active}; fi
    if [[ "${unit}" == "rquant-daily-receipt-signer.service" ]]; then active=${service_active}; fi
    if [[ "${unit}" == "rquant-daily-receipt-signer.service" &&
          "${FAKE_SYSTEMCTL_FAIL_MODE:-}" == "post_start_health" &&
          "${service_started}" == "1" && ! -e "${state}.post_start_health.once" ]]; then
        : >"${state}.post_start_health.once"
        exit 3
    fi
    if [[ "${active}" == "1" ]]; then
        [[ "${2:-}" == "--quiet" ]] || printf 'active\n'
        exit 0
    fi
    [[ "${2:-}" == "--quiet" ]] || printf 'inactive\n'
    exit 3
    ;;
  is-enabled)
    unit="${@: -1}"
    enabled=0
    if [[ "${unit}" == "rquant-daily-receipt-signer.socket" ]]; then enabled=${socket_enabled}; fi
    if [[ "${unit}" == "rquant-daily-receipt-signer.service" ]]; then enabled=${service_enabled}; fi
    if [[ "${enabled}" == "1" ]]; then
        [[ "${2:-}" == "--quiet" ]] || printf 'enabled\n'
        exit 0
    fi
    [[ "${2:-}" == "--quiet" ]] || printf 'disabled\n'
    exit 1
    ;;
  show)
    exit 1
    ;;
esac
exit 1
""",
        encoding="utf-8",
    )
    path.chmod(0o755)


def test_daily_receipt_authority_runs_only_a_root_managed_release_artifact() -> None:
    """The root socket service must never execute the writable checkout or its venv."""

    service = (ROOT / "deploy/systemd/rquant-daily-receipt-signer.service").read_text(
        encoding="utf-8"
    )
    installer = INSTALLER.read_text(encoding="utf-8")

    assert "/home/lighthouse/rquant" not in service
    assert "ExecStart=/usr/bin/python3 -I -S " in service
    assert "/usr/local/libexec/rquant-daily-receipt-authority/current/authority.pyz" in service
    assert "DAILY_AUTHORITY_RELEASES_DIR" in installer
    assert "authority.pyz" in installer
    assert "sha256sum" in installer


def test_installed_daily_authority_zipapp_is_independent_from_checkout_and_venv(
    tmp_path: Path,
) -> None:
    """Later mutation of a runtime user's source tree cannot change the root artifact."""

    _provision_highwater_key_material(tmp_path)
    installed = subprocess.run(
        ["/bin/bash", str(INSTALLER), "--test-root", str(tmp_path)],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    assert installed.returncode == 0, installed.stderr

    authority_root = tmp_path / "usr/local/libexec/rquant-daily-receipt-authority"
    current = authority_root / "current"
    assert current.is_symlink()
    assert os.readlink(current).startswith("releases/")
    assert tuple(path for path in authority_root.iterdir() if path.is_symlink()) == (current,)
    artifact = current / "authority.pyz"
    digest_before = artifact.read_bytes()
    assert stat.S_IMODE(artifact.stat().st_mode) == 0o555
    assert (current / "source.sha256").read_text(encoding="utf-8").strip()

    with zipfile.ZipFile(artifact) as bundle:
        source = bundle.read("__main__.py").decode("utf-8")
    release_sha = current.resolve().name
    assert hashlib.sha256(source.encode("utf-8")).hexdigest() == release_sha
    assert (current / "source.sha256").read_text(encoding="utf-8") == f"{release_sha}\n"
    assert "import rquant" not in source
    assert "pydantic" not in source
    assert "/home/lighthouse/rquant" not in source

    checkout = tmp_path / "home/lighthouse/rquant"
    checkout.mkdir(parents=True)
    (checkout / "src.py").write_text("raise RuntimeError('tampered checkout')\n")
    venv = checkout / ".venv/bin"
    venv.mkdir(parents=True)
    (venv / "python").write_text("raise RuntimeError('tampered venv')\n")

    probe = subprocess.run(
        ["/usr/bin/python3", "-I", "-S", str(artifact)],
        cwd=checkout,
        check=False,
        capture_output=True,
        text=True,
    )
    assert probe.returncode == 2
    assert "socket activation" in probe.stderr
    assert "tampered" not in probe.stderr
    assert artifact.read_bytes() == digest_before


def test_daily_authority_installer_rejects_a_writable_release_parent(tmp_path: Path) -> None:
    _provision_highwater_key_material(tmp_path)
    first = subprocess.run(
        ["/bin/bash", str(INSTALLER), "--test-root", str(tmp_path)],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    assert first.returncode == 0, first.stderr

    releases = tmp_path / "usr/local/libexec/rquant-daily-receipt-authority/releases"
    releases.chmod(0o777)
    rejected = subprocess.run(
        ["/bin/bash", str(INSTALLER), "--test-root", str(tmp_path)],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )

    assert rejected.returncode != 0
    assert "Unsafe Daily authority ancestor" in rejected.stderr


@pytest.mark.parametrize(
    ("failure_step", "release_is_published"),
    (
        ("daily_authority_build_after", True),
        ("daily_socket_unit_publish", True),
        ("daily_key_material", True),
        ("sudoers_validate_final", True),
        ("daily_authority_switch_after", True),
    ),
)
def test_daily_authority_late_failure_never_leaves_a_new_current_pointer(
    tmp_path: Path,
    failure_step: str,
    release_is_published: bool,
) -> None:
    """A prepared root release cannot become live before every later gate passes."""

    _provision_highwater_key_material(tmp_path)
    first = subprocess.run(
        ["/bin/bash", str(INSTALLER), "--test-root", str(tmp_path)],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    assert first.returncode == 0, first.stderr

    authority_root = tmp_path / "usr/local/libexec/rquant-daily-receipt-authority"
    current = authority_root / "current"
    old_target = os.readlink(current)
    candidate_source = tmp_path / f"candidate-{failure_step}.py"
    candidate_source.write_bytes(
        (ROOT / "deploy/root-runtime/daily_receipt_authority.py").read_bytes()
        + f"\n# fault-injection {failure_step}\n".encode("ascii")
    )
    candidate_sha = hashlib.sha256(candidate_source.read_bytes()).hexdigest()

    failed = subprocess.run(
        [
            "/bin/bash",
            str(INSTALLER),
            "--test-root",
            str(tmp_path),
            "--test-daily-authority-source",
            str(candidate_source),
            "--fail-step",
            failure_step,
        ],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )

    assert failed.returncode != 0
    assert f"Injected failure: {failure_step}" in failed.stderr
    assert os.readlink(current) == old_target
    candidate_release = authority_root / "releases" / candidate_sha
    assert candidate_release.exists() is release_is_published


def test_daily_authority_existing_release_must_bind_its_zipapp_payload(tmp_path: Path) -> None:
    """A same-source rerun rejects a release whose metadata and zip payload diverge."""

    _provision_highwater_key_material(tmp_path)
    first = subprocess.run(
        ["/bin/bash", str(INSTALLER), "--test-root", str(tmp_path)],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    assert first.returncode == 0, first.stderr

    artifact = tmp_path / "usr/local/libexec/rquant-daily-receipt-authority/current/authority.pyz"
    artifact.chmod(0o755)
    with zipfile.ZipFile(artifact, "w") as bundle:
        bundle.writestr("__main__.py", b"raise RuntimeError('tampered')\n")
    artifact.chmod(0o555)

    rejected = subprocess.run(
        ["/bin/bash", str(INSTALLER), "--test-root", str(tmp_path)],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )

    assert rejected.returncode != 0
    assert "zipapp" in rejected.stderr


@pytest.mark.parametrize("ancestor", ("usr/local", "usr/local/libexec"))
def test_daily_authority_installer_rejects_writable_or_symlinked_ancestor(
    tmp_path: Path,
    ancestor: str,
) -> None:
    _provision_highwater_key_material(tmp_path)
    first = subprocess.run(
        ["/bin/bash", str(INSTALLER), "--test-root", str(tmp_path)],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    assert first.returncode == 0, first.stderr

    ancestor_path = tmp_path / ancestor
    ancestor_path.chmod(0o777)
    writable = subprocess.run(
        ["/bin/bash", str(INSTALLER), "--test-root", str(tmp_path)],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    assert writable.returncode != 0
    assert "ancestor" in writable.stderr.lower()

    ancestor_path.chmod(0o755)
    renamed = ancestor_path.with_name(f"{ancestor_path.name}-real")
    ancestor_path.rename(renamed)
    ancestor_path.symlink_to(renamed.name, target_is_directory=True)
    symlinked = subprocess.run(
        ["/bin/bash", str(INSTALLER), "--test-root", str(tmp_path)],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    assert symlinked.returncode != 0
    assert "ancestor" in symlinked.stderr.lower()


def test_failed_sudoers_restore_preserves_backup_for_next_run(tmp_path: Path) -> None:
    _provision_highwater_key_material(tmp_path)
    sudoers_dir = tmp_path / "etc/sudoers.d"
    sudoers_dir.mkdir(parents=True, mode=0o750)
    sudoers_dir.chmod(0o750)
    target = sudoers_dir / "rquant-production-deploy"
    target.write_text("old-known-good\n")
    target.chmod(0o440)

    failed = subprocess.run(
        [
            "/bin/bash",
            str(INSTALLER),
            "--test-root",
            str(tmp_path),
            "--fail-step",
            "sudoers_validate_final,sudoers_restore",
        ],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )

    assert failed.returncode != 0
    backup = Path(f"{target}.backup")
    assert backup.read_text() == "old-known-good\n"
    assert str(backup) in failed.stderr

    recovered = subprocess.run(
        ["/bin/bash", str(INSTALLER), "--test-root", str(tmp_path)],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    assert recovered.returncode == 0, recovered.stderr
    assert target.read_bytes() == (ROOT / "deploy/sudoers/rquant-production-deploy").read_bytes()
    assert not backup.exists()
    assert stat.S_IMODE(sudoers_dir.stat().st_mode) == 0o750


def test_existing_sudoers_directory_mode_is_preserved(tmp_path: Path) -> None:
    _provision_highwater_key_material(tmp_path)
    sudoers_dir = tmp_path / "etc/sudoers.d"
    sudoers_dir.mkdir(parents=True, mode=0o700)
    sudoers_dir.chmod(0o700)

    result = subprocess.run(
        ["/bin/bash", str(INSTALLER), "--test-root", str(tmp_path)],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr
    assert stat.S_IMODE(sudoers_dir.stat().st_mode) == 0o700


def test_legacy_deployer_reconciles_infra_before_same_head_exit() -> None:
    deployer = (ROOT / "scripts/deploy.sh").read_text(encoding="utf-8")

    reconcile = deployer.index("install-runtime-credential-infra.sh")
    systemd_reconcile = deployer.index(
        "sudo cp '${SYSTEMD_DIR}/'*.{service,timer,socket,slice} '${SYSTEMD_TARGET}/'"
    )
    same_head_exit = deployer.index('ok "代码已最新（${PRE_HEAD:0:7}），基础设施校准完成。"')
    assert reconcile < same_head_exit
    assert systemd_reconcile < same_head_exit


def test_legacy_deployer_installs_runtime_slices_with_systemd_units() -> None:
    deployer = (ROOT / "scripts/deploy.sh").read_text(encoding="utf-8")

    assert "\\.(service|timer|socket|slice)$" in deployer
    assert "*.{service,timer,socket,slice}" in deployer


def test_key_fixture_resolves_openssl_from_linux_path(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        shutil,
        "which",
        lambda name: "/usr/bin/openssl" if name == "openssl" else None,
    )

    assert _fixture_openssl_binary() == "/usr/bin/openssl"


def _fixture_openssl_binary() -> str:
    binary = shutil.which("openssl")
    if binary is None:
        pytest.fail("openssl is required by the runtime infra installer fixture")
    return binary


def _provision_highwater_key_material(root: Path) -> None:
    # macOS creates direct /tmp children with gid 0 even for an unprivileged
    # owner.  Test mode deliberately validates the invoking uid/effective gid,
    # so normalize the fixture root before creating descendants.
    os.chown(root, os.geteuid(), os.getegid())
    key_dir = root / "etc/rquant/lab-highwater"
    key_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
    private_key = key_dir / "hw-v1.private.pem"
    subprocess.run(
        [_fixture_openssl_binary(), "genpkey", "-algorithm", "ED25519", "-out", str(private_key)],
        check=True,
        capture_output=True,
    )
    private_key.chmod(0o600)
    manifest = root / "etc/rquant/lab-highwater-keys.json"
    manifest.write_bytes(
        canonical_json_bytes(
            {
                "schema_version": 3,
                "generation": 1,
                "previous_manifest_hash": "0" * 64,
                "active_key_id": "hw-v1",
                "active_private_key_path": str(private_key),
                "previous_public_keys": {},
            }
        )
    )
    manifest.chmod(0o600)
    canvas_dir = root / "etc/rquant/canvas-publication"
    canvas_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
    canvas_private = canvas_dir / "canvas-v1.private.pem"
    subprocess.run(
        [
            _fixture_openssl_binary(),
            "genpkey",
            "-algorithm",
            "ED25519",
            "-out",
            str(canvas_private),
        ],
        check=True,
        capture_output=True,
    )
    canvas_private.chmod(0o600)
    canvas_manifest = root / "etc/rquant/canvas-publication-keys.json"
    canvas_manifest.write_bytes(
        canonical_json_bytes(
            {
                "schema_version": 1,
                "active_key_id": "canvas-v1",
                "active_private_key_path": str(canvas_private),
                "previous_public_keys": {},
            }
        )
    )
    canvas_manifest.chmod(0o600)
    shadow_dir = root / "etc/rquant/shadow-report"
    shadow_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
    shadow_private = shadow_dir / "shadow-v1.private.pem"
    subprocess.run(
        [
            _fixture_openssl_binary(),
            "genpkey",
            "-algorithm",
            "ED25519",
            "-out",
            str(shadow_private),
        ],
        check=True,
        capture_output=True,
    )
    shadow_private.chmod(0o600)
    recovery_calendar = shadow_dir / "legacy-recovery-calendar.json"
    recovery_calendar_body = {
        "schema_version": 1,
        "exchange": "SSE",
        "coverage_start": "2026-08-01",
        "coverage_end": "2026-08-31",
        "open_dates": ["2026-08-03"],
    }
    recovery_calendar.write_bytes(
        canonical_json_bytes(
            {
                **recovery_calendar_body,
                "content_sha256": canonical_sha256(recovery_calendar_body),
            }
        )
    )
    recovery_calendar.chmod(0o600)
    shadow_manifest = root / "etc/rquant/shadow-report-keys.json"
    shadow_manifest.write_bytes(
        canonical_json_bytes(
            {
                "schema_version": 2,
                "active_key_id": "shadow-v1",
                "active_private_key_path": str(shadow_private),
                "previous_public_keys": {},
                "legacy_recovery_calendar_path": str(recovery_calendar),
            }
        )
    )
    shadow_manifest.chmod(0o600)
    # The infra installer publishes a fifth public keyring, so the fixture host has to
    # look like one `install-runtime-credential-keys.sh init` produced: the completion
    # manifest is the *daily* shape (schema_version 2, chained), because the daily helper
    # is what validates and exports it.
    completion_dir = root / "etc/rquant/shadow-completion"
    completion_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
    completion_private = completion_dir / "completion-v1.private.pem"
    subprocess.run(
        [
            _fixture_openssl_binary(),
            "genpkey",
            "-algorithm",
            "ED25519",
            "-out",
            str(completion_private),
        ],
        check=True,
        capture_output=True,
    )
    completion_private.chmod(0o600)
    completion_manifest = root / "etc/rquant/shadow-completion-keys.json"
    completion_manifest.write_bytes(
        canonical_json_bytes(
            {
                "schema_version": 2,
                "generation": 1,
                "previous_manifest_hash": "0" * 64,
                "active_key_id": "completion-v1",
                "active_private_key_path": str(completion_private),
                "previous_public_keys": {},
            }
        )
    )
    completion_manifest.chmod(0o600)
    daily_dir = root / "etc/rquant/daily-receipt"
    daily_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
    daily_private = daily_dir / "daily-v1.private.pem"
    subprocess.run(
        [
            _fixture_openssl_binary(),
            "genpkey",
            "-algorithm",
            "ED25519",
            "-out",
            str(daily_private),
        ],
        check=True,
        capture_output=True,
    )
    daily_private.chmod(0o600)
    daily_manifest = root / "etc/rquant/daily-receipt-keys.json"
    daily_manifest.write_bytes(
        canonical_json_bytes(
            {
                "schema_version": 2,
                "generation": 1,
                "previous_manifest_hash": "0" * 64,
                "active_key_id": "daily-v1",
                "active_private_key_path": str(daily_private),
                "previous_public_keys": {},
            }
        )
    )
    daily_manifest.chmod(0o600)


def _public_key(private_key: Path) -> str:
    result = subprocess.run(
        [
            _fixture_openssl_binary(),
            "pkey",
            "-in",
            str(private_key),
            "-pubout",
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    return result.stdout


def _rotate_highwater_key_material(root: Path, *, previous_manifest_hash: str) -> None:
    key_dir = root / "etc/rquant/lab-highwater"
    previous_private = key_dir / "hw-v1.private.pem"
    previous_public = _public_key(previous_private)
    active_private = key_dir / "hw-v2.private.pem"
    subprocess.run(
        [
            _fixture_openssl_binary(),
            "genpkey",
            "-algorithm",
            "ED25519",
            "-out",
            str(active_private),
        ],
        check=True,
        capture_output=True,
    )
    active_private.chmod(0o600)
    manifest = root / "etc/rquant/lab-highwater-keys.json"
    manifest.write_bytes(
        canonical_json_bytes(
            {
                "schema_version": 3,
                "generation": 2,
                "previous_manifest_hash": previous_manifest_hash,
                "active_key_id": "hw-v2",
                "active_private_key_path": str(active_private),
                "previous_public_keys": {"hw-v1": previous_public},
            }
        )
    )
    manifest.chmod(0o600)
    previous_private.unlink()


def _rotate_daily_key_material(
    root: Path,
    *,
    previous_manifest_hash: str,
    keep_previous_private: bool = False,
) -> None:
    key_dir = root / "etc/rquant/daily-receipt"
    previous_private = key_dir / "daily-v1.private.pem"
    previous_public = _public_key(previous_private)
    active_private = key_dir / "daily-v2.private.pem"
    subprocess.run(
        [
            _fixture_openssl_binary(),
            "genpkey",
            "-algorithm",
            "ED25519",
            "-out",
            str(active_private),
        ],
        check=True,
        capture_output=True,
    )
    active_private.chmod(0o600)
    manifest = root / "etc/rquant/daily-receipt-keys.json"
    manifest.write_bytes(
        canonical_json_bytes(
            {
                "schema_version": 2,
                "generation": 2,
                "previous_manifest_hash": previous_manifest_hash,
                "active_key_id": "daily-v2",
                "active_private_key_path": str(active_private),
                "previous_public_keys": {"daily-v1": previous_public},
            }
        )
    )
    manifest.chmod(0o600)
    if not keep_previous_private:
        previous_private.unlink()


def test_installer_rotates_public_keyring_atomically_and_rejects_bad_chain(
    tmp_path: Path,
) -> None:
    _provision_highwater_key_material(tmp_path)
    first = subprocess.run(
        ["/bin/bash", str(INSTALLER), "--test-root", str(tmp_path)],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    assert first.returncode == 0, first.stderr
    public_path = tmp_path / "etc/rquant/lab-highwater-trusted-keys.json"
    first_payload = public_path.read_bytes()
    first_ring = load_highwater_trusted_keys(public_path)

    _rotate_highwater_key_material(tmp_path, previous_manifest_hash="f" * 64)
    rejected = subprocess.run(
        ["/bin/bash", str(INSTALLER), "--test-root", str(tmp_path)],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    assert rejected.returncode != 0
    assert public_path.read_bytes() == first_payload

    private_manifest = tmp_path / "etc/rquant/lab-highwater-keys.json"
    corrected = json.loads(private_manifest.read_text(encoding="utf-8"))
    corrected["previous_manifest_hash"] = first_ring.manifest_hash
    private_manifest.write_bytes(canonical_json_bytes(corrected))
    private_manifest.chmod(0o600)
    rotated = subprocess.run(
        ["/bin/bash", str(INSTALLER), "--test-root", str(tmp_path)],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    assert rotated.returncode == 0, rotated.stderr
    rotated_ring = load_highwater_trusted_keys(public_path)
    assert rotated_ring.active_key_id == "hw-v2"
    assert rotated_ring.previous_key_ids == ("hw-v1",)
    assert not (tmp_path / "etc/rquant/lab-highwater/hw-v1.private.pem").exists()


def test_installer_rotates_daily_public_keyring_without_private_history(
    tmp_path: Path,
) -> None:
    _provision_highwater_key_material(tmp_path)
    first = subprocess.run(
        ["/bin/bash", str(INSTALLER), "--test-root", str(tmp_path)],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    assert first.returncode == 0, first.stderr
    public_path = tmp_path / "etc/rquant/daily-receipt-trusted-keys.json"
    first_keyring = json.loads(public_path.read_text(encoding="utf-8"))

    _rotate_daily_key_material(tmp_path, previous_manifest_hash="f" * 64)
    rejected = subprocess.run(
        ["/bin/bash", str(INSTALLER), "--test-root", str(tmp_path)],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    assert rejected.returncode != 0
    assert json.loads(public_path.read_text(encoding="utf-8")) == first_keyring

    manifest = tmp_path / "etc/rquant/daily-receipt-keys.json"
    corrected = json.loads(manifest.read_text(encoding="utf-8"))
    corrected["previous_manifest_hash"] = first_keyring["manifest_hash"]
    manifest.write_bytes(canonical_json_bytes(corrected))
    manifest.chmod(0o600)
    rotated = subprocess.run(
        ["/bin/bash", str(INSTALLER), "--test-root", str(tmp_path)],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )

    assert rotated.returncode == 0, rotated.stderr
    keyring = json.loads(public_path.read_text(encoding="utf-8"))
    assert keyring["active_key_id"] == "daily-v2"
    assert keyring["generation"] == 2
    assert keyring["previous_manifest_hash"] == first_keyring["manifest_hash"]
    assert tuple(keyring["previous_public_keys"]) == ("daily-v1",)
    assert "PRIVATE KEY" not in public_path.read_text(encoding="utf-8")
    assert not (tmp_path / "etc/rquant/daily-receipt/daily-v1.private.pem").exists()


def test_sudoers_allows_only_no_argument_root_helpers() -> None:
    sudoers = (ROOT / "deploy/sudoers/rquant-production-deploy").read_text(encoding="utf-8")

    assert '/usr/local/libexec/rquant-lab-highwater-authority ""' in sudoers
    assert "rquant-lab-highwater-authority *" not in sudoers
    assert '/usr/local/libexec/rquant-canvas-publication-signer ""' in sudoers
    assert "rquant-canvas-publication-signer *" not in sudoers
    assert '/usr/local/libexec/rquant-shadow-report-signer ""' in sudoers
    assert "rquant-shadow-report-signer *" not in sudoers
    assert '/usr/local/libexec/rquant-daily-receipt-signer ""' not in sudoers
    assert "rquant-daily-receipt-signer *" not in sudoers
