"""Fault and rollback tests for legacy systemd runtime migration."""

from __future__ import annotations

import os
import subprocess
import sys
import time
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "scripts" / "migrate-legacy-runtime-slices.sh"
LEGACY_TEMPLATE = "rquant-runtime-live@.service"
LEGACY_INSTANCE = "rquant-runtime-live@svc-old.service"
REPLACEMENT = "rquant-runtime-feature@svc-new.service"
UNIT_CONTENT = "[Service]\nExecStart=/trusted/legacy\n"
TEST_CAPABILITY = "rquant-migration-test-capability-v1\n"


def _write_executable(path: Path, content: str) -> None:
    path.write_text(content, encoding="utf-8")
    path.chmod(0o755)


def _fixture(tmp_path: Path) -> tuple[Path, Path, Path]:
    root = tmp_path / "root"
    bin_dir = root / "usr/bin"
    unit_dir = root / "etc/systemd/system"
    state_dir = root / "state"
    bin_dir.mkdir(parents=True)
    unit_dir.mkdir(parents=True)
    state_dir.mkdir()
    (unit_dir / LEGACY_TEMPLATE).write_text(UNIT_CONTENT, encoding="utf-8")
    (state_dir / "old-active").write_text("active\n", encoding="utf-8")
    (state_dir / "old-enabled").write_text("enabled\n", encoding="utf-8")
    (state_dir / "durable-old-enabled").write_text("enabled\n", encoding="utf-8")
    (state_dir / "durable-template").write_text(UNIT_CONTENT, encoding="utf-8")
    capability = root / ".rquant-migration-test-capability-v1"
    capability.write_text(TEST_CAPABILITY, encoding="ascii")
    capability.chmod(0o600)
    _write_executable(
        bin_dir / "systemctl",
        r"""#!/usr/bin/env bash
set -euo pipefail
root=$(cd "$(dirname "$0")/../.." && pwd)
state="${root}/state"
units="${root}/etc/systemd/system"
printf '%s\n' "$*" >>"${state}/calls"
fail_once() {
    local step=$1
    local marker="${state}/failed-${step}"
    if [[ "${RQUANT_MIGRATION_FAULT:-}" == "${step}" && ! -e "${marker}" ]]; then
        : >"${marker}"
        return 1
    fi
}
case "${1:-}" in
  list-units)
    printf '%s loaded active running legacy\n' 'rquant-runtime-live@svc-old.service'
    ;;
  show)
    unit=${2:-}
    if [[ "${unit}" == 'rquant-live.slice' || "${unit}" == 'rquant-research.slice' ]]; then
        printf '/rquant.slice/%s\n' "${unit}"
    elif [[ "${unit}" == 'rquant-runtime-feature@svc-new.service' ]]; then
        fail_once replacement_show
        replacement_count=0
        if [[ -e "${state}/replacement-count" ]]; then
            replacement_count=$(cat "${state}/replacement-count")
        fi
        replacement_count=$((replacement_count + 1))
        printf '%s\n' "${replacement_count}" >"${state}/replacement-count"
        replacement_active=active
        if [[ "${RQUANT_MIGRATION_FAULT:-}" == replacement_post_state && \
              "${replacement_count}" -gt 1 ]]; then
            replacement_active=failed
        fi
        replacement_slice="${RQUANT_REPLACEMENT_SLICE:-rquant-live.slice}"
        printf '%s\n' \
          'LoadState=loaded' "ActiveState=${replacement_active}" 'UnitFileState=enabled' \
          "Slice=${replacement_slice}" \
          "ControlGroup=/rquant.slice/${replacement_slice}/rquant-runtime-feature@svc-new.service"
    elif [[ "${unit}" == 'rquant-runtime-live@svc-old.service' ]]; then
        if [[ "${RQUANT_MIGRATION_FAULT:-}" == snapshot_show ]]; then
            if [[ -e "${state}/seen-old-show" ]]; then
                exit 1
            fi
            : >"${state}/seen-old-show"
        fi
        unit_file_state=$(tr -d '\n' <"${state}/old-enabled")
        if [[ ! -e "${units}/rquant-runtime-live@.service" ]]; then
            unit_file_state=not-found
        fi
        printf 'LoadState=loaded\nActiveState=%s\nUnitFileState=%s\n' \
          "$(tr -d '\n' <"${state}/old-active")" "${unit_file_state}"
    elif [[ "${unit}" == 'rquant-runtime-live@.service' ]]; then
        if [[ "${RQUANT_MIGRATION_FAULT:-}" == post_verify && \
              ! -e "${state}/failed-post_verify" && ! -e "${units}/${unit}" ]]; then
            : >"${state}/failed-post_verify"
            printf '%s\n' loaded
        elif [[ -e "${units}/${unit}" ]]; then
            printf '%s\n' loaded
        else
            printf '%s\n' not-found
        fi
    else
        printf '%s\n' 'LoadState=not-found'
    fi
    ;;
  disable)
    fail_once disable
    printf 'inactive\n' >"${state}/old-active"
    printf 'disabled\n' >"${state}/old-enabled"
    ;;
  daemon-reload)
    fail_once daemon_reload
    ;;
  enable)
    printf 'enabled\n' >"${state}/old-enabled"
    ;;
  start)
    printf 'active\n' >"${state}/old-active"
    ;;
  stop)
    printf 'inactive\n' >"${state}/old-active"
    ;;
  *)
    printf 'unsupported fake systemctl call: %s\n' "$*" >&2
    exit 9
    ;;
esac
""",
    )
    _write_executable(
        bin_dir / "rm",
        r"""#!/usr/bin/env bash
set -euo pipefail
root=$(cd "$(dirname "$0")/../.." && pwd)
state="${root}/state"
marker="${state}/failed-remove"
if [[ "${RQUANT_MIGRATION_FAULT:-}" == remove && ! -e "${marker}" ]]; then
    : >"${marker}"
    exit 1
fi
exec /bin/rm "$@"
""",
    )
    _write_executable(
        bin_dir / "flock",
        f"""#!{sys.executable}
import fcntl
import os
import pathlib
import sys
import time

fd = int(sys.argv[-1])
try:
    fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
except BlockingIOError:
    raise SystemExit(1)
hold_seconds = float(os.environ.get("RQUANT_MIGRATION_TEST_FLOCK_HOLD_SECONDS", "0"))
if hold_seconds:
    root = pathlib.Path(__file__).resolve().parents[2]
    (root / "state" / "migration-lock-held").write_text("held\\n", encoding="utf-8")
    time.sleep(hold_seconds)
""",
    )
    _write_executable(
        bin_dir / "sync",
        r"""#!/usr/bin/env bash
set -euo pipefail
root=$(cd "$(dirname "$0")/../.." && pwd)
state="${root}/state"
units="${root}/etc/systemd/system"
[[ "$*" == "-f ${units}" ]] || {
    printf 'unexpected filesystem sync target: %s\n' "$*" >&2
    exit 9
}
count=0
if [[ -e "${state}/sync-count" ]]; then
    count=$(<"${state}/sync-count")
fi
count=$((count + 1))
printf '%s\n' "${count}" >"${state}/sync-count"
printf 'sync -f %s\n' "${units}" >>"${state}/calls"
marker="${state}/failed-filesystem_sync"
if [[ "${RQUANT_MIGRATION_FAULT:-}" == filesystem_sync_always ]]; then
    exit 1
fi
if [[ "${RQUANT_MIGRATION_FAULT:-}" == filesystem_sync && ! -e "${marker}" ]]; then
    : >"${marker}"
    exit 1
fi
if [[ "${RQUANT_MIGRATION_TEST_KILL_ON_SYNC_CALL:-}" == "${count}" ]]; then
    kill -KILL "${PPID}"
    exit 137
fi
/bin/cp "${state}/old-enabled" "${state}/durable-old-enabled"
if [[ -e "${units}/rquant-runtime-live@.service" ]]; then
    /bin/cp "${units}/rquant-runtime-live@.service" "${state}/durable-template"
else
    /bin/rm -f "${state}/durable-template"
fi
""",
    )
    (bin_dir / "python3").symlink_to(sys.executable)
    return root, unit_dir, state_dir


def _run(
    root: Path,
    *,
    accept: bool,
    fault: str | None = None,
    replacement: str = REPLACEMENT,
    replacement_slice: str | None = None,
    signal_after_disable: str | None = None,
    power_loss_after_prepared: bool = False,
    power_loss_after_disable: bool = False,
    power_loss_after_commit: bool = False,
    fail_after_unit_sync: bool = False,
    kill_on_sync_call: int | None = None,
) -> subprocess.CompletedProcess[str]:
    command = [
        "/bin/bash",
        str(SCRIPT),
        "--test-root",
        str(root),
        "--replacement",
        f"{LEGACY_TEMPLATE}={replacement}",
    ]
    if accept:
        command.append("--accept")
    env = os.environ.copy()
    if fault is not None:
        env["RQUANT_MIGRATION_FAULT"] = fault
    if replacement_slice is not None:
        env["RQUANT_REPLACEMENT_SLICE"] = replacement_slice
    if signal_after_disable is not None:
        env["RQUANT_MIGRATION_TEST_SIGNAL_AFTER_DISABLE"] = signal_after_disable
    if power_loss_after_prepared:
        env["RQUANT_MIGRATION_TEST_POWER_LOSS_AFTER_PREPARED"] = "1"
    if power_loss_after_disable:
        env["RQUANT_MIGRATION_TEST_POWER_LOSS_AFTER_DISABLE"] = "1"
    if power_loss_after_commit:
        env["RQUANT_MIGRATION_TEST_POWER_LOSS_AFTER_COMMIT"] = "1"
    if fail_after_unit_sync:
        env["RQUANT_MIGRATION_TEST_FAIL_AFTER_UNIT_SYNC"] = "1"
    if kill_on_sync_call is not None:
        env["RQUANT_MIGRATION_TEST_KILL_ON_SYNC_CALL"] = str(kill_on_sync_call)
    return subprocess.run(
        command,
        cwd=ROOT,
        env=env,
        check=False,
        capture_output=True,
        text=True,
    )


def _tear_primary_phase(journal: Path) -> None:
    (journal / "phase").write_text("comm", encoding="utf-8")


def _simulate_power_loss(root: Path, unit_dir: Path, state: Path) -> None:
    (state / "old-enabled").write_bytes((state / "durable-old-enabled").read_bytes())
    durable_template = state / "durable-template"
    volatile_template = unit_dir / LEGACY_TEMPLATE
    if durable_template.exists():
        volatile_template.write_bytes(durable_template.read_bytes())
    else:
        volatile_template.unlink(missing_ok=True)
    (state / "old-active").write_text("inactive\n", encoding="utf-8")


def _run_untrusted_test_root(test_root: Path) -> subprocess.CompletedProcess[str]:
    env = os.environ.copy()
    env["RQUANT_MIGRATION_TEST_POWER_LOSS_AFTER_DISABLE"] = "1"
    return subprocess.run(
        [
            "/bin/bash",
            str(SCRIPT),
            "--test-root",
            str(test_root),
            "--replacement",
            f"{LEGACY_TEMPLATE}={REPLACEMENT}",
            "--accept",
        ],
        cwd=ROOT,
        env=env,
        check=False,
        capture_output=True,
        text=True,
    )


def test_test_root_guard_precedes_all_derived_or_mutating_paths() -> None:
    script = SCRIPT.read_text(encoding="utf-8")

    euid_guard = script.index("EUID == 0")
    canonicalization = script.index('canonical_test_root=$(canonicalize_test_root "${TEST_ROOT}")')
    test_systemctl = script.index('SYSTEMCTL="${TEST_ROOT}/usr/bin/systemctl"')
    first_mkdir = script.index('"${MKDIR}" -p -- "${JOURNAL_ROOT}"')
    assert euid_guard < canonicalization < test_systemctl < first_mkdir
    assert ".rquant-migration-test-capability-v1" in script
    assert "TEST_MODE_ACTIVE" in script


@pytest.mark.parametrize("root_text", ("/", "//", "/tmp/.."))
def test_test_root_resolving_to_filesystem_root_fails_before_side_effects(
    root_text: str,
) -> None:
    result = _run_untrusted_test_root(Path(root_text))

    assert result.returncode == 2
    assert "unsafe --test-root" in result.stderr
    assert "systemctl" not in result.stdout


def test_symlink_to_filesystem_root_fails_before_side_effects(tmp_path: Path) -> None:
    alias = tmp_path / "root-alias"
    alias.symlink_to("/", target_is_directory=True)

    result = _run_untrusted_test_root(alias)

    assert result.returncode == 2
    assert "unsafe --test-root" in result.stderr
    assert "systemctl" not in result.stdout


@pytest.mark.parametrize(
    ("relative_path", "production_path"),
    (
        ("usr/bin/systemctl", "/usr/bin/systemctl"),
        ("usr/bin/sync", "/usr/bin/sync"),
        ("etc/systemd/system", "/etc/systemd/system"),
        ("state", "/state"),
    ),
)
def test_test_root_rejects_derived_paths_resolving_into_production(
    tmp_path: Path,
    relative_path: str,
    production_path: str,
) -> None:
    root, _unit_dir, state = _fixture(tmp_path)
    candidate = root / relative_path
    observed_state = state
    if candidate.is_dir():
        fixture_path = candidate.with_name(f"{candidate.name}-fixture")
        candidate.rename(fixture_path)
        if candidate == state:
            observed_state = fixture_path
    else:
        candidate.unlink()
    candidate.symlink_to(production_path, target_is_directory=relative_path.startswith("etc/"))

    result = _run_untrusted_test_root(root)

    assert result.returncode == 2
    assert "escapes canonical test root" in result.stderr
    assert not (observed_state / "calls").exists()
    assert not (observed_state / "rquant-workload-migration").exists()


def test_test_root_requires_nonproduction_capability_before_systemctl(
    tmp_path: Path,
) -> None:
    root, _unit_dir, state = _fixture(tmp_path)
    (root / ".rquant-migration-test-capability-v1").unlink()

    result = _run_untrusted_test_root(root)

    assert result.returncode == 2
    assert "test capability" in result.stderr
    assert not (state / "calls").exists()
    assert not (state / "rquant-workload-migration").exists()


def test_preview_reports_without_changing_files_or_state(tmp_path: Path) -> None:
    root, unit_dir, state = _fixture(tmp_path)

    result = _run(root, accept=False)

    assert result.returncode == 3
    assert "preview: no service state was changed" in result.stdout
    assert (unit_dir / LEGACY_TEMPLATE).read_text(encoding="utf-8") == UNIT_CONTENT
    assert (state / "old-active").read_text(encoding="utf-8") == "active\n"
    assert (state / "old-enabled").read_text(encoding="utf-8") == "enabled\n"
    assert "disable" not in (state / "calls").read_text(encoding="utf-8")


def test_accept_rejects_a_template_replacement_before_mutation(tmp_path: Path) -> None:
    root, unit_dir, state = _fixture(tmp_path)

    result = _run(
        root,
        accept=True,
        replacement="rquant-runtime-feature@.service",
    )

    assert result.returncode == 4
    assert "concrete replacement instance" in result.stderr
    assert (unit_dir / LEGACY_TEMPLATE).read_text(encoding="utf-8") == UNIT_CONTENT
    assert "disable" not in (state / "calls").read_text(encoding="utf-8")


@pytest.mark.parametrize(
    "replacement",
    (
        "rquant-runtime-live@svc-replacement.service",
        "rquant-runtime-research@svc-replacement.service",
        LEGACY_INSTANCE,
    ),
)
def test_accept_rejects_every_legacy_derived_or_loaded_legacy_replacement(
    tmp_path: Path,
    replacement: str,
) -> None:
    root, unit_dir, state = _fixture(tmp_path)

    result = _run(root, accept=True, replacement=replacement)

    assert result.returncode == 4
    assert "legacy" in result.stderr
    assert (unit_dir / LEGACY_TEMPLATE).read_text(encoding="utf-8") == UNIT_CONTENT
    assert "disable" not in (state / "calls").read_text(encoding="utf-8")


@pytest.mark.parametrize("active_state", ("failed", "activating", "deactivating", "reloading"))
def test_accept_rejects_legacy_states_that_cannot_be_rolled_back_exactly(
    tmp_path: Path,
    active_state: str,
) -> None:
    root, unit_dir, state = _fixture(tmp_path)
    (state / "old-active").write_text(f"{active_state}\n", encoding="utf-8")

    result = _run(root, accept=True)

    assert result.returncode == 4
    assert "recoverable state" in result.stderr
    assert (unit_dir / LEGACY_TEMPLATE).read_text(encoding="utf-8") == UNIT_CONTENT
    assert "disable" not in (state / "calls").read_text(encoding="utf-8")


def test_accept_blocks_research_replacement_while_maintenance_is_uncalibrated(
    tmp_path: Path,
) -> None:
    root, unit_dir, state = _fixture(tmp_path)

    result = _run(
        root,
        accept=True,
        replacement_slice="rquant-research.slice",
    )

    assert result.returncode == 4
    assert "pending calibration" in result.stderr
    assert (unit_dir / LEGACY_TEMPLATE).read_text(encoding="utf-8") == UNIT_CONTENT
    assert "disable" not in (state / "calls").read_text(encoding="utf-8")


@pytest.mark.parametrize("fault", ("replacement_show", "snapshot_show"))
def test_accept_preflight_fault_never_starts_mutation(
    tmp_path: Path,
    fault: str,
) -> None:
    root, unit_dir, state = _fixture(tmp_path)

    result = _run(root, accept=True, fault=fault)

    assert result.returncode != 0
    assert (unit_dir / LEGACY_TEMPLATE).read_text(encoding="utf-8") == UNIT_CONTENT
    assert (state / "old-active").read_text(encoding="utf-8") == "active\n"
    assert (state / "old-enabled").read_text(encoding="utf-8") == "enabled\n"
    assert "disable" not in (state / "calls").read_text(encoding="utf-8")


def test_accept_removes_legacy_only_after_verified_replacement(tmp_path: Path) -> None:
    root, unit_dir, state = _fixture(tmp_path)

    result = _run(root, accept=True)

    assert result.returncode == 0, result.stderr
    assert not (unit_dir / LEGACY_TEMPLATE).exists()
    assert (state / "old-active").read_text(encoding="utf-8") == "inactive\n"
    assert (state / "old-enabled").read_text(encoding="utf-8") == "disabled\n"
    calls = (state / "calls").read_text(encoding="utf-8")
    assert calls.index(f"show {REPLACEMENT}") < calls.index("disable --now")


@pytest.mark.parametrize(
    "fault",
    ("disable", "remove", "daemon_reload", "post_verify", "replacement_post_state"),
)
def test_accept_fault_rolls_back_files_enabled_and_active_state(
    tmp_path: Path,
    fault: str,
) -> None:
    root, unit_dir, state = _fixture(tmp_path)

    result = _run(root, accept=True, fault=fault)

    assert result.returncode != 0
    assert "rollback restored legacy files and unit state" in result.stderr
    assert (unit_dir / LEGACY_TEMPLATE).read_text(encoding="utf-8") == UNIT_CONTENT
    assert (state / "old-active").read_text(encoding="utf-8") == "active\n"
    assert (state / "old-enabled").read_text(encoding="utf-8") == "enabled\n"
    calls = (state / "calls").read_text(encoding="utf-8")
    assert "daemon-reload" in calls
    assert f"enable {LEGACY_INSTANCE}" in calls
    assert f"start {LEGACY_INSTANCE}" in calls


@pytest.mark.parametrize(
    ("signal_name", "returncode"),
    (("TERM", 143), ("INT", 130), ("HUP", 129)),
)
def test_catchable_signal_uses_the_same_transaction_rollback(
    tmp_path: Path,
    signal_name: str,
    returncode: int,
) -> None:
    root, unit_dir, state = _fixture(tmp_path)

    result = _run(
        root,
        accept=True,
        signal_after_disable=signal_name,
    )

    assert result.returncode == returncode
    assert "rollback restored legacy files and unit state" in result.stderr
    assert (unit_dir / LEGACY_TEMPLATE).read_text(encoding="utf-8") == UNIT_CONTENT
    assert (state / "old-active").read_text(encoding="utf-8") == "active\n"
    assert (state / "old-enabled").read_text(encoding="utf-8") == "enabled\n"
    assert not (state / "rquant-workload-migration/active").exists()


def test_power_loss_journal_recovers_before_reentrant_accept(tmp_path: Path) -> None:
    root, unit_dir, state = _fixture(tmp_path)

    interrupted = _run(root, accept=True, power_loss_after_disable=True)

    assert interrupted.returncode == -9
    journal = state / "rquant-workload-migration/active"
    assert journal.is_dir()
    assert (journal / "legacy-states.tsv").is_file()
    assert (state / "old-active").read_text(encoding="utf-8") == "inactive\n"

    recovered = _run(root, accept=True)

    assert recovered.returncode == 0, recovered.stdout + recovered.stderr
    assert "recovered interrupted legacy migration transaction" in recovered.stdout
    assert not journal.exists()
    assert not (unit_dir / LEGACY_TEMPLATE).exists()
    calls = (state / "calls").read_text(encoding="utf-8")
    assert calls.index(f"enable {LEGACY_INSTANCE}") < calls.rindex("disable --now")


def test_power_loss_after_commit_never_restores_removed_legacy(tmp_path: Path) -> None:
    root, unit_dir, state = _fixture(tmp_path)

    interrupted = _run(root, accept=True, power_loss_after_commit=True)

    assert interrupted.returncode == -9
    journal = state / "rquant-workload-migration/active"
    assert (journal / "phase").read_text(encoding="utf-8") == "committed\n"
    assert not (unit_dir / LEGACY_TEMPLATE).exists()
    assert (state / "old-active").read_text(encoding="utf-8") == "inactive\n"
    assert (state / "durable-old-enabled").read_text(encoding="utf-8") == "disabled\n"
    assert not (state / "durable-template").exists()

    _simulate_power_loss(root, unit_dir, state)

    finalized = _run(root, accept=False)

    assert finalized.returncode == 3
    assert "finalized committed legacy migration journal" in finalized.stdout
    assert not journal.exists()
    assert not (unit_dir / LEGACY_TEMPLATE).exists()
    assert (state / "old-enabled").read_text(encoding="utf-8") == "disabled\n"


def test_migration_uses_fixed_lifetime_lock_and_atomic_journal_writes() -> None:
    script = SCRIPT.read_text(encoding="utf-8")

    assert "readonly FLOCK=/usr/bin/flock" in script
    assert "readonly FILESYSTEM_SYNC=/usr/bin/sync" in script
    assert 'readonly MIGRATION_LOCK="${JOURNAL_ROOT}/migration.lock"' in script
    assert "atomic_write_file()" in script
    assert "fsync_file()" in script
    assert "fsync_directory()" in script
    assert '>"${TRANSACTION_DIR}/phase"' not in script
    assert '>"${STAGING_DIR}/phase"' not in script
    assert "filesystem_durability_barrier rollback || return 1" in script
    rollback_barrier = script.index("filesystem_durability_barrier rollback")
    rollback_journal_delete = script.index('remove_journal "${journal}"', rollback_barrier)
    assert rollback_barrier < rollback_journal_delete
    accept_barrier = script.rindex("filesystem_durability_barrier accept")
    committed_phase = script.rindex('write_transaction_phase "${TRANSACTION_DIR}" committed')
    assert accept_barrier < committed_phase


def test_filesystem_barrier_failure_rolls_back_and_flushes_before_journal_delete(
    tmp_path: Path,
) -> None:
    root, unit_dir, state = _fixture(tmp_path)

    result = _run(root, accept=True, fault="filesystem_sync")

    assert result.returncode != 0
    assert "filesystem durability barrier failed for accept" in result.stderr
    assert not (state / "rquant-workload-migration/active").exists()
    assert (unit_dir / LEGACY_TEMPLATE).read_text(encoding="utf-8") == UNIT_CONTENT
    assert (state / "durable-old-enabled").read_text(encoding="utf-8") == "enabled\n"
    assert (state / "durable-template").read_text(encoding="utf-8") == UNIT_CONTENT
    assert (state / "sync-count").read_text(encoding="utf-8") == "2\n"


def test_rollback_barrier_failure_retains_recovery_journal(tmp_path: Path) -> None:
    root, unit_dir, state = _fixture(tmp_path)

    result = _run(root, accept=True, fault="filesystem_sync_always")

    assert result.returncode == 5
    assert result.stderr.count("filesystem durability barrier failed") == 2
    assert (state / "rquant-workload-migration/active").is_dir()
    assert (unit_dir / LEGACY_TEMPLATE).read_text(encoding="utf-8") == UNIT_CONTENT
    assert (state / "old-enabled").read_text(encoding="utf-8") == "enabled\n"


def test_sigkill_before_rollback_barrier_keeps_journal_until_enablement_is_durable(
    tmp_path: Path,
) -> None:
    root, unit_dir, state = _fixture(tmp_path)

    interrupted = _run(
        root,
        accept=True,
        fail_after_unit_sync=True,
        kill_on_sync_call=2,
    )

    assert interrupted.returncode == -9
    journal = state / "rquant-workload-migration/active"
    assert journal.is_dir()
    assert (state / "old-enabled").read_text(encoding="utf-8") == "enabled\n"
    assert (unit_dir / LEGACY_TEMPLATE).exists()
    assert (state / "durable-old-enabled").read_text(encoding="utf-8") == "disabled\n"
    assert not (state / "durable-template").exists()

    _simulate_power_loss(root, unit_dir, state)
    assert (state / "old-enabled").read_text(encoding="utf-8") == "disabled\n"
    assert not (unit_dir / LEGACY_TEMPLATE).exists()
    assert journal.is_dir()

    recovered = _run(root, accept=False)

    assert recovered.returncode == 3, recovered.stdout + recovered.stderr
    assert "recovered interrupted legacy migration transaction" in recovered.stdout
    assert not journal.exists()
    assert (unit_dir / LEGACY_TEMPLATE).read_text(encoding="utf-8") == UNIT_CONTENT
    assert (state / "old-enabled").read_text(encoding="utf-8") == "enabled\n"
    assert (state / "durable-old-enabled").read_text(encoding="utf-8") == "enabled\n"
    assert (state / "durable-template").read_text(encoding="utf-8") == UNIT_CONTENT


def test_concurrent_accept_returns_busy_without_a_second_mutation(
    tmp_path: Path,
) -> None:
    root, unit_dir, state = _fixture(tmp_path)
    command = [
        "/bin/bash",
        str(SCRIPT),
        "--test-root",
        str(root),
        "--replacement",
        f"{LEGACY_TEMPLATE}={REPLACEMENT}",
        "--accept",
    ]
    env = os.environ.copy()
    env["RQUANT_MIGRATION_TEST_FLOCK_HOLD_SECONDS"] = "1.5"
    first = subprocess.Popen(
        command,
        cwd=ROOT,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    marker = state / "migration-lock-held"
    deadline = time.monotonic() + 5
    while not marker.exists() and time.monotonic() < deadline:
        time.sleep(0.02)
    assert marker.exists(), "first migration never acquired its lifetime lock"

    second = _run(root, accept=True)
    first_stdout, first_stderr = first.communicate(timeout=10)

    assert second.returncode == 6
    assert "migration transaction is busy" in second.stderr
    assert first.returncode == 0, first_stdout + first_stderr
    assert not (unit_dir / LEGACY_TEMPLATE).exists()
    calls = (state / "calls").read_text(encoding="utf-8")
    assert calls.count("disable --now") == 1


def test_torn_prepared_phase_recovers_from_last_good_and_reenters(
    tmp_path: Path,
) -> None:
    root, unit_dir, state = _fixture(tmp_path)

    interrupted = _run(root, accept=True, power_loss_after_prepared=True)

    assert interrupted.returncode == -9
    journal = state / "rquant-workload-migration/active"
    assert (journal / "phase.last-good").read_text(encoding="utf-8") == "prepared\n"
    _tear_primary_phase(journal)

    recovered = _run(root, accept=True)

    assert recovered.returncode == 0, recovered.stdout + recovered.stderr
    assert "recovered interrupted legacy migration transaction" in recovered.stdout
    assert not journal.exists()
    assert not (unit_dir / LEGACY_TEMPLATE).exists()


def test_torn_mutating_phase_fails_safe_to_rollback_before_reentry(
    tmp_path: Path,
) -> None:
    root, unit_dir, state = _fixture(tmp_path)

    interrupted = _run(root, accept=True, power_loss_after_disable=True)

    assert interrupted.returncode == -9
    journal = state / "rquant-workload-migration/active"
    assert (journal / "phase.last-good").read_text(encoding="utf-8") == "prepared\n"
    _tear_primary_phase(journal)

    recovered = _run(root, accept=True)

    assert recovered.returncode == 0, recovered.stdout + recovered.stderr
    assert "recovered interrupted legacy migration transaction" in recovered.stdout
    assert not journal.exists()
    assert not (unit_dir / LEGACY_TEMPLATE).exists()


def test_torn_committed_phase_fails_safe_to_rollback_instead_of_getting_stuck(
    tmp_path: Path,
) -> None:
    root, unit_dir, state = _fixture(tmp_path)

    interrupted = _run(root, accept=True, power_loss_after_commit=True)

    assert interrupted.returncode == -9
    journal = state / "rquant-workload-migration/active"
    assert (journal / "phase.last-good").read_text(encoding="utf-8") == "mutating\n"
    _tear_primary_phase(journal)

    recovered = _run(root, accept=False)

    assert recovered.returncode == 3, recovered.stdout + recovered.stderr
    assert "recovered interrupted legacy migration transaction" in recovered.stdout
    assert not journal.exists()
    assert (unit_dir / LEGACY_TEMPLATE).read_text(encoding="utf-8") == UNIT_CONTENT
    assert (state / "old-active").read_text(encoding="utf-8") == "active\n"
    assert (state / "old-enabled").read_text(encoding="utf-8") == "enabled\n"


def test_both_torn_phase_copies_still_use_fail_safe_rollback(
    tmp_path: Path,
) -> None:
    root, unit_dir, state = _fixture(tmp_path)

    interrupted = _run(root, accept=True, power_loss_after_disable=True)

    assert interrupted.returncode == -9
    journal = state / "rquant-workload-migration/active"
    _tear_primary_phase(journal)
    (journal / "phase.last-good").write_text("muta", encoding="utf-8")

    recovered = _run(root, accept=True)

    assert recovered.returncode == 0, recovered.stdout + recovered.stderr
    assert "migration phase is torn; applying fail-safe rollback" in recovered.stderr
    assert "recovered interrupted legacy migration transaction" in recovered.stdout
    assert not journal.exists()
    assert not (unit_dir / LEGACY_TEMPLATE).exists()
