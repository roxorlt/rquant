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


# ---------------------------------------------------------------------------------------
# Codex round-3 verdict 2026-08-28, RQ-WI-R2-P1-02: the generation-local finalizer entry
# ---------------------------------------------------------------------------------------

#: The argv the root-owned profile freezes for the `lab_claim_finalizer` role, which is the
#: same binding the unit used to spell out after `run-lab-daemon.py formal`. Held here as a
#: literal and tied back to `PRODUCTION_ROLE_POLICY` by
#: `test_the_finalizer_role_argv_is_the_binding_this_module_parses`, so the entry module and
#: the role policy cannot drift apart in silence.
FINALIZER_ARGV = (
    "--runtime-code-config",
    "/etc/rquant/runtime-code-bootstrap.json",
    "--runtime-code-trusted-base",
    "/etc/rquant",
    "--runtime-code-authority-uid",
    "0",
    "--runtime-code-authority-gid",
    "0",
    "--deployment-lock-path",
    "/run/rquant-lab-claim-finalizer/deployment.lock",
    "--",
    "lab-claim-finalizer",
)


def _finalizer_stage_recorder(
    monkeypatch: pytest.MonkeyPatch,
    *,
    raises_at: str | None = None,
) -> list[str]:
    """Replace every collaborator of the entry module with an order-recording stub."""

    import os
    from types import SimpleNamespace

    from rquant import lab_formal_runtime_entry as entry

    order: list[str] = []

    def stage(name: str, result: object):  # type: ignore[no-untyped-def]
        def recorded(*_args: object, **_kwargs: object) -> object:
            order.append(name)
            if raises_at == name:
                raise entry.RuntimeCodeOperationError(f"{name} refused")
            return result

        return recorded

    capability = SimpleNamespace(
        evidence=SimpleNamespace(provenance_commit="c" * 40),
        loaded=SimpleNamespace(
            attestation=SimpleNamespace(execution_spec=SimpleNamespace(python_abi="cp311"))
        ),
        close=lambda: order.append("capability-closed"),
    )

    def locked(*_args: object, **_kwargs: object) -> int:
        order.append("lock")
        if raises_at == "lock":
            raise entry.LabFormalRuntimeEntryError("lock refused")
        return os.open(os.devnull, os.O_RDONLY)

    monkeypatch.setattr(entry, "acquire_formal_deployment_lock", locked)
    monkeypatch.setattr(entry, "open_formal_runtime_capability", stage("capability", capability))
    monkeypatch.setattr(
        entry, "assert_migration_request_is_satisfiable", stage("migration-gate", ())
    )
    monkeypatch.setattr(entry, "compose_formal_daemon_argv", stage("compose", ("argv",)))
    monkeypatch.setattr(entry, "bind_formal_runtime", stage("bind", SimpleNamespace()))
    monkeypatch.setattr(entry, "exec_formal_runtime", stage("exec", None))
    return order


def test_the_formal_bootstrap_reads_the_root_owned_config_only_after_generation_verification(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The finalizer's own chain runs in one order, and it is not interchangeable.

    The outer chain — the root-owned wrapper verifying a whole generation against a
    root-owned full manifest — has already finished by the time this module's first
    instruction runs; that is what `--role lab_claim_finalizer` buys and what
    `tests/unit/test_runtime_exec_wrapper.py` holds. What is held here is the inner chain:
    the deployment lock is taken before any root-owned document is read, the capability
    (ed25519 attestation plus promotion receipt) is opened before the retired `ExecStartPre`
    migration gate runs, and nothing composes a daemon argv or execs anything until both
    have succeeded.
    """

    from rquant import lab_formal_runtime_entry as entry

    order = _finalizer_stage_recorder(monkeypatch)

    assert entry.main(list(FINALIZER_ARGV)) == 1
    assert order == ["lock", "capability", "migration-gate", "compose", "bind", "exec"]


@pytest.mark.parametrize(
    ("failing", "reached"),
    (
        ("lock", []),
        ("capability", ["lock"]),
        ("migration-gate", ["lock", "capability"]),
        ("compose", ["lock", "capability", "migration-gate"]),
        ("bind", ["lock", "capability", "migration-gate", "compose"]),
    ),
)
def test_a_refusal_at_any_stage_stops_every_later_stage(
    monkeypatch: pytest.MonkeyPatch,
    failing: str,
    reached: list[str],
) -> None:
    from rquant import lab_formal_runtime_entry as entry

    order = _finalizer_stage_recorder(monkeypatch, raises_at=failing)

    assert entry.main(list(FINALIZER_ARGV)) == 1
    assert [event for event in order if event != "capability-closed"] == [*reached, failing]
    assert "exec" not in order


def test_the_entry_refuses_an_argv_that_is_not_the_frozen_binding(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """A profile that froze the wrong literals must fail before anything else happens."""

    from rquant import lab_formal_runtime_entry as entry

    order = _finalizer_stage_recorder(monkeypatch)

    assert entry.main([]) == 1
    assert entry.main([*FINALIZER_ARGV, "--unknown-drift"]) == 1
    assert entry.main(["--trusted-git-path", "/usr/bin/git", "--", "lab-claim-finalizer"]) == 1
    assert order == []
    assert "Lab formal daemon wrapper failed" in capsys.readouterr().err


def _real_migration_gate(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    *,
    legacy_residue: bool,
) -> object:
    """Wire the entry module's gate to a real operator over a real migration request."""

    import os
    from types import SimpleNamespace

    from rquant import lab_formal_runtime_entry as entry
    from rquant.runtime_code_operations import (
        RuntimeCodeFormalService,
        RuntimeCodeGenerationOperator,
        RuntimeCodeMigrationRequest,
    )
    from tests.runtime_code_e2e_support import build_test_package

    package = build_test_package(tmp_path / "package")
    trusted_base = tmp_path / "trusted"
    trusted_base.mkdir(mode=0o700, parents=True, exist_ok=True)
    runtime_root = trusted_base / "runtime-code"
    runtime_root.mkdir(mode=0o700, exist_ok=True)
    operator = RuntimeCodeGenerationOperator(
        runtime_root=runtime_root,
        trusted_base=trusted_base,
        root_keyring=package.root_keyring,
        runtime_keyring=package.runtime_keyring,
        promotion_trust=package.promotion_trust,
        expected_uid=os.getuid(),
        expected_gid=os.getgid(),
        expected_audience="formal-lab",
        expected_installation_id="installation-a",
        expected_target_platform="test-platform",
    )
    legacy_paths: tuple[Path, ...] = ()
    if legacy_residue:
        legacy = tmp_path / "legacy-checkout-authority.json"
        legacy.write_text("{}", encoding="ascii")
        legacy_paths = (legacy,)
    request = RuntimeCodeMigrationRequest(
        install=package.request(),
        formal_services=(
            RuntimeCodeFormalService(
                command="lab-claim-finalizer",
                unit_path=UNIT,
                wrapper_path=WRAPPER,
            ),
        ),
        expected_configuration_path=Path("/etc/rquant/runtime-code-bootstrap.json"),
        expected_trusted_base=Path("/etc/rquant"),
        expected_authority_uid=0,
        expected_authority_gid=0,
        legacy_paths=legacy_paths,
    )
    seen: dict[str, object] = {}

    def loaded_configuration(path: Path, **kwargs: object) -> object:
        seen["configuration_path"] = path
        seen.update(kwargs)
        return SimpleNamespace(name="configuration")

    def loaded_request(path: Path, model: object, **kwargs: object) -> object:
        seen["request_path"] = path
        seen["request_model"] = model
        return request

    monkeypatch.setattr(entry, "load_runtime_code_bootstrap_configuration", loaded_configuration)
    monkeypatch.setattr(entry, "load_runtime_code_operation_request", loaded_request)
    monkeypatch.setattr(
        entry, "compose_runtime_code_generation_operator", lambda _configuration: operator
    )
    return SimpleNamespace(seen=seen, request=request)


def test_the_dry_run_gate_still_refuses_an_unsatisfiable_migration_request(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """S3(i): deleting the `ExecStartPre` line must not delete what it enforced.

    The unit used to run `rquant runtime-code dry-run --request
    /etc/rquant/runtime-code-migration.json` before the daemon started. That call proved
    nothing about the checkout it ran from, but it did stop the service when the migration
    request could not be satisfied. This runs the real operator over a real request and
    holds both halves: a satisfiable request returns the operator's own checks, and legacy
    checkout residue still refuses.
    """

    from rquant import lab_formal_runtime_entry as entry
    from rquant.formal_runtime_command import (
        RUNTIME_CODE_MIGRATION_REQUEST_PATH,
        parse_formal_wrapper_argv,
    )
    from rquant.runtime_code_operations import (
        RuntimeCodeMigrationRequest,
        RuntimeCodeOperationError,
    )

    binding = parse_formal_wrapper_argv(list(FINALIZER_ARGV))
    wired = _real_migration_gate(monkeypatch, tmp_path / "ok", legacy_residue=False)

    checks = entry.assert_migration_request_is_satisfiable(binding)

    assert "legacy-runtime-residue-absent" in checks
    assert "formal-service-artifacts-and-argv-verified" in checks
    assert wired.seen["request_path"] == RUNTIME_CODE_MIGRATION_REQUEST_PATH
    assert wired.seen["request_model"] is RuntimeCodeMigrationRequest
    assert wired.seen["configuration_path"] == Path("/etc/rquant/runtime-code-bootstrap.json")
    assert wired.seen["trusted_base"] == Path("/etc/rquant")
    assert wired.seen["expected_uid"] == 0
    assert wired.seen["expected_gid"] == 0

    _real_migration_gate(monkeypatch, tmp_path / "residue", legacy_residue=True)

    with pytest.raises(RuntimeCodeOperationError, match="legacy"):
        entry.assert_migration_request_is_satisfiable(binding)


def test_the_migration_gate_refuses_a_request_bound_to_other_authority(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """The CLI compared the request's `expected_*` against its own flags; so does this."""

    from rquant import lab_formal_runtime_entry as entry
    from rquant.formal_runtime_command import parse_formal_wrapper_argv
    from rquant.runtime_code_operations import RuntimeCodeOperationError

    binding = parse_formal_wrapper_argv(list(FINALIZER_ARGV))
    wired = _real_migration_gate(monkeypatch, tmp_path, legacy_residue=False)
    drifted = wired.request.model_copy(update={"expected_authority_uid": 1})
    monkeypatch.setattr(
        entry, "load_runtime_code_operation_request", lambda *_a, **_k: drifted
    )

    with pytest.raises(RuntimeCodeOperationError, match="authority identity"):
        entry.assert_migration_request_is_satisfiable(binding)
