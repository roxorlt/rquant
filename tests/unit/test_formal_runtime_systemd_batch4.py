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


def test_checked_in_finalizer_unit_composes_real_parser_argv_without_legacy() -> None:
    """The unit contributes a role literal; the daemon argv comes from the role policy.

    Codex round-3 verdict 2026-08-28, RQ-WI-R2-P1-02. This used to read a start-phase
    `ExecStartPre` out of the unit and parse it, and to reconstruct the wrapper binding from
    the unit's own `ExecStart` tail — both of which were checkout commands the unit was free
    to write. There is no preflight line left to parse and no tail left to read: the binding
    is the root-owned profile's, and what is checked here is that it still composes an argv
    the real CLI parser accepts.
    """

    inspected = inspect_formal_systemd_service(unit_path=UNIT)

    daemon_argv = compose_formal_daemon_argv(
        inspected.wrapper,
        deployment_generation="a" * 40,
        deployment_generation_fd=17,
        startup_deadline_monotonic=9_999_999_999.0,
    )
    daemon = build_parser().parse_args(list(daemon_argv))

    assert inspected.role == "lab_claim_finalizer"
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
    assert "ExecStartPre" not in unit_text
    assert "/home/lighthouse/rquant/.venv/" not in unit_text
    assert "run-lab-daemon.py" not in unit_text


@pytest.mark.parametrize(
    ("old", "new", "match"),
    (
        (
            "--role lab_claim_finalizer",
            "--role lab_claim_finalizer --trusted-git-path /usr/bin/git",
            "legacy",
        ),
        (
            "--role lab_claim_finalizer",
            "--role lab_claim_finalizer --instance svc-0",
            "executable binding",
        ),
        (
            "/usr/bin/python3.11 -I -S /usr/local/libexec/rquant-runtime-exec.pyz",
            "/home/lighthouse/rquant/.venv/bin/python -I",
            "checkout interpreter",
        ),
        (
            "ExecStart=/usr/local/libexec/rquant-workload-arbiter",
            (
                "ExecStartPre=/home/lighthouse/rquant/.venv/bin/rquant runtime-code dry-run\n"
                "ExecStart=/usr/local/libexec/rquant-workload-arbiter"
            ),
            "checkout interpreter",
        ),
        (
            "ExecStart=/usr/local/libexec/rquant-workload-arbiter",
            "ExecStartPre=/bin/true\nExecStart=/usr/local/libexec/rquant-workload-arbiter",
            "start-phase preflight",
        ),
        (
            "EnvironmentFile=/etc/rquant/lab-claim-finalizer.env",
            "",
            "required immutable binding",
        ),
        (
            "Environment=APP_ENV=prod",
            "Environment=APP_ENV=staging",
            "required immutable environment",
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
        inspect_formal_systemd_service(unit_path=unit)


def test_static_inspection_fails_closed_when_the_role_policy_argv_drifts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The typed composition check moved from the checkout wrapper to the role policy.

    It used to parse `scripts/run-lab-daemon.py` and demand that `_formal_main` still called
    the typed helpers — a check on a file any deployment could rewrite. The binding now comes
    from `PRODUCTION_ROLE_POLICY`, so that is what has to fail closed when it drifts.
    """

    from dataclasses import replace

    import rquant.formal_runtime_command as command_module
    from rquant.runtime_authority import PRODUCTION_ROLE_POLICY

    drifted = tuple(
        replace(entry, module_arguments=("lab-worker",))
        if entry.name == "lab_claim_finalizer"
        else entry
        for entry in PRODUCTION_ROLE_POLICY
    )
    monkeypatch.setattr(command_module, "PRODUCTION_ROLE_POLICY", drifted)

    with pytest.raises(FormalRuntimeCommandError, match="not canonical"):
        inspect_formal_systemd_service(unit_path=UNIT)


def test_static_inspection_refuses_a_role_the_policy_does_not_declare(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import rquant.formal_runtime_command as command_module
    from rquant.runtime_authority import PRODUCTION_ROLE_POLICY

    monkeypatch.setattr(
        command_module,
        "PRODUCTION_ROLE_POLICY",
        tuple(entry for entry in PRODUCTION_ROLE_POLICY if entry.name != "lab_claim_finalizer"),
    )

    with pytest.raises(FormalRuntimeCommandError, match="not declared by the policy"):
        inspect_formal_systemd_service(unit_path=UNIT)


# ---------------------------------------------------------------------------------------
# Codex round-3 verdict 2026-08-28, RQ-WI-R2-P1-02: the generation-local finalizer entry
# ---------------------------------------------------------------------------------------

#: The argv the root-owned profile freezes for the `lab_claim_finalizer` role: the daemon
#: entry, with the immutable bootstrap binding supplied by `compose_formal_wrapper_argv`.
#: Held here as a literal and tied back to `PRODUCTION_ROLE_POLICY` by
#: `test_the_finalizer_role_argv_is_the_binding_this_module_parses`, so the entry module and
#: the role policy cannot drift apart in silence.
FINALIZER_ARGV = ("lab-claim-finalizer",)


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
    assert entry.main(["--", "lab-claim-finalizer"]) == 1
    assert entry.main(["lab-worker"]) == 1
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
        compose_formal_wrapper_argv,
        parse_formal_wrapper_argv,
    )
    from rquant.runtime_code_operations import (
        RuntimeCodeMigrationRequest,
        RuntimeCodeOperationError,
    )

    binding = parse_formal_wrapper_argv(compose_formal_wrapper_argv(FINALIZER_ARGV))
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
    from rquant.formal_runtime_command import (
        compose_formal_wrapper_argv,
        parse_formal_wrapper_argv,
    )
    from rquant.runtime_code_operations import RuntimeCodeOperationError

    binding = parse_formal_wrapper_argv(compose_formal_wrapper_argv(FINALIZER_ARGV))
    wired = _real_migration_gate(monkeypatch, tmp_path, legacy_residue=False)
    drifted = wired.request.model_copy(update={"expected_authority_uid": 1})
    monkeypatch.setattr(
        entry, "load_runtime_code_operation_request", lambda *_a, **_k: drifted
    )

    with pytest.raises(RuntimeCodeOperationError, match="authority identity"):
        entry.assert_migration_request_is_satisfiable(binding)


def test_the_finalizer_role_argv_is_the_binding_this_module_parses() -> None:
    """One frozen argv, named by the policy and parsed by the entry, with no restatement."""

    from rquant.formal_runtime_command import (
        FINALIZER_BOOTSTRAP_ARGUMENTS,
        compose_formal_wrapper_argv,
        parse_formal_wrapper_argv,
    )
    from rquant.runtime_authority import PRODUCTION_ROLE_POLICY

    entry = next(
        item for item in PRODUCTION_ROLE_POLICY if item.name == "lab_claim_finalizer"
    )
    binding = parse_formal_wrapper_argv(compose_formal_wrapper_argv(entry.module_arguments))

    assert entry.module_arguments == FINALIZER_ARGV
    assert entry.module == "rquant.lab_formal_runtime_entry"
    assert binding.command == "lab-claim-finalizer"
    assert binding.command_arguments == ()
    assert binding.bootstrap.configuration_path == Path("/etc/rquant/runtime-code-bootstrap.json")
    assert binding.bootstrap.trusted_base == Path("/etc/rquant")
    assert binding.bootstrap.authority_uid == 0
    assert binding.bootstrap.authority_gid == 0
    assert binding.deployment_lock_path == Path(
        "/run/rquant-lab-claim-finalizer/deployment.lock"
    )
    # The bootstrap half is a generation constant, not a unit line and not eight entries of
    # the role's own arguments: the profile schema bounds a role at eight.
    assert len(entry.module_arguments) <= 8
    assert FINALIZER_BOOTSTRAP_ARGUMENTS[0] == "--runtime-code-config"


def test_the_finalizer_entry_module_loads_nothing_dynamically() -> None:
    """Independent review R3B-SPEC-02: the entry may only use the narrowed `sys.path`.

    The retired checkout script reached its work through three `importlib.import_module`
    calls that resolved via `rquant.pth` back into the checkout. This module exists so those
    become ordinary top-level imports, taken after the wrapper has already narrowed
    `sys.path` to the generation - which is only true while the module has no other way to
    name a file. A dynamic loader here would reopen exactly the door the round closed, and it
    would do so without changing any argv, unit or profile that the other cases inspect.

    Read as source rather than by importing it: the point is what the file may contain, and
    an import would only tell us what one execution happened to reach.
    """

    import ast

    source = ROOT / "src" / "rquant" / "lab_formal_runtime_entry.py"
    tree = ast.parse(source.read_text(encoding="utf-8"), filename=str(source))

    forbidden_names = {
        "__import__",
        "import_module",
        "spec_from_file_location",
        "spec_from_loader",
        "module_from_spec",
        "exec_module",
        "SourceFileLoader",
        "load_module",
        "exec",
        "eval",
    }
    called: set[str] = set()
    sys_path_uses: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            function = node.func
            if isinstance(function, ast.Name):
                called.add(function.id)
            elif isinstance(function, ast.Attribute):
                called.add(function.attr)
        if isinstance(node, ast.Attribute) and node.attr == "path":
            value = node.value
            if isinstance(value, ast.Name) and value.id == "sys":
                sys_path_uses.append(ast.dump(node))
        if isinstance(node, (ast.Import, ast.ImportFrom)):
            module = getattr(node, "module", None) or ""
            names = {alias.name for alias in node.names}
            assert not module.startswith("importlib"), ast.dump(node)
            assert "importlib" not in names, ast.dump(node)

    assert called & forbidden_names == set(), sorted(called & forbidden_names)
    # `sys.path` is not read, appended to, or assigned anywhere in the module: the wrapper
    # owns it, and this file has to live with whatever it was handed.
    assert sys_path_uses == []
