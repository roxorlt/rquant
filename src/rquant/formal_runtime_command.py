"""Typed composition and inspection for the formal runtime service boundary."""

from __future__ import annotations

import argparse
import shlex
from collections.abc import Sequence
from pathlib import Path
from typing import Literal

from pydantic import Field, field_validator

from rquant.runtime_authority import PRODUCTION_ROLE_POLICY
from rquant.runtime_contracts import RuntimeContractModel

FORMAL_RUNTIME_WRAPPER_CONTRACT = "rquant-formal-runtime-wrapper/v1"

_WORKLOAD_ARBITER = "/usr/local/libexec/rquant-workload-arbiter"
#: Amended per Codex round-3 verdict 2026-08-28, item RQ-WI-R2-P1-02. The unit named
#: `/home/lighthouse/rquant/.venv/bin/rquant` for its dry-run and
#: `/home/lighthouse/rquant/.venv/bin/python .../run-lab-daemon.py formal` for the daemon.
#: Both are lighthouse-writable and `.venv` is an editable install pointing back at
#: `<checkout>/src`, so the deployment was validated by the tree under validation. The
#: unit now names the same fixed root-owned wrapper every other protected unit names, and
#: the two retired executables are refused outright rather than merely unexpected.
_SYSTEM_PYTHON = "/usr/bin/python3.11"
_RUNTIME_PYZ = "/usr/local/libexec/rquant-runtime-exec.pyz"
FINALIZER_ROLE = "lab_claim_finalizer"
_RETIRED_CHECKOUT_EXECUTABLES = (
    "/home/lighthouse/rquant/.venv/",
    "/home/lighthouse/rquant/scripts/run-lab-daemon.py",
)
_RUNTIME_CODE_CONFIG = Path("/etc/rquant/runtime-code-bootstrap.json")
#: The migration request the retired `ExecStartPre` dry-run named on the unit line. The
#: gate now runs inside the verified generation (`rquant.lab_formal_runtime_entry`), so
#: the path has to be a frozen constant: a protected unit contributes a role literal and
#: nothing else, and may no longer name a file.
RUNTIME_CODE_MIGRATION_REQUEST_PATH = Path("/etc/rquant/runtime-code-migration.json")
_RUNTIME_CODE_TRUSTED_BASE = Path("/etc/rquant")
_DEPLOYMENT_LOCK_PATH = Path("/run/rquant-lab-claim-finalizer/deployment.lock")
_ENVIRONMENT_FILE = Path("/etc/rquant/lab-claim-finalizer.env")
_REQUIRED_ENVIRONMENT = {
    "APP_ENV": "prod",
    "PYTHONDONTWRITEBYTECODE": "1",
    "RQUANT_DISABLE_DOTENV": "1",
}
_LEGACY_ARGUMENTS = frozenset(
    {
        "--checkout-root",
        "--expected-checkout-root",
        "--expected-code-root",
        "--release-managed-checkout",
        "--trusted-git-path",
    }
)
_BOOTSTRAP_OPTIONS = (
    "--runtime-code-config",
    "--runtime-code-trusted-base",
    "--runtime-code-authority-uid",
    "--runtime-code-authority-gid",
    "--deployment-lock-path",
)
#: The immutable bootstrap binding the finalizer unit used to spell out after
#: `run-lab-daemon.py formal`. It is a constant here rather than eight entries of the role's
#: `module_arguments` because this module ships inside the generation the wrapper verifies
#: byte for byte against a root-owned full manifest before executing any of it: a literal
#: here and a literal in `/etc/rquant/production-runtime-profile.json` are protected by the
#: same root authority, and the profile schema bounds a role at eight arguments.
FINALIZER_BOOTSTRAP_ARGUMENTS: tuple[str, ...] = (
    "--runtime-code-config",
    str(_RUNTIME_CODE_CONFIG),
    "--runtime-code-trusted-base",
    str(_RUNTIME_CODE_TRUSTED_BASE),
    "--runtime-code-authority-uid",
    "0",
    "--runtime-code-authority-gid",
    "0",
    "--deployment-lock-path",
    str(_DEPLOYMENT_LOCK_PATH),
)
_FORMAL_COMMANDS = (
    "lab-claim-finalizer",
    "lab-finalizer",
    "lab-runtime-prepare",
    "lab-scheduler",
    "lab-worker",
)
_COMMAND_FLAG_ARGUMENTS: dict[str, frozenset[str]] = {
    "lab-claim-finalizer": frozenset({"--once"}),
    "lab-finalizer": frozenset({"--once"}),
    "lab-runtime-prepare": frozenset(),
    "lab-scheduler": frozenset({"--once", "--remediate-full-integrity"}),
    "lab-worker": frozenset({"--once"}),
}


class FormalRuntimeCommandError(RuntimeError):
    """A formal runtime command artifact does not match the closed contract."""


def _canonical_absolute(value: Path) -> Path:
    if not value.is_absolute() or Path(value.absolute()) != value:
        raise ValueError("formal runtime command paths must be canonical absolute")
    return value


class FormalRuntimeBootstrapBinding(RuntimeContractModel):
    configuration_path: Path
    trusted_base: Path
    authority_uid: int = Field(strict=True, ge=0)
    authority_gid: int = Field(strict=True, ge=0)

    @field_validator("configuration_path", "trusted_base", mode="after")
    @classmethod
    def validate_paths(cls, value: Path) -> Path:
        return _canonical_absolute(value)


class FormalRuntimeWrapperBinding(RuntimeContractModel):
    schema_version: Literal[1] = 1
    contract: Literal["rquant-formal-runtime-wrapper-binding/v1"] = (
        "rquant-formal-runtime-wrapper-binding/v1"
    )
    bootstrap: FormalRuntimeBootstrapBinding
    deployment_lock_path: Path
    command: Literal[
        "lab-claim-finalizer",
        "lab-finalizer",
        "lab-runtime-prepare",
        "lab-scheduler",
        "lab-worker",
    ]
    command_arguments: tuple[str, ...] = ()

    @field_validator("deployment_lock_path", mode="after")
    @classmethod
    def validate_lock_path(cls, value: Path) -> Path:
        return _canonical_absolute(value)


class InspectedFormalSystemdService(RuntimeContractModel):
    schema_version: Literal[1] = 1
    contract: Literal["rquant-formal-systemd-service/v1"] = "rquant-formal-systemd-service/v1"
    unit_path: Path
    #: The one literal the unit contributes. Everything else below is derived from the
    #: root-owned role policy, never from the unit line.
    role: Literal["lab_claim_finalizer"]
    wrapper: FormalRuntimeWrapperBinding
    environment_file: Path
    environment: dict[str, str]

    @field_validator("unit_path", "environment_file", mode="after")
    @classmethod
    def validate_paths(cls, value: Path) -> Path:
        return _canonical_absolute(value)


def add_formal_runtime_bootstrap_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--runtime-code-config", type=Path, required=True)
    parser.add_argument("--runtime-code-trusted-base", type=Path, required=True)
    parser.add_argument("--runtime-code-authority-uid", type=int, required=True)
    parser.add_argument("--runtime-code-authority-gid", type=int, required=True)


def add_formal_runtime_deployment_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--deployment-generation", required=True)
    parser.add_argument("--deployment-lock-path", required=True)
    parser.add_argument("--deployment-generation-fd", required=True, type=int)
    parser.add_argument("--startup-deadline-monotonic", required=True, type=float)
    parser.add_argument("--deployment-operation-id")
    parser.add_argument("--deployment-environment-generation")


def _parse_nonnegative_integer(raw: str, *, label: str) -> int:
    try:
        parsed = int(raw)
    except ValueError as exc:
        raise FormalRuntimeCommandError(f"{label} is invalid") from exc
    if parsed < 0 or str(parsed) != raw:
        raise FormalRuntimeCommandError(f"{label} is invalid")
    return parsed


def _parse_command_tail(tokens: Sequence[str]) -> tuple[str, tuple[str, ...]]:
    if not tokens or tokens[0] not in _FORMAL_COMMANDS:
        raise FormalRuntimeCommandError("formal runtime command is missing or unknown")
    command = tokens[0]
    arguments = tuple(tokens[1:])
    allowed = _COMMAND_FLAG_ARGUMENTS[command]
    if any(argument not in allowed for argument in arguments):
        raise FormalRuntimeCommandError("formal runtime command contains an unknown argument")
    if len(set(arguments)) != len(arguments):
        raise FormalRuntimeCommandError("formal runtime command contains a duplicate argument")
    return command, arguments


def parse_formal_wrapper_argv(argv: Sequence[str]) -> FormalRuntimeWrapperBinding:
    tokens = tuple(argv)
    if tokens.count("--") != 1:
        raise FormalRuntimeCommandError("formal runtime wrapper command separator is invalid")
    separator = tokens.index("--")
    option_tokens = tokens[:separator]
    if len(option_tokens) != len(_BOOTSTRAP_OPTIONS) * 2:
        raise FormalRuntimeCommandError("required immutable binding is missing or duplicated")
    values: dict[str, str] = {}
    for index in range(0, len(option_tokens), 2):
        option = option_tokens[index]
        if option not in _BOOTSTRAP_OPTIONS:
            if option in _LEGACY_ARGUMENTS:
                raise FormalRuntimeCommandError(
                    "legacy runtime Git or checkout argument is forbidden"
                )
            raise FormalRuntimeCommandError("formal runtime wrapper contains an unknown argument")
        if option in values:
            raise FormalRuntimeCommandError("required immutable binding is duplicated")
        values[option] = option_tokens[index + 1]
    missing = tuple(option for option in _BOOTSTRAP_OPTIONS if option not in values)
    if missing:
        raise FormalRuntimeCommandError("required immutable binding is missing")
    command, command_arguments = _parse_command_tail(tokens[separator + 1 :])
    try:
        return FormalRuntimeWrapperBinding(
            bootstrap=FormalRuntimeBootstrapBinding(
                configuration_path=Path(values["--runtime-code-config"]),
                trusted_base=Path(values["--runtime-code-trusted-base"]),
                authority_uid=_parse_nonnegative_integer(
                    values["--runtime-code-authority-uid"],
                    label="formal runtime authority uid",
                ),
                authority_gid=_parse_nonnegative_integer(
                    values["--runtime-code-authority-gid"],
                    label="formal runtime authority gid",
                ),
            ),
            deployment_lock_path=Path(values["--deployment-lock-path"]),
            command=command,
            command_arguments=command_arguments,
        )
    except ValueError as exc:
        raise FormalRuntimeCommandError("formal runtime wrapper binding is invalid") from exc


def compose_formal_daemon_argv(
    binding: FormalRuntimeWrapperBinding,
    *,
    deployment_generation: str,
    deployment_generation_fd: int,
    startup_deadline_monotonic: float,
) -> tuple[str, ...]:
    if deployment_generation_fd < 0:
        raise FormalRuntimeCommandError("formal runtime deployment descriptor is invalid")
    if (
        not deployment_generation
        or any(character not in "0123456789abcdef" for character in deployment_generation)
        or len(deployment_generation) not in {40, 64}
    ):
        raise FormalRuntimeCommandError("formal runtime deployment generation is invalid")
    return (
        binding.command,
        "--runtime-code-config",
        str(binding.bootstrap.configuration_path),
        "--runtime-code-trusted-base",
        str(binding.bootstrap.trusted_base),
        "--runtime-code-authority-uid",
        str(binding.bootstrap.authority_uid),
        "--runtime-code-authority-gid",
        str(binding.bootstrap.authority_gid),
        "--deployment-generation",
        deployment_generation,
        "--deployment-lock-path",
        str(binding.deployment_lock_path),
        "--deployment-generation-fd",
        str(deployment_generation_fd),
        "--startup-deadline-monotonic",
        str(startup_deadline_monotonic),
        *binding.command_arguments,
    )


def _unit_values(source: str, key: str) -> tuple[str, ...]:
    prefix = f"{key}="
    return tuple(
        line.strip()[len(prefix) :]
        for line in source.splitlines()
        if line.strip().startswith(prefix)
    )


def compose_formal_wrapper_argv(module_arguments: Sequence[str]) -> tuple[str, ...]:
    """The full wrapper argv: the frozen bootstrap binding plus the role's chosen entry.

    The role policy names the daemon command (and any flag of it the deployment wants); the
    binding in front of the separator is fixed. `parse_formal_wrapper_argv` then validates
    the whole thing exactly as it validated the unit's own tail before, so a profile that
    froze the wrong entry fails at the parser rather than starting a different daemon.
    """

    return (*FINALIZER_BOOTSTRAP_ARGUMENTS, "--", *module_arguments)


def _role_module_arguments(role: str) -> tuple[str, ...]:
    """The frozen argv the root-owned profile hands the role, and the only source for it.

    The unit used to carry these literals itself, which is exactly what made the finalizer a
    self-certifying checkout entry point. They now live in `PRODUCTION_ROLE_POLICY`, are
    published into `/etc/rquant/production-runtime-profile.json`, and are handed to the
    module by the wrapper. Reading them back from the policy here — rather than restating
    them — is what keeps this static inspection and the running service describing one thing.
    """

    matches = tuple(entry for entry in PRODUCTION_ROLE_POLICY if entry.name == role)
    if len(matches) != 1:
        raise FormalRuntimeCommandError("formal runtime role is not declared by the policy")
    if matches[0].module != "rquant.lab_formal_runtime_entry":
        raise FormalRuntimeCommandError("formal runtime role names an unexpected module")
    return compose_formal_wrapper_argv(matches[0].module_arguments)


def inspect_formal_systemd_service(*, unit_path: Path) -> InspectedFormalSystemdService:
    """Hold the finalizer unit to the shape a protected runtime unit is allowed to have.

    Amended per Codex round-3 verdict 2026-08-28, item RQ-WI-R2-P1-02. This used to require
    exactly one `ExecStartPre` running `rquant runtime-code dry-run` out of the checkout, and
    an `ExecStart` whose tail spelled out the whole wrapper binding. Both were checkout code
    executing ahead of the verification meant to authorise it, so both are now refused: the
    preflight count is zero, the start command is the fixed root-owned wrapper and a role
    literal, and the binding is read from the root-owned role policy instead of the unit.
    """

    try:
        source = unit_path.read_text(encoding="utf-8")
    except (OSError, UnicodeError) as exc:
        raise FormalRuntimeCommandError("formal runtime service artifact is unavailable") from exc
    if any(argument in source for argument in _LEGACY_ARGUMENTS):
        raise FormalRuntimeCommandError("legacy runtime Git or checkout argument is forbidden")
    if any(executable in source for executable in _RETIRED_CHECKOUT_EXECUTABLES):
        raise FormalRuntimeCommandError("formal runtime service names a checkout interpreter")
    if _unit_values(source, "ExecStartPre"):
        raise FormalRuntimeCommandError(
            "formal runtime service carries a start-phase preflight; the migration gate "
            "belongs inside the verified generation"
        )
    start_values = _unit_values(source, "ExecStart")
    environment_files = _unit_values(source, "EnvironmentFile")
    if len(start_values) != 1 or len(environment_files) != 1:
        raise FormalRuntimeCommandError("required immutable binding is missing from service")
    try:
        start_tokens = tuple(shlex.split(start_values[0], posix=True))
        environment_tokens = tuple(
            token
            for value in _unit_values(source, "Environment")
            for token in shlex.split(value, posix=True)
        )
    except ValueError as exc:
        raise FormalRuntimeCommandError("formal runtime service argv is invalid") from exc
    if start_tokens != (
        _WORKLOAD_ARBITER,
        "research",
        "--",
        _SYSTEM_PYTHON,
        "-I",
        "-S",
        _RUNTIME_PYZ,
        "--role",
        FINALIZER_ROLE,
    ):
        raise FormalRuntimeCommandError("formal runtime service executable binding is invalid")
    wrapper = parse_formal_wrapper_argv(_role_module_arguments(FINALIZER_ROLE))
    if wrapper != FormalRuntimeWrapperBinding(
        bootstrap=FormalRuntimeBootstrapBinding(
            configuration_path=_RUNTIME_CODE_CONFIG,
            trusted_base=_RUNTIME_CODE_TRUSTED_BASE,
            authority_uid=0,
            authority_gid=0,
        ),
        deployment_lock_path=_DEPLOYMENT_LOCK_PATH,
        command="lab-claim-finalizer",
    ):
        raise FormalRuntimeCommandError("formal runtime service binding is not canonical")
    environment: dict[str, str] = {}
    for token in environment_tokens:
        name, separator, value = token.partition("=")
        if not separator or not name or name in environment:
            raise FormalRuntimeCommandError("formal runtime service environment is invalid")
        environment[name] = value
    if environment != _REQUIRED_ENVIRONMENT:
        raise FormalRuntimeCommandError("required immutable environment binding is missing")
    if Path(environment_files[0]) != _ENVIRONMENT_FILE:
        raise FormalRuntimeCommandError("formal runtime service environment file is invalid")
    try:
        return InspectedFormalSystemdService(
            unit_path=unit_path,
            role=FINALIZER_ROLE,
            wrapper=wrapper,
            environment_file=_ENVIRONMENT_FILE,
            environment=environment,
        )
    except ValueError as exc:
        raise FormalRuntimeCommandError("formal runtime service artifact is invalid") from exc


__all__ = [
    "FINALIZER_BOOTSTRAP_ARGUMENTS",
    "FINALIZER_ROLE",
    "FORMAL_RUNTIME_WRAPPER_CONTRACT",
    "RUNTIME_CODE_MIGRATION_REQUEST_PATH",
    "FormalRuntimeBootstrapBinding",
    "FormalRuntimeCommandError",
    "FormalRuntimeWrapperBinding",
    "InspectedFormalSystemdService",
    "add_formal_runtime_bootstrap_arguments",
    "add_formal_runtime_deployment_arguments",
    "compose_formal_daemon_argv",
    "compose_formal_wrapper_argv",
    "inspect_formal_systemd_service",
    "parse_formal_wrapper_argv",
]
