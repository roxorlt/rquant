"""Typed composition and inspection for the formal runtime service boundary."""

from __future__ import annotations

import argparse
import ast
import shlex
from collections.abc import Sequence
from pathlib import Path
from typing import Literal

from pydantic import Field, field_validator

from rquant.runtime_contracts import RuntimeContractModel

FORMAL_RUNTIME_WRAPPER_CONTRACT = "rquant-formal-runtime-wrapper/v1"

_RQUANT_EXECUTABLE = "/home/lighthouse/rquant/.venv/bin/rquant"
_PYTHON_EXECUTABLE = "/home/lighthouse/rquant/.venv/bin/python"
_WRAPPER_EXECUTABLE = "/home/lighthouse/rquant/scripts/run-lab-daemon.py"
_WORKLOAD_ARBITER = "/usr/local/libexec/rquant-workload-arbiter"
_RUNTIME_CODE_CONFIG = Path("/etc/rquant/runtime-code-bootstrap.json")
_RUNTIME_CODE_MIGRATION = Path("/etc/rquant/runtime-code-migration.json")
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
    wrapper_source_path: Path
    preflight_argv: tuple[str, ...] = Field(min_length=1)
    wrapper: FormalRuntimeWrapperBinding
    environment_file: Path
    environment: dict[str, str]

    @field_validator("unit_path", "wrapper_source_path", "environment_file", mode="after")
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


def _inspect_wrapper_source(path: Path) -> None:
    try:
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    except (OSError, SyntaxError, UnicodeError) as exc:
        raise FormalRuntimeCommandError("formal runtime wrapper source is invalid") from exc
    contract_matches = tuple(
        node
        for node in tree.body
        if isinstance(node, ast.Assign)
        and any(
            isinstance(target, ast.Name) and target.id == "FORMAL_RUNTIME_WRAPPER_CONTRACT"
            for target in node.targets
        )
        and isinstance(node.value, ast.Constant)
        and node.value.value == FORMAL_RUNTIME_WRAPPER_CONTRACT
    )
    formal_functions = tuple(
        node
        for node in tree.body
        if isinstance(node, ast.FunctionDef) and node.name == "_formal_main"
    )
    if len(contract_matches) != 1 or len(formal_functions) != 1:
        raise FormalRuntimeCommandError("formal runtime wrapper contract is missing")
    formal_function = formal_functions[0]
    names = {node.id for node in ast.walk(formal_function) if isinstance(node, ast.Name)}
    required = {
        "bind_formal_runtime",
        "compose_formal_daemon_argv",
        "exec_formal_runtime",
        "open_formal_runtime_capability",
        "parse_formal_wrapper_argv",
    }
    forbidden = {"_git_commit", "_require_trusted_git", "run_contained", "subprocess"}
    if not required.issubset(names) or names.intersection(forbidden):
        raise FormalRuntimeCommandError("formal runtime wrapper call graph is invalid")


def inspect_formal_systemd_service(
    *,
    unit_path: Path,
    wrapper_source_path: Path,
) -> InspectedFormalSystemdService:
    try:
        source = unit_path.read_text(encoding="utf-8")
    except (OSError, UnicodeError) as exc:
        raise FormalRuntimeCommandError("formal runtime service artifact is unavailable") from exc
    if any(argument in source for argument in _LEGACY_ARGUMENTS):
        raise FormalRuntimeCommandError("legacy runtime Git or checkout argument is forbidden")
    preflight_values = _unit_values(source, "ExecStartPre")
    start_values = _unit_values(source, "ExecStart")
    environment_files = _unit_values(source, "EnvironmentFile")
    if len(preflight_values) != 1 or len(start_values) != 1 or len(environment_files) != 1:
        raise FormalRuntimeCommandError("required immutable binding is missing from service")
    try:
        preflight_tokens = tuple(shlex.split(preflight_values[0], posix=True))
        start_tokens = tuple(shlex.split(start_values[0], posix=True))
        environment_tokens = tuple(
            token
            for value in _unit_values(source, "Environment")
            for token in shlex.split(value, posix=True)
        )
    except ValueError as exc:
        raise FormalRuntimeCommandError("formal runtime service argv is invalid") from exc
    expected_preflight = (
        _RQUANT_EXECUTABLE,
        "runtime-code",
        "dry-run",
        "--runtime-code-config",
        str(_RUNTIME_CODE_CONFIG),
        "--runtime-code-trusted-base",
        str(_RUNTIME_CODE_TRUSTED_BASE),
        "--runtime-code-authority-uid",
        "0",
        "--runtime-code-authority-gid",
        "0",
        "--request",
        str(_RUNTIME_CODE_MIGRATION),
        "--format",
        "json",
    )
    if preflight_tokens != expected_preflight:
        raise FormalRuntimeCommandError("required immutable binding is missing from preflight")
    start_prefix = (
        _WORKLOAD_ARBITER,
        "research",
        "--",
        _PYTHON_EXECUTABLE,
        "-I",
        _WRAPPER_EXECUTABLE,
        "formal",
    )
    if start_tokens[: len(start_prefix)] != start_prefix:
        raise FormalRuntimeCommandError("formal runtime service executable binding is invalid")
    wrapper = parse_formal_wrapper_argv(start_tokens[len(start_prefix) :])
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
    _inspect_wrapper_source(wrapper_source_path)
    try:
        return InspectedFormalSystemdService(
            unit_path=unit_path,
            wrapper_source_path=wrapper_source_path,
            preflight_argv=preflight_tokens[1:],
            wrapper=wrapper,
            environment_file=_ENVIRONMENT_FILE,
            environment=environment,
        )
    except ValueError as exc:
        raise FormalRuntimeCommandError("formal runtime service artifact is invalid") from exc


__all__ = [
    "FORMAL_RUNTIME_WRAPPER_CONTRACT",
    "FormalRuntimeBootstrapBinding",
    "FormalRuntimeCommandError",
    "FormalRuntimeWrapperBinding",
    "InspectedFormalSystemdService",
    "add_formal_runtime_bootstrap_arguments",
    "add_formal_runtime_deployment_arguments",
    "compose_formal_daemon_argv",
    "inspect_formal_systemd_service",
    "parse_formal_wrapper_argv",
]
