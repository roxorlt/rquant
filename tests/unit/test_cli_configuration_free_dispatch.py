"""BLK-8: the route A production commands have to run in the `.env`-less bootstrap worktree.

`main()` constructs `Settings` before it dispatches (T9-9), which is deliberate: a missing
configuration must fail at the entry point rather than half-way through a command. Acceptance
A22 carved out `runtime-authority-stage`, because the Release A bootstrap worktree
(`/home/lighthouse/rquant-relA` on the production host) has no `.env` at all.

The fifth route A install run found the same wall three commands further on:
`runtime-production-prerequisites`, `runtime-production-profile` and
`runtime-deployment-profile` are run from that very worktree and died in
`ValidationError: 5 validation errors for Settings` before their own parsers were reached.
Neither their parsers nor their handlers read anything out of `Settings`, so they get the A22
treatment — and this module holds all four halves of that in place:

* each of the three reaches its own parser, and a full dry run reaches its own handler,
  with no configuration anywhere in the environment;
* `rquant --help` and the neighbouring runtime commands keep failing closed (T9-9);
* the three handlers, and every module they import, stay clear of `rquant.config`, so a
  later edit cannot quietly reintroduce the failure this module fixes.
"""

from __future__ import annotations

import ast
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
CLI_SOURCE = REPO_ROOT / "src" / "rquant" / "cli.py"

#: Exactly the environment the Release A bootstrap worktree offers: no `.env`, and none of the
#: five required `Settings` fields. Mirrors `test_logging_lazy_settings.py`.
CONFIGURATION_FREE_ENVIRONMENT = {
    "PATH": os.environ.get("PATH", os.defpath),
    "LANG": "C",
    "RQUANT_DISABLE_DOTENV": "1",
}

#: The commands under test, each with a flag its own parser owns — proof that `--help` came
#: from the subparser rather than from the top-level parser.
ROUTE_A_COMMANDS = (
    ("runtime-deployment-profile", "--schema-v1-migration-authority"),
    ("runtime-production-prerequisites", "--runtime-mode"),
    ("runtime-production-profile", "--output-dir"),
)

#: Sibling runtime commands that are *not* config-free: they keep failing closed.
FAIL_CLOSED_COMMANDS = ("--help", "runtime-deployment-rollout", "ingest")

#: A full `runtime-production-prerequisites` dry run driven through `main()`. The two loaders
#: are replaced in the child so the run needs no canonical inputs bundle on disk; everything
#: else — the early dispatch, the parser, the handler, `market_calendar_generation_path` — is
#: the real thing, and the two `sys.modules` assertions are the actual subject of the test.
_DRY_RUN_PROBE = '''
import sys
from pathlib import Path
from types import SimpleNamespace

import rquant.runtime_production_profile as production_profile

assert "rquant.config" not in sys.modules, "importing the handler module built Settings"

root = Path({root!r})
retention = SimpleNamespace(
    service_kind=SimpleNamespace(value="artifact_retention"),
    settings={{"catalog_authority_root": str(root / "catalog-authority")}},
)
profile = SimpleNamespace(profile_id="c" * 64, manifests=(retention,))
inputs = SimpleNamespace(
    runtime_root=root / "runtime",
    market_calendar_content_sha256="d" * 64,
    definition_registry_root=root / "definitions",
)
production_profile.load_production_runtime_profile_inputs = lambda *a, **k: inputs
production_profile.build_production_runtime_profile = lambda value: profile

sys.argv = [
    "rquant",
    "runtime-production-prerequisites",
    "--inputs",
    str(root / "inputs.json"),
    "--expected-commit",
    "a" * 40,
]
from rquant.cli import main

assert "rquant.config" not in sys.modules, "importing rquant.cli built Settings"
code = main()
assert "rquant.config" not in sys.modules, "the early dispatch built Settings"
raise SystemExit(code)
'''

#: Import-time probe for every module the three handlers pull in.
_IMPORT_PROBE = (
    "import sys\n"
    "__import__({module!r})\n"
    "assert 'rquant.config' not in sys.modules, {module!r} + ' imported rquant.config'\n"
    "print('IMPORT-CONFIG-FREE')\n"
)

HANDLER_MODULES = (
    "rquant.runtime_deployment_profile",
    "rquant.runtime_market_calendar_generation",
    "rquant.runtime_production_profile",
)


def _run(argv: tuple[str, ...], *, cwd: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        argv,
        cwd=str(cwd),
        env=dict(CONFIGURATION_FREE_ENVIRONMENT),
        capture_output=True,
        text=True,
        check=False,
    )


def _drive_main(command_argv: tuple[str, ...]) -> tuple[str, ...]:
    """Run `main()` the way the console script does, without needing it installed."""

    program = (
        f"import sys; sys.argv = {['rquant', *command_argv]!r};"
        " from rquant.cli import main; raise SystemExit(main())"
    )
    return (sys.executable, "-c", program)


@pytest.mark.parametrize(("command", "own_flag"), ROUTE_A_COMMANDS)
def test_route_a_command_reaches_its_own_parser_without_configuration(
    command: str,
    own_flag: str,
    tmp_path: Path,
) -> None:
    """BLK-8: `--help` used to die in the entry point's fail-fast `get_settings()`."""

    result = _run(_drive_main((command, "--help")), cwd=tmp_path)

    assert result.returncode == 0, result.stderr
    assert f"usage: rquant {command}" in result.stdout
    assert own_flag in result.stdout
    assert "ValidationError" not in result.stderr


def test_route_a_dry_run_reaches_its_handler_without_configuration(tmp_path: Path) -> None:
    """The whole path, not just the parser: dispatch, handler and its output, config-free."""

    probe = tmp_path / "dry_run_probe.py"
    probe.write_text(_DRY_RUN_PROBE.format(root=str(tmp_path)), encoding="utf-8")

    result = _run((sys.executable, str(probe)), cwd=tmp_path)

    assert result.returncode == 0, result.stderr
    preview = json.loads(result.stdout)
    assert preview["status"] == "dry_run"
    assert preview["profile_id"] == "c" * 64
    assert preview["targets"] == [
        str(
            tmp_path
            / "runtime"
            / "authorities"
            / "market-calendar"
            / "generations"
            / f"{'d' * 64}.json"
        ),
        str(tmp_path / "definitions"),
        str(tmp_path / "catalog-authority" / "current.json"),
    ]


@pytest.mark.parametrize("command", FAIL_CLOSED_COMMANDS)
def test_every_other_command_still_fails_closed_without_configuration(
    command: str,
    tmp_path: Path,
) -> None:
    """T9-9 is narrowed by exactly three names, not weakened: the rest still fail at the door."""

    result = _run(_drive_main((command, "--help")), cwd=tmp_path)

    assert result.returncode != 0
    assert "Settings" in result.stderr


def test_the_configuration_free_set_is_exactly_the_route_a_commands() -> None:
    """The carve-out is a closed list; widening it is an explicit edit, never a side effect."""

    from rquant.cli import CONFIGURATION_FREE_COMMANDS

    assert set(CONFIGURATION_FREE_COMMANDS) == {command for command, _flag in ROUTE_A_COMMANDS}


def test_the_configuration_free_handlers_never_name_the_configuration_module() -> None:
    """Static guard: an added `get_settings()` would fail here instead of on the host."""

    from rquant.cli import CONFIGURATION_FREE_COMMANDS

    handler_names = {handler.__name__ for handler in CONFIGURATION_FREE_COMMANDS.values()}
    module = ast.parse(CLI_SOURCE.read_text(encoding="utf-8"), filename=str(CLI_SOURCE))
    handlers = {
        node.name: node
        for node in module.body
        if isinstance(node, ast.FunctionDef) and node.name in handler_names
    }
    assert handlers.keys() == handler_names, f"handlers not found in {CLI_SOURCE}"

    offenders: list[str] = []
    for name, handler in sorted(handlers.items()):
        for node in ast.walk(handler):
            if isinstance(node, ast.ImportFrom) and (node.module or "").startswith("rquant.config"):
                offenders.append(f"{name}:{node.lineno}: from {node.module} import ...")
            elif isinstance(node, ast.Import):
                offenders.extend(
                    f"{name}:{node.lineno}: import {alias.name}"
                    for alias in node.names
                    if alias.name.startswith("rquant.config")
                )
            elif isinstance(node, ast.Name) and node.id == "get_settings":
                offenders.append(f"{name}:{node.lineno}: get_settings")
            elif isinstance(node, ast.Attribute) and node.attr == "get_settings":
                offenders.append(f"{name}:{node.lineno}: .get_settings")

    assert not offenders, "a configuration-free handler reads Settings:\n" + "\n".join(offenders)


@pytest.mark.parametrize("module_name", HANDLER_MODULES)
def test_the_handler_modules_import_without_touching_the_configuration_module(
    module_name: str,
    tmp_path: Path,
) -> None:
    """The handlers import these lazily; importing one must not drag `Settings` in either."""

    result = _run(
        (sys.executable, "-c", _IMPORT_PROBE.format(module=module_name)),
        cwd=tmp_path,
    )

    assert result.returncode == 0, result.stderr
    assert "IMPORT-CONFIG-FREE" in result.stdout
