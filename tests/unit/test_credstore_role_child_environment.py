"""#215: the credstore roles' own import chain, in the wrapper's child environment.

TP9 pinned that all 28 role *modules* import under `-I -S` with `LANG` / `LC_ALL` / `TZ` and
nothing else. That was necessary and not sufficient: three of these six roles reach their
Tushare adapter and the seventh reaches its notification providers only while their builder
runs, one lazy import deeper than `rquant.runtime_service_main`, and both of those chains
still constructed `Settings` while being imported. Under the wrapper's environment the
construction cannot succeed, so `reference_slow_source`, `market_minute_source` and
`auction_match_source` died with `5 validation errors for Settings` before any role code ran,
and the notifier would have as soon as #218's route spool stopped shadowing it.

The end-to-end acceptance cannot catch this: it runs in a process whose environment can build
a `Settings`, and one is cached by the time a role starts. Only a child with the real
environment can, which is why this file spawns one — the same probe shape as TP9's, against a
generation-shaped tree with no `.env` and a working directory that is not a checkout.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest

from tests.unit.test_tp9_role_child_runtime import (
    CHILD_ENVIRONMENT_NAMES,
    build_generation_code_tree,
    child_environment,
    site_packages_path,
)

#: Build the registry, then walk the two chains that only a running builder reaches: the
#: default Tushare adapter factory (`reference_slow_source`, `market_minute_source`,
#: `auction_match_source`, and `daily_close_source` through its own fetcher) and the
#: environment notification provider loader (`notifier`). Then read back whether anything on
#: the way built a `Settings` — the module may legitimately be imported; the object may not
#: be constructed, because in this environment constructing it is exactly what fails.
_BUILDER_PROBE = """
import sys
sys.path[:0] = {paths!r}
from rquant.runtime_service_builtin import build_builtin_registry, _default_adapter_factory
from rquant.runtime_service_entrypoint import RuntimeServiceKind

capabilities = {{"TUSHARE_TOKEN_MAIN": "probe-token", "PUSHDEER_KEYS": "probe-key"}}
registry = build_builtin_registry(runtime_capabilities=capabilities)
for name in {kinds!r}:
    assert RuntimeServiceKind(name) in registry.registered_kinds, name

adapter = _default_adapter_factory(capabilities)
assert type(adapter).__name__ == "TushareAdapter"

from rquant.runtime_builder_daily import _tushare_daily_close_fetcher

assert callable(_tushare_daily_close_fetcher(capabilities))

from rquant.runtime_notification_providers import (
    build_environment_notification_provider_loader,
)

providers = build_environment_notification_provider_loader(environment=capabilities)()
assert providers, "the notifier built no provider"

import rquant.config as configuration

assert configuration._SETTINGS is None, "something built Settings in the child environment"
print("BUILDERS-OK")
"""

CREDSTORE_KINDS = (
    "reference_slow_source",
    "reference_slow_publisher",
    "market_minute_source",
    "auction_match_source",
    "daily_close_source",
    "notifier",
    "artifact_retention",
)

#: The modules the six roles' builders import that used to construct `Settings`. Named one by
#: one so a regression says which chain came back rather than only that one did.
CREDSTORE_CALL_TIME_MODULES = (
    "rquant.adapter.tushare",
    "rquant.notify",
    "rquant.notify.api",
    "rquant.notify.log",
    "rquant.notify.client",
    "rquant.runtime_notification_providers",
    "rquant.runtime_builder_daily",
    "rquant.runtime_builder_signal",
    "rquant.runtime_builder_retention",
    "rquant.reference_slow_runtime",
    "rquant.reference_data_registry",
)

_IMPORT_PROBE = """
import sys
sys.path[:0] = {paths!r}
try:
    __import__({module!r})
except BaseException as exc:
    print("IMPORT-FAIL", type(exc).__name__, str(exc).splitlines()[0][:160])
    raise SystemExit(1)
print("IMPORT-OK")
"""


@pytest.fixture(scope="module")
def generation(tmp_path_factory: pytest.TempPathFactory) -> tuple[Path, Path]:
    root = tmp_path_factory.mktemp("credstore-generation")
    app_source = build_generation_code_tree(root, mirror_checkout=True)
    cwd = root / "cwd"
    cwd.mkdir()
    return app_source, cwd


def _run(program: str, *, app_source: Path, cwd: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-I", "-S", "-c", program],
        cwd=str(cwd),
        env=child_environment(),
        capture_output=True,
        text=True,
        check=False,
    )


def test_the_child_environment_is_the_three_names_the_wrapper_copies() -> None:
    """The probe is worth nothing if it runs with more than the wrapper would give."""

    assert CHILD_ENVIRONMENT_NAMES == ("LANG", "LC_ALL", "TZ")
    assert set(child_environment()) <= set(CHILD_ENVIRONMENT_NAMES)


def test_every_credstore_call_time_module_imports_in_the_child_environment(
    generation: tuple[Path, Path],
) -> None:
    """One module per line, so a regression names the chain that came back."""

    app_source, cwd = generation
    assert not (cwd / ".git").exists()
    assert not (cwd / ".env").exists()

    failures: list[str] = []
    for module in CREDSTORE_CALL_TIME_MODULES:
        result = _run(
            _IMPORT_PROBE.format(paths=[str(app_source), site_packages_path()], module=module),
            app_source=app_source,
            cwd=cwd,
        )
        if result.returncode != 0:
            failures.append(f"{module}: {(result.stdout + result.stderr).strip()[:200]}")
    assert not failures, "credstore chains still die in the child environment:\n" + "\n".join(
        failures
    )


def test_the_credstore_builders_construct_in_the_child_environment(
    generation: tuple[Path, Path],
) -> None:
    """Registry, real Tushare adapter, real notification providers — and no `Settings`."""

    app_source, cwd = generation
    result = _run(
        _BUILDER_PROBE.format(
            paths=[str(app_source), site_packages_path()], kinds=list(CREDSTORE_KINDS)
        ),
        app_source=app_source,
        cwd=cwd,
    )

    assert result.returncode == 0, result.stdout + result.stderr
    assert "BUILDERS-OK" in result.stdout
    assert "ValidationError" not in result.stderr


def test_the_probe_would_notice_a_settings_built_on_the_way(
    generation: tuple[Path, Path],
) -> None:
    """A probe that cannot fail proves nothing: make the same child build one, and watch.

    Constructing `Settings` in this environment is what raises, so this both shows the
    assertion is reachable and re-states the defect: five required fields, none of them
    present, no `.env` to fall back on.
    """

    app_source, cwd = generation
    result = _run(
        "import sys\n"
        f"sys.path[:0] = {[str(app_source), site_packages_path()]!r}\n"
        "from rquant.config import get_settings\n"
        "get_settings()\n",
        app_source=app_source,
        cwd=cwd,
    )

    assert result.returncode != 0
    assert "validation errors for Settings" in result.stderr
