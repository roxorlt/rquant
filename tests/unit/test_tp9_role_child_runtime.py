"""TP9: the 28 runtime roles have to survive the wrapper's child environment.

The wrapper builds a role child from an empty environment (`LANG` / `LC_ALL` / `TZ` only),
starts it `-I -S`, and points it at an immutable generation whose working directory is not
a git checkout and holds no `.env`. Before TP9 only 4 of the 28 roles could even be
imported there. These tests hold the two halves of the fix in place: the lazy `Settings`
construction (T9-1, T9-7, T9-9) and the generation layout that mirrors the checkout so the
two `__file__`-relative hops in `lab_daemon` / `release_generation` stay inside the
generation (T9-1, T9-10).

Every temporary root is a `tempfile.mkdtemp()` subdirectory: `_require_trusted_directory`
rejects a world-writable ancestor and `/tmp` itself is 1777 on Linux CI.
"""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
import sysconfig
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
CHECKOUT_SRC = REPO_ROOT / "src"

#: Every module the frozen role policy can name, with how many of the 28 roles it serves.
#: Kept as a literal so a policy edit that adds a module fails this test rather than
#: silently narrowing what the import probe covers.
ROLE_MODULE_COVERAGE: tuple[tuple[str, int], ...] = (
    ("rquant.runtime_service_main", 23),
    ("rquant.page_control_service", 1),
    ("rquant.runtime_recovery_service", 2),
    ("rquant.workload_isolation", 1),
    ("rquant.lab_formal_runtime_entry", 1),
)

#: `build_child_environment` copies only these names, and only when the host already has
#: them. Anything else the child needs has to come from the root-owned documents.
CHILD_ENVIRONMENT_NAMES = ("LANG", "LC_ALL", "TZ")

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


def child_environment() -> dict[str, str]:
    source = dict(os.environ)
    return {name: source[name] for name in CHILD_ENVIRONMENT_NAMES if name in source}


def site_packages_path() -> str:
    return sysconfig.get_paths()["purelib"]


def build_generation_code_tree(root: Path, *, mirror_checkout: bool) -> Path:
    """Lay out a generation code tree and return its `app_source`.

    `mirror_checkout=False` reproduces the flat `<gen>/app` layout the v1 spec described,
    which is what the negative half of T9-10 asserts is unusable.
    """

    app_source = root / ("src" if mirror_checkout else "app")
    shutil.copytree(
        CHECKOUT_SRC / "rquant",
        app_source / "rquant",
        ignore=shutil.ignore_patterns("__pycache__"),
    )
    if mirror_checkout:
        scripts = root / "scripts"
        scripts.mkdir(exist_ok=True)
        shutil.copyfile(REPO_ROOT / "scripts" / "strict_json.py", scripts / "strict_json.py")
    return app_source


def import_role_module(module: str, *, app_source: Path, cwd: Path) -> subprocess.CompletedProcess:
    return subprocess.run(
        [
            sys.executable,
            "-I",
            "-S",
            "-c",
            _IMPORT_PROBE.format(
                paths=[str(app_source), site_packages_path()],
                module=module,
            ),
        ],
        cwd=str(cwd),
        env=child_environment(),
        capture_output=True,
        text=True,
        check=False,
    )


@pytest.fixture(scope="module")
def mirrored_generation(tmp_path_factory: pytest.TempPathFactory) -> tuple[Path, Path]:
    """A generation tree shaped like the checkout, plus a non-git working directory."""

    root = tmp_path_factory.mktemp("tp9-generation")
    app_source = build_generation_code_tree(root, mirror_checkout=True)
    cwd = root / "cwd"
    cwd.mkdir()
    return app_source, cwd


# ---------------------------------------------------------------------------------------
# T9-1 / T9-10: the code tree
# ---------------------------------------------------------------------------------------


def test_t9_1_every_role_module_imports_in_the_wrapper_child_environment(
    mirrored_generation: tuple[Path, Path],
) -> None:
    """T9-1: 28/28 roles import under `-I -S`, three environment names, non-git cwd."""

    app_source, cwd = mirrored_generation
    assert not (cwd / ".git").exists()
    assert not (cwd / ".env").exists()

    covered = 0
    failures: list[str] = []
    for module, roles in ROLE_MODULE_COVERAGE:
        result = import_role_module(module, app_source=app_source, cwd=cwd)
        if result.returncode == 0:
            covered += roles
        else:
            failures.append(f"{module}: {(result.stdout + result.stderr).strip()[:200]}")
    assert not failures, "role modules still die in the child environment:\n" + "\n".join(failures)
    assert covered == 28


def test_t9_1_role_policy_modules_match_the_probed_set() -> None:
    """The 28/28 claim is only worth as much as the module list it iterates."""

    from rquant.runtime_authority import PRODUCTION_ROLE_POLICY

    probed = {module for module, _roles in ROLE_MODULE_COVERAGE}
    assert {entry.module for entry in PRODUCTION_ROLE_POLICY} == probed
    counted = {module: 0 for module in probed}
    for entry in PRODUCTION_ROLE_POLICY:
        counted[entry.module] += 1
    assert counted == dict(ROLE_MODULE_COVERAGE)
    assert len(PRODUCTION_ROLE_POLICY) == 28


def test_t9_10_flat_generation_layout_breaks_the_import_time_file_hops(
    tmp_path: Path,
) -> None:
    """T9-10 (negative): `<gen>/app` without `<gen>/scripts` cannot import the role modules.

    `lab_daemon` reads `parents[2]/scripts/strict_json.py` while it is being imported, and
    `scripts/strict_json.py` reads `parents[1]/src/rquant/strict_json.py` right back. Only
    the mirrored layout keeps both hops inside the generation.
    """

    app_source = build_generation_code_tree(tmp_path, mirror_checkout=False)
    cwd = tmp_path / "cwd"
    cwd.mkdir()

    result = import_role_module("rquant.runtime_service_main", app_source=app_source, cwd=cwd)
    assert result.returncode != 0
    assert "FileNotFoundError" in result.stdout + result.stderr


def test_t9_10_mirrored_generation_holds_both_file_hop_targets(
    mirrored_generation: tuple[Path, Path],
) -> None:
    """T9-10: the generation carries `src/rquant/**` and `scripts/strict_json.py`."""

    app_source, _cwd = mirrored_generation
    generation = app_source.parent
    assert app_source.name == "src"
    assert (generation / "scripts" / "strict_json.py").is_file()
    assert (app_source / "rquant" / "__init__.py").is_file()
    assert (app_source / "rquant" / "strict_json.py").is_file()
    assert (app_source / "rquant" / "lab_daemon.py").is_file()


# ---------------------------------------------------------------------------------------
# T9-7 / T9-9: the lazy settings object
# ---------------------------------------------------------------------------------------


def test_t9_7_get_settings_is_a_singleton() -> None:
    from rquant.config import get_settings

    assert get_settings() is get_settings()


def test_t9_7_module_attribute_yields_the_same_object_as_get_settings() -> None:
    """The 78 `setattr(<…>settings, …)` call points mutate this one cached instance."""

    import rquant.config as config

    from rquant.config import get_settings

    assert config.settings is get_settings()
    assert getattr(config, "settings") is get_settings()  # noqa: B009 - the hook is the point


def test_t9_7_attribute_mutation_is_visible_through_every_reader(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """What the suite actually does: mutate an attribute of the shared instance."""

    import rquant.config as config
    from rquant.config import get_settings
    from rquant.storage.duckdb import _settings as duckdb_settings

    monkeypatch.setattr(config.settings, "pushdeer_keys", "tp9-probe")
    assert get_settings().pushdeer_keys == "tp9-probe"
    assert duckdb_settings().pushdeer_keys == "tp9-probe"


def test_t9_7_module_rebinding_still_shadows_the_lazy_hook(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The one other form in the suite: rebind `rquant.config.settings` wholesale."""

    import rquant.config as config

    sentinel = object()
    monkeypatch.setattr(config, "settings", sentinel)
    assert config.settings is sentinel

    from rquant.config import settings as imported

    assert imported is sentinel


def test_t9_7_unknown_module_attribute_still_raises_attribute_error() -> None:
    import rquant.config as config

    with pytest.raises(AttributeError, match="no attribute 'not_a_setting'"):
        config.not_a_setting  # noqa: B018 - the access is the assertion


def test_t9_9_cli_entry_builds_settings_before_parsing_arguments(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """T9-9: the lazy construction must not push a missing-config failure into a command."""

    import rquant.cli as cli
    import rquant.config as config

    class _ConfigMissing(RuntimeError):
        pass

    def refuse() -> object:
        raise _ConfigMissing("settings are unavailable")

    def unreachable_parser() -> object:
        raise AssertionError("the entry point parsed arguments before reading the settings")

    monkeypatch.setattr(config, "get_settings", refuse)
    monkeypatch.setattr(cli, "build_parser", unreachable_parser)

    with pytest.raises(_ConfigMissing):
        cli.main()


def test_t9_9_cli_help_still_fails_closed_without_configuration(tmp_path: Path) -> None:
    """The same guarantee end to end: no configuration, no `--help`, non-zero exit."""

    empty_env = {
        name: value
        for name, value in os.environ.items()
        if name in {"PATH", "HOME", "LANG", "LC_ALL", "TZ", "SYSTEMROOT"}
    }
    empty_env["RQUANT_DISABLE_DOTENV"] = "1"
    program = "import sys; sys.argv = ['rquant', '--help']; from rquant.cli import main; raise SystemExit(main())"  # noqa: E501

    result = subprocess.run(
        [sys.executable, "-c", program],
        cwd=str(tmp_path),
        env=empty_env,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode != 0
    assert "Settings" in result.stderr

    configured = subprocess.run(
        [sys.executable, "-c", program],
        cwd=str(tmp_path),
        env=dict(os.environ),
        capture_output=True,
        text=True,
        check=False,
    )
    assert configured.returncode == 0
    assert "usage:" in configured.stdout
