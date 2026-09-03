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

import hashlib
import json
import os
import shutil
import stat
import subprocess
import sys
import sysconfig
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any

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

    class _ConfigMissingError(RuntimeError):
        pass

    def refuse() -> object:
        raise _ConfigMissingError("settings are unavailable")

    def unreachable_parser() -> object:
        raise AssertionError("the entry point parsed arguments before reading the settings")

    monkeypatch.setattr(config, "get_settings", refuse)
    monkeypatch.setattr(cli, "build_parser", unreachable_parser)

    with pytest.raises(_ConfigMissingError):
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


# ---------------------------------------------------------------------------------------
# T9-4 / T9-5 / T9-6: `runtime_service_main.run()` under `--authority-runtime`
# ---------------------------------------------------------------------------------------

COMMIT = "0123456789abcdef0123456789abcdef01234567"
GENERATION = "f" * 64
INSTANCE = "svc-" + "a" * 64
PRODUCTION_RUNTIME_ROOT = Path("/home/lighthouse/rquant/data/runtime")


def _kind_manifest(kind: str, *, service_id: str = "source.market-minute") -> object:
    from rquant.runtime_service_entrypoint import RuntimeServiceKind, RuntimeServiceManifest

    return RuntimeServiceManifest(
        service_id=service_id,
        service_kind=RuntimeServiceKind(kind),
        plane="live",
        interval_seconds=15,
        stale_after_seconds=45,
        producer_commit=COMMIT,
        settings={},
    )


def _write_generation_manifest(
    root: Path,
    manifest: object,
    *,
    generation: str = GENERATION,
    instance: str = INSTANCE,
    mode: int = 0o444,
) -> Path:
    """`<root>/generations/<generation>/manifests/<instance>.json`, as a generation holds it."""

    directory = root / "generations" / generation / "manifests"
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"{instance}.json"
    path.write_text(manifest.model_dump_json(), encoding="utf-8")  # type: ignore[attr-defined]
    path.chmod(mode)
    return path


def _authority_args(
    manifest_path: Path,
    control_root: Path,
    *,
    kind: str | None = None,
    once: bool = False,
) -> object:
    from rquant.runtime_service_main import build_parser

    argv = [
        "--manifest",
        str(manifest_path),
        "--control-root",
        str(control_root),
        "--expected-commit",
        COMMIT,
        "--expected-generation",
        GENERATION,
    ]
    if kind is not None:
        argv.extend(["--expected-kind", kind])
    if once:
        argv.append("--once")
    argv.append("--authority-runtime")
    return build_parser().parse_args(argv)


def _forbid_git(monkeypatch: pytest.MonkeyPatch) -> list[object]:
    calls: list[object] = []

    def refuse(*args: object, **kwargs: object) -> object:
        calls.append((args, kwargs))
        raise AssertionError("run() must not start a git subprocess under --authority-runtime")

    monkeypatch.setattr("rquant.runtime_service_main.subprocess.run", refuse)
    return calls


def test_t9_4_authority_runtime_trusts_expected_commit_and_runs_no_git(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """T9-4: no git subprocess; the commit is `--expected-commit`, checked against the manifest."""

    import rquant.runtime_service_main as service_main

    manifest = _kind_manifest("market_minute_source")
    manifest_path = _write_generation_manifest(tmp_path, manifest)
    control_root = tmp_path / "runtime" / "control" / "market-minute-sources" / INSTANCE
    git_calls = _forbid_git(monkeypatch)
    observed: dict[str, object] = {}

    def fake_registry(**kwargs: object) -> object:
        observed["registry_kwargs"] = kwargs
        return object()

    def fake_run(
        loaded: object, *, registry: object, control_root: Path, **_kwargs: object
    ) -> object:
        observed.update(manifest=loaded, registry=registry, control_root=control_root)
        return object()

    monkeypatch.setattr(service_main, "build_builtin_registry", fake_registry)
    monkeypatch.setattr(service_main, "run_runtime_service_manifest", fake_run)

    args = _authority_args(manifest_path, control_root, kind="market_minute_source")
    assert service_main.run(args) == 0

    assert git_calls == []
    assert observed["manifest"] == manifest
    assert observed["control_root"] == control_root
    assert observed["registry_kwargs"]["runtime_capabilities"] == {}  # type: ignore[index]


def test_t9_4_authority_runtime_rejects_a_manifest_for_another_commit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The commit comparison did not go away with git: the manifest still has to match it."""

    import rquant.runtime_service_main as service_main
    from rquant.runtime_service_entrypoint import RuntimeServiceManifest

    manifest = _kind_manifest("market_minute_source")
    other = RuntimeServiceManifest.model_validate(
        {**manifest.model_dump(), "producer_commit": "9" * 40}
    )
    manifest_path = _write_generation_manifest(tmp_path, other)
    control_root = tmp_path / "runtime" / "control" / "market-minute-sources" / INSTANCE
    _forbid_git(monkeypatch)
    monkeypatch.setattr(
        service_main,
        "build_builtin_registry",
        lambda **_kwargs: pytest.fail("registry must not be built"),
    )

    with pytest.raises(ValueError, match="commit does not match running code"):
        service_main.run(_authority_args(manifest_path, control_root))


@pytest.mark.parametrize(
    ("relocate", "match"),
    [
        (lambda p: p.parent.parent / "elsewhere" / p.name, "outside the generation manifest"),
        (lambda p: p.parents[2] / ("e" * 64) / "manifests" / p.name, "generation does not match"),
    ],
)
def test_t9_4_authority_manifest_must_sit_in_the_expected_generation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    relocate: object,
    match: str,
) -> None:
    from rquant.runtime_service_main import load_authority_service_manifest

    manifest = _kind_manifest("market_minute_source")
    manifest_path = _write_generation_manifest(tmp_path, manifest)
    moved = relocate(manifest_path)  # type: ignore[operator]
    moved.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.rename(moved)

    with pytest.raises(ValueError, match=match):
        load_authority_service_manifest(
            moved, expected_commit=COMMIT, expected_generation=GENERATION
        )


def test_t9_4_authority_manifest_accepts_root_style_0444_and_refuses_group_writable(
    tmp_path: Path,
) -> None:
    """A generation's copy is root-owned 0444; the legacy reader's 0600 rule cannot apply."""

    from rquant.runtime_service_main import load_authority_service_manifest

    manifest = _kind_manifest("market_minute_source")
    manifest_path = _write_generation_manifest(tmp_path, manifest, mode=0o444)
    loaded = load_authority_service_manifest(
        manifest_path, expected_commit=COMMIT, expected_generation=GENERATION
    )
    assert loaded == manifest

    manifest_path.chmod(0o464)
    with pytest.raises(ValueError, match="writable outside its owner"):
        load_authority_service_manifest(
            manifest_path, expected_commit=COMMIT, expected_generation=GENERATION
        )


def test_t9_4_authority_manifest_refuses_a_symlinked_component(tmp_path: Path) -> None:
    from rquant.runtime_service_main import load_authority_service_manifest

    manifest = _kind_manifest("market_minute_source")
    real = _write_generation_manifest(tmp_path / "real", manifest)
    linked_root = tmp_path / "linked"
    (linked_root / "generations").mkdir(parents=True)
    (linked_root / "generations" / GENERATION).symlink_to(real.parent.parent)
    via_link = linked_root / "generations" / GENERATION / "manifests" / real.name
    assert via_link.read_bytes() == real.read_bytes()

    with pytest.raises(ValueError, match="contains a symlink"):
        load_authority_service_manifest(
            via_link, expected_commit=COMMIT, expected_generation=GENERATION
        )


def test_t9_5_runtime_root_arithmetic_holds_for_every_control_root_in_the_policy() -> None:
    """T9-5: `parents[2]` of every real control root, with any instance label appended."""

    from rquant.runtime_authority import PRODUCTION_ROLE_POLICY
    from rquant.runtime_service_main import runtime_root_from_control_root

    with_control_root = [entry for entry in PRODUCTION_ROLE_POLICY if entry.control_root]
    # 22 kind-backed roles + page_control + the two recovery roles. The three roles without
    # a control root (`daily`, `lab_claim_finalizer`, `workload_admission`) derive nothing.
    assert len(with_control_root) == 25
    for entry in with_control_root:
        derived = runtime_root_from_control_root(Path(entry.control_root) / INSTANCE)
        assert derived == PRODUCTION_RUNTIME_ROOT, entry.name


@pytest.mark.parametrize(
    "control_root",
    [
        "/svc-instance",
        "/control/svc-instance",
        "/runtime/not-control/strategies/svc-instance",
        "/home/lighthouse/rquant/data/runtime/strategies/svc-instance",
    ],
)
def test_t9_5_runtime_root_arithmetic_raises_on_an_unexpected_shape(control_root: str) -> None:
    """Never `None`: a control root that is not `<root>/control/<kind>/<instance>` is an error."""

    from rquant.runtime_service_main import runtime_root_from_control_root

    with pytest.raises(ValueError, match="runtime control tree"):
        runtime_root_from_control_root(Path(control_root))


def test_t9_5_runtime_root_is_not_the_recovery_specific_helper() -> None:
    """S-16: `runtime_recovery_service.runtime_root_for` refuses 22 of the 26 control roots."""

    from rquant.runtime_authority import PRODUCTION_ROLE_POLICY
    from rquant.runtime_recovery_service import runtime_root_for
    from rquant.runtime_service_main import runtime_root_from_control_root

    refused = 0
    for entry in PRODUCTION_ROLE_POLICY:
        if not entry.control_root:
            continue
        control_root = Path(entry.control_root) / INSTANCE
        assert runtime_root_from_control_root(control_root) == PRODUCTION_RUNTIME_ROOT
        try:
            runtime_root_for(control_root)
        except ValueError:
            refused += 1
    assert refused == 23  # every kind directory except `recovery` (2 roles share it)


def test_t9_5_existing_runtime_root_is_used_for_schema_bindings(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When the derived root exists, it is the one handed to the schema-binding loader."""

    import rquant.runtime_service_main as service_main

    manifest = _kind_manifest("market_minute_source")
    manifest_path = _write_generation_manifest(tmp_path, manifest)
    runtime_root = tmp_path / "runtime"
    control_root = runtime_root / "control" / "market-minute-sources" / INSTANCE
    control_root.mkdir(parents=True)
    _forbid_git(monkeypatch)
    observed: dict[str, object] = {}

    def fake_bindings(root: Path, **kwargs: object) -> tuple[()]:
        observed["schema_root"] = root
        return ()

    monkeypatch.setattr(service_main, "load_runtime_schema_service_bindings", fake_bindings)
    monkeypatch.setattr(
        service_main,
        "build_builtin_registry",
        lambda **kwargs: observed.update(registry_kwargs=kwargs) or object(),
    )
    monkeypatch.setattr(
        service_main, "run_runtime_service_manifest", lambda *_a, **_k: object()
    )

    assert service_main.run(_authority_args(manifest_path, control_root)) == 0
    assert observed["schema_root"] == runtime_root
    assert "startup_degraded_reasons" not in observed["registry_kwargs"]  # type: ignore[operator]


def test_t9_6_missing_runtime_root_warns_and_marks_the_registry_degraded(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """T9-6: the degradation is a WARNING in the journal and a reason on the registry."""

    from loguru import logger

    import rquant.runtime_service_main as service_main

    manifest = _kind_manifest("market_minute_source")
    manifest_path = _write_generation_manifest(tmp_path, manifest)
    control_root = tmp_path / "runtime" / "control" / "market-minute-sources" / INSTANCE
    assert not (tmp_path / "runtime").exists()
    _forbid_git(monkeypatch)
    monkeypatch.setattr(
        service_main,
        "load_runtime_schema_service_bindings",
        lambda *_a, **_k: pytest.fail("schema bindings must not be loaded without a root"),
    )
    observed: dict[str, object] = {}
    monkeypatch.setattr(
        service_main,
        "build_builtin_registry",
        lambda **kwargs: observed.update(registry_kwargs=kwargs) or object(),
    )
    monkeypatch.setattr(
        service_main, "run_runtime_service_manifest", lambda *_a, **_k: object()
    )
    records: list[object] = []
    sink = logger.add(lambda message: records.append(message.record), level="WARNING")
    try:
        assert service_main.run(_authority_args(manifest_path, control_root)) == 0
    finally:
        logger.remove(sink)

    warnings = [r for r in records if r["level"].name == "WARNING"]  # type: ignore[index]
    assert len(warnings) == 1
    text = warnings[0]["message"]  # type: ignore[index]
    assert str(tmp_path / "runtime") in text
    assert "market_minute_source" in text
    assert "schema dual write" in text
    kwargs = observed["registry_kwargs"]
    assert kwargs["startup_degraded_reasons"] == (  # type: ignore[index]
        service_main.RUNTIME_ROOT_DEGRADED_REASON,
    )


def test_t9_6_degraded_reason_reaches_the_published_heartbeat(tmp_path: Path) -> None:
    """The health signal, not only the log: the loop's heartbeat carries the reason."""

    from threading import Event

    from rquant.runtime_service_control import (
        RuntimeServiceControl,
        RuntimeServiceStatus,
        RuntimeStepResult,
    )
    from rquant.runtime_service_entrypoint import (
        RuntimeServiceKind,
        RuntimeServiceRegistry,
        run_runtime_service_manifest,
    )
    from rquant.runtime_service_main import (
        RUNTIME_ROOT_DEGRADED_REASON,
        _StartupDegradedRegistry,
    )

    manifest = _kind_manifest("market_minute_source")
    inner = RuntimeServiceRegistry()
    inner.register(
        RuntimeServiceKind.MARKET_MINUTE_SOURCE,
        lambda _manifest: (lambda: RuntimeStepResult(processed_count=1)),
    )
    registry = _StartupDegradedRegistry(inner, reasons=(RUNTIME_ROOT_DEGRADED_REASON,))
    assert registry.registered_kinds == (RuntimeServiceKind.MARKET_MINUTE_SOURCE,)

    # The step the loop will call carries the startup reason on top of its own result.
    step = registry.build(manifest)
    stamped = step()
    assert stamped.processed_count == 1
    assert stamped.degraded_reasons == (RUNTIME_ROOT_DEGRADED_REASON,)

    # One real loop iteration through the real control: the persisted heartbeat, which is
    # what `runtime_health_publisher` reads, keeps the reason after the loop stops.
    control_root = tmp_path / "control"
    heartbeat = run_runtime_service_manifest(
        manifest,
        registry=registry,
        control_root=control_root,
        stop_event=Event(),
        max_iterations=1,
    )
    assert heartbeat.total_successes == 1
    assert RUNTIME_ROOT_DEGRADED_REASON in heartbeat.degraded_reasons
    persisted = RuntimeServiceControl.read_heartbeat(control_root, manifest.service_spec)
    assert persisted is not None
    assert RUNTIME_ROOT_DEGRADED_REASON in persisted.degraded_reasons

    # And while the service runs, that reason is what turns `running` into `degraded`.
    control = RuntimeServiceControl(tmp_path / "control-live", spec=manifest.service_spec)
    control.start()
    try:
        live = control.record_success(stamped)
    finally:
        control.stop(reason="test complete")
    assert live.status is RuntimeServiceStatus.DEGRADED
    assert live.degraded_reasons == (RUNTIME_ROOT_DEGRADED_REASON,)


def test_t9_6_strategy_live_still_fails_closed_without_a_runtime_root(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The one kind that must not degrade: no root, no service, no registry, no loop."""

    import rquant.runtime_service_main as service_main

    manifest = _kind_manifest("strategy_live", service_id="strategy.n_shape.v1")
    manifest_path = _write_generation_manifest(tmp_path, manifest)
    control_root = tmp_path / "runtime" / "control" / "strategies" / INSTANCE
    _forbid_git(monkeypatch)
    monkeypatch.setattr(
        service_main,
        "build_builtin_registry",
        lambda **_kwargs: pytest.fail("registry must not be built"),
    )
    monkeypatch.setattr(
        service_main,
        "run_runtime_service_manifest",
        lambda *_a, **_k: pytest.fail("service must not run"),
    )

    with pytest.raises(ValueError, match="strategy-live runtime must use a current deployment"):
        service_main.run(_authority_args(manifest_path, control_root, kind="strategy_live"))


# ---------------------------------------------------------------------------------------
# TCB-2: the frozen policy hands `--authority-runtime` to the 22 kind-backed roles
# ---------------------------------------------------------------------------------------

AUTHORITY_RUNTIME_ARGUMENT = "--authority-runtime"


def test_tcb_2_every_kind_backed_role_carries_the_authority_runtime_literal() -> None:
    """The 22 roles served by `runtime_service_main` with a service kind, and only those."""

    from rquant.runtime_authority import PRODUCTION_ROLE_POLICY

    kind_backed = [entry for entry in PRODUCTION_ROLE_POLICY if entry.service_kind]
    assert len(kind_backed) == 22
    for entry in kind_backed:
        assert entry.module == "rquant.runtime_service_main", entry.name
        assert entry.control_root, entry.name
        assert entry.module_arguments == (AUTHORITY_RUNTIME_ARGUMENT,), entry.name

    untouched = {
        entry.name: entry.module_arguments
        for entry in PRODUCTION_ROLE_POLICY
        if not entry.service_kind
    }
    assert untouched == {
        "daily": (),
        "lab_claim_finalizer": ("lab-claim-finalizer",),
        "page_control": (),
        "runtime_recovery": ("--mode", "execute"),
        "runtime_recovery_rehearsal": ("--mode", "rehearse"),
        "workload_admission": ("research-admission",),
    }


def test_tcb_2_the_literal_survives_the_profile_role_and_parses_as_the_flag() -> None:
    """Profile-side validation admits it, and the module parser reads it as the flag."""

    from rquant.runtime_authority import PRODUCTION_ROLE_POLICY, RuntimeProfileRole
    from rquant.runtime_service_main import build_parser

    for entry in PRODUCTION_ROLE_POLICY:
        if not entry.service_kind:
            continue
        role = RuntimeProfileRole(
            module=entry.module,
            environment_allowlist=entry.environment_allowlist,
            instances=(INSTANCE,),
            service_kind=entry.service_kind,
            control_root=entry.control_root,
            once=entry.once,
            module_arguments=entry.module_arguments,
        )
        assert role.payload()["module_arguments"] == [AUTHORITY_RUNTIME_ARGUMENT]
        derived = [
            "--manifest",
            f"/g/{GENERATION}/manifests/{INSTANCE}.json",
            "--control-root",
            f"{entry.control_root}/{INSTANCE}",
            "--expected-commit",
            COMMIT,
            "--expected-generation",
            GENERATION,
            "--expected-kind",
            entry.service_kind,
            *(["--once"] if entry.once else []),
            *entry.module_arguments,
        ]
        parsed = build_parser().parse_args(derived)
        assert parsed.authority_runtime is True
        assert parsed.once is entry.once
        assert [kind.value for kind in parsed.expected_kind] == [entry.service_kind]


def test_tcb_2_module_arguments_are_part_of_the_profile_identity() -> None:
    """`profile_id = sha256(canonical(body))` and `body` carries `module_arguments`."""

    import hashlib

    from rquant.runtime_authority import PRODUCTION_ROLE_POLICY
    from rquant.strict_json import canonical_json_bytes

    def body(strip_flag: bool) -> dict[str, object]:
        return {
            "roles": {
                entry.name: {
                    "module": entry.module,
                    "module_arguments": [
                        argument
                        for argument in entry.module_arguments
                        if not (strip_flag and argument == AUTHORITY_RUNTIME_ARGUMENT)
                    ],
                }
                for entry in PRODUCTION_ROLE_POLICY
            }
        }

    with_flag = hashlib.sha256(canonical_json_bytes(body(strip_flag=False))).hexdigest()
    without_flag = hashlib.sha256(canonical_json_bytes(body(strip_flag=True))).hexdigest()
    assert with_flag != without_flag


# ---------------------------------------------------------------------------------------
# T9-2 / T9-3 / T9-10: one published authority world, and the first-gate roles as children
# ---------------------------------------------------------------------------------------

#: The 16 units the first gate enables (`acceptance-pra.md` §5), by role name.
FIRST_GATE_ROLES: tuple[str, ...] = (
    "auction_universe_publisher",  # rquant-runtime-auction-universe@
    "candidate_publisher",  # rquant-runtime-candidate@
    "daily_pipeline_orchestrator",  # rquant-runtime-daily-orchestrator@ (the one oneshot)
    "feature_live",  # rquant-runtime-feature@
    "lab_artifact_catalog",  # rquant-runtime-artifact-catalog@
    "lab_claim_finalizer",  # rquant-lab-claim-finalizer
    "lab_jobs_publisher",  # rquant-runtime-lab-jobs@
    "paper_broker",  # rquant-runtime-paper-broker@
    "paper_constraint_publisher",  # rquant-runtime-paper-constraint@
    "promotions_publisher",  # rquant-runtime-promotions@
    "runtime_health_publisher",  # rquant-runtime-runtime-health@
    "serving_publisher",  # rquant-runtime-serving@
    "shadow_session",  # rquant-runtime-shadow@
    "signal_router",  # rquant-runtime-signal-router@
    "strategy_live",  # rquant-runtime-strategy@
    "watchlist_quote_source",  # rquant-runtime-watchlist-quote@
)
#: Deferred to the shadow window: they hard-load the legacy `data/runtime/current` (§10.5).
LEGACY_ROOT_ROLES: tuple[str, ...] = (
    "page_control",
    "runtime_recovery",
    "runtime_recovery_rehearsal",
)
#: Deferred to the shadow window: their units need the credstore (U-13).
CREDSTORE_ROLES: tuple[str, ...] = (
    "artifact_retention",
    "auction_match_source",
    "daily_close_source",
    "market_minute_source",
    "notifier",
    "reference_slow_publisher",
    "reference_slow_source",
)
#: No unit of their own: the HYBRID daily adapter and the arbiter-invoked probe.
UNITLESS_ROLES: tuple[str, ...] = ("daily", "workload_admission")

#: `strategy_live` is in the first-gate list and is also the one kind §10.4 keeps failing
#: closed without a runtime root. Route B publishes no `data/runtime`, so on this host — and
#: on the server — it cannot reach the service loop; its positive evidence is the exact
#: refusal, not the loop. See the PA-1 report, deviation D-3.
STRATEGY_LIVE_REFUSAL = "strategy-live runtime must use a current deployment profile"
#: What `load_current_runtime_deployment_profile` raises for the absent legacy root: on
#: Linux the pointer is simply missing; on macOS `/home` is itself a symlink and the loader
#: refuses one step earlier. Either is the profile loader speaking, before any import dies.
LEGACY_ROOT_REFUSALS = (
    "ValueError: runtime current deployment is missing",
    "ValueError: runtime root contains a symlink parent: /home",
)
RUNTIME_ROOT_WARNING = "is unavailable: schema dual write and the artifact terminal lifecycle"

WORLD_COMMIT = "2b26280cf118c54a4ae4bb495f28bc2bc849b17d"
RECORD_PREFIX = "TP9-RECORD "

#: The child's `-c` payload after `_verify`'s own source: the frozen trailer's call, with
#: the five injection-seam keywords the offline suite is allowed to pass (ruling O5), plus
#: the seams this test needs *inside* the generation interpreter. `runpy.run_module` is the
#: only hook: `child_main` has already rebuilt `sys.path` from the manifest-covered paths by
#: the time it is called, and the module about to run as `__main__` will import
#: `run_runtime_service_manifest` from the entry-point module patched here.
_TP9_TRAILER = r'''

import json as _json
import os as _os
import runpy as _runpy
import subprocess as _subprocess
import sys as _sys

_ROLE, _INSTANCE = _sys.argv[1], (_sys.argv[2] or None)
_PROFILE, _AUTHORITY, _GENERATION_ROOT, _TRUSTED_ROOT = _sys.argv[3:7]
_OWNER_UID = int(_sys.argv[7])
_CHECKOUT_SITE_PACKAGES = _sys.argv[8]
_FINALIZER_LOCK = _sys.argv[9]
_real_run_module = _runpy.run_module


def _record(**payload):
    _sys.stdout.write("TP9-RECORD " + _json.dumps(payload, sort_keys=True) + "\n")
    _sys.stdout.flush()


def _no_subprocess(*args, **kwargs):
    raise AssertionError("a role child started a subprocess: " + repr(args[:1]))


def _run_module_with_test_seams(module, **kwargs):
    # The generation in this world carries a marker site-packages only. The third-party
    # packages the roles import come from the checkout venv, appended *after*
    # `child_import_paths` has done its work, so `rquant` still resolves inside the
    # generation and the baseline check above saw exactly what production would.
    _sys.path.append(_CHECKOUT_SITE_PACKAGES)
    _subprocess.run = _no_subprocess
    _subprocess.Popen = _no_subprocess

    import rquant.runtime_service_entrypoint as _entrypoint

    def _stop_at_service_loop(
        manifest, *, registry, control_root, stop_event, max_iterations=None, clock=None
    ):
        _record(
            seam="run_runtime_service_manifest",
            service_id=manifest.service_id,
            service_kind=manifest.service_kind.value,
            producer_commit=manifest.producer_commit,
            control_root=str(control_root),
            registry_type=type(registry).__name__,
            registry_kinds=sorted(kind.value for kind in registry.registered_kinds),
            startup_degraded_reasons=list(getattr(registry, "startup_degraded_reasons", ())),
            max_iterations=max_iterations,
            stop_event_type=type(stop_event).__name__,
            cwd=_os.getcwd(),
            sys_path_head=list(_sys.path[:2]),
            module_file=_sys.modules["__main__"].__file__,
        )
        stop_event.set()
        return None

    _entrypoint.run_runtime_service_manifest = _stop_at_service_loop

    import rquant.formal_runtime_command as _command
    import rquant.formal_runtime_composition as _composition

    _bootstrap = list(_command.FINALIZER_BOOTSTRAP_ARGUMENTS)
    _bootstrap[_bootstrap.index("--deployment-lock-path") + 1] = _FINALIZER_LOCK
    _command.FINALIZER_BOOTSTRAP_ARGUMENTS = tuple(_bootstrap)

    def _stop_at_capability(**payload):
        _record(
            seam="open_formal_runtime_capability",
            configuration_path=str(payload["configuration_path"]),
            trusted_base=str(payload["trusted_base"]),
            expected_authority_uid=payload["expected_authority_uid"],
            expected_authority_gid=payload["expected_authority_gid"],
            lock_path=_FINALIZER_LOCK,
            lock_exists=_os.path.exists(_FINALIZER_LOCK),
            sys_argv=list(_sys.argv),
            cwd=_os.getcwd(),
            module_file=_sys.modules["__main__"].__file__,
        )
        raise SystemExit(0)

    _composition.open_formal_runtime_capability = _stop_at_capability
    return _real_run_module(module, **kwargs)


_runpy.run_module = _run_module_with_test_seams
raise SystemExit(
    child_main(
        _ROLE,
        _INSTANCE,
        profile_path=_PROFILE,
        authority_path=_AUTHORITY,
        generation_root=_GENERATION_ROOT,
        trusted_root=_TRUSTED_ROOT,
        expected_owner_uid=_OWNER_UID,
    )
)
'''


def _canonical(value: Any, *, newline: bool = False) -> bytes:
    from rquant.strict_json import canonical_json_bytes

    return canonical_json_bytes(value, trailing_newline=newline)


def _instance_label(role: str) -> str:
    return "svc-" + hashlib.sha256(f"tp9:{role}".encode()).hexdigest()


class AuthorityWorld:
    """A published Route-B world under one `tempfile`-derived root, mirroring the checkout.

    Layout is the one `acceptance-pra.md` E-1 fixes: `app_source` is `<gen>/src`, the whole
    `src/rquant/**` tree is copied in, `<gen>/scripts/strict_json.py` sits beside it, and
    the per-instance service manifests live in `<gen>/manifests/`. Everything is frozen
    0555/0444, hashed into the full manifest, and named by that manifest's hash.
    """

    def __init__(self, root: Path) -> None:
        self.root = root
        self.etc = root / "etc" / "rquant"
        self.var = root / "var" / "lib" / "rquant" / "runtime-authority"
        self.generation_root = self.var / "generations"
        self.profile_path = self.etc / "production-runtime-profile.json"
        self.authority_path = self.var / "current.json"
        self.finalizer_lock_dir = root / "finalizer-lock"
        self.owner_uid = os.getuid()
        self.instances: dict[str, str] = {}
        self.entries: list[dict[str, Any]] = []
        self.profile_body: dict[str, Any] = {}
        self.profile_id = ""
        self.manifest_bytes = b""
        self.generation_id = ""
        self.generation = Path()
        self.record: dict[str, Any] = {}

    # -- construction -------------------------------------------------------------

    def build(self) -> AuthorityWorld:
        from rquant.runtime_authority import PRODUCTION_ROLE_POLICY

        self.root.chmod(0o755)
        for directory in (self.etc, self.generation_root):
            directory.mkdir(parents=True)
        for directory in (
            self.root / "etc",
            self.root / "var",
            self.root / "var" / "lib",
            self.var.parent,
            self.var,
            self.etc,
            self.generation_root,
        ):
            directory.chmod(0o755)
        self.finalizer_lock_dir.mkdir(mode=0o700)
        # Pre-created: on APFS a directory's link count includes its files, so letting
        # `acquire_formal_deployment_lock` create the file would change the parent identity
        # it re-checks after opening (a Linux ext4 directory count only tracks subdirs).
        lock = self.finalizer_lock_dir / "deployment.lock"
        lock.touch(mode=0o600)
        lock.chmod(0o600)

        staging = self.generation_root / ".staging"
        self._lay_out_generation(staging, PRODUCTION_ROLE_POLICY)
        self.entries = self._freeze(staging)

        self.profile_body = self._profile_body(PRODUCTION_ROLE_POLICY)
        self.profile_id = hashlib.sha256(_canonical(self.profile_body)).hexdigest()
        self._write_root_file(
            self.profile_path,
            _canonical({**self.profile_body, "profile_id": self.profile_id}),
            0o444,
        )

        placeholder = self.generation_root / ("0" * 64)
        manifest_document = {
            "schema_id": "rquant-full-manifest/v1",
            "profile_id": self.profile_id,
            "roles": {
                entry.name: self._relative_role(entry.module) for entry in PRODUCTION_ROLE_POLICY
            },
            "entries": self.entries,
        }
        del placeholder
        self.manifest_bytes = _canonical(manifest_document, newline=True)
        self.generation_id = hashlib.sha256(self.manifest_bytes).hexdigest()
        self.generation = self.generation_root / self.generation_id
        staging.chmod(0o700)
        staging.replace(self.generation)
        self._write_root_file(self.generation / "full-manifest.json", self.manifest_bytes, 0o444)
        self.generation.chmod(0o555)

        slot = {
            "lifecycle": "active",
            "generation_id": self.generation_id,
            "generation_path": str(self.generation),
            "commit": WORLD_COMMIT,
            "full_manifest_hash": self.generation_id,
            "profile_id": self.profile_id,
            "roles": {
                entry.name: self._absolute_role(entry.module) for entry in PRODUCTION_ROLE_POLICY
            },
        }
        from rquant.runtime_exec_wrapper import _verify

        self.record = {
            "schema_version": 1,
            "operation_id": "a" * 32,
            "sequence": 1,
            "state": "active",
        }
        for field in _verify._SLOT_FIELDS:
            self.record[f"current_{field}"] = slot[field]
            self.record[f"prior_{field}"] = None
        self._write_root_file(self.authority_path, _canonical(self.record, newline=True), 0o444)
        return self

    def _lay_out_generation(self, staging: Path, policy: tuple[Any, ...]) -> None:
        (staging / "bin").mkdir(parents=True)
        (staging / "lib" / "site-packages").mkdir(parents=True)
        (staging / "cwd").mkdir()
        (staging / "manifests").mkdir()
        (staging / "bin" / "python").write_text(
            "#!/bin/sh\nexec /usr/bin/true\n", encoding="utf-8"
        )
        (staging / "lib" / "site-packages" / "_marker.py").write_text(
            "VALUE = 1\n", encoding="utf-8"
        )
        build_generation_code_tree(staging, mirror_checkout=True)
        (staging / "pyvenv.cfg").write_text(
            "home = /usr/bin\ninclude-system-site-packages = false\nversion = 3.11.15\n",
            encoding="utf-8",
        )
        for entry in policy:
            if not entry.instanced:
                continue
            label = _instance_label(entry.name)
            self.instances[entry.name] = label
            service_id = f"{entry.name.replace('_', '-')}.tp9.v1"
            if entry.service_kind:
                document: dict[str, Any] = {
                    "schema_version": 2,
                    "service_id": service_id,
                    "service_kind": entry.service_kind,
                    "plane": "live",
                    "interval_seconds": 60.0,
                    "stale_after_seconds": 900.0,
                    "producer_commit": WORLD_COMMIT,
                    "settings": {},
                }
            else:
                # page_control / runtime_recovery*: the file must exist and be covered; the
                # services never open it (spec C-12, review M-3).
                document = {"service_id": service_id, "producer_commit": WORLD_COMMIT}
            (staging / "manifests" / f"{label}.json").write_bytes(
                _canonical(document, newline=True)
            )

    @staticmethod
    def _freeze(staging: Path) -> list[dict[str, Any]]:
        for path in sorted(staging.rglob("*"), reverse=True):
            path.chmod(0o555 if path.is_dir() or path.name == "python" else 0o444)
        staging.chmod(0o555)
        entries: list[dict[str, Any]] = []
        for path in sorted(staging.rglob("*"), key=lambda p: p.relative_to(staging).as_posix()):
            relative = path.relative_to(staging).as_posix()
            info = path.lstat()
            if path.is_dir():
                entries.append(
                    {
                        "path": relative,
                        "type": "directory",
                        "owner_uid": info.st_uid,
                        "mode": stat.S_IMODE(info.st_mode),
                        "nlink": info.st_nlink,
                        "size": 0,
                        "sha256": None,
                    }
                )
                continue
            payload = path.read_bytes()
            entries.append(
                {
                    "path": relative,
                    "type": "file",
                    "owner_uid": info.st_uid,
                    "mode": stat.S_IMODE(info.st_mode),
                    "nlink": info.st_nlink,
                    "size": len(payload),
                    "sha256": hashlib.sha256(payload).hexdigest(),
                }
            )
        return entries

    def _profile_body(self, policy: tuple[Any, ...]) -> dict[str, Any]:
        from rquant.runtime_authority import (
            PRODUCTION_ALLOWED_OPERATIONS,
            PRODUCTION_MANIFEST_SCHEMA,
        )

        def file_policy(path: str, digest: str, mode: int) -> dict[str, Any]:
            return {"path": path, "sha256": digest, "owner_uid": 0, "mode": mode}

        closure = [
            "/usr/bin/python3.11",
            "/usr/lib64/ld-linux-x86-64.so.2",
            "/usr/lib64/python3.11/os.py",
            "/usr/lib64/libpython3.11.so.1.0",
            "/usr/local/libexec/rquant-production-deploy.pyz",
            "/usr/local/libexec/rquant-runtime-exec.pyz",
        ]
        ancestors = sorted({str(parent) for item in closure for parent in Path(item).parents})
        return {
            "schema_version": 1,
            "platform": "linux",
            "ancestors": [{"path": p, "owner_uid": 0, "mode": 0o755} for p in ancestors],
            "system_python": file_policy("/usr/bin/python3.11", "0" * 64, 0o555),
            "elf_loader": file_policy("/usr/lib64/ld-linux-x86-64.so.2", "1" * 64, 0o555),
            "stdlib": [file_policy("/usr/lib64/python3.11/os.py", "2" * 64, 0o444)],
            "shared_libraries": [
                file_policy("/usr/lib64/libpython3.11.so.1.0", "3" * 64, 0o555)
            ],
            "deploy_pyz": file_policy(
                "/usr/local/libexec/rquant-production-deploy.pyz", "4" * 64, 0o555
            ),
            "runtime_pyz": file_policy(
                "/usr/local/libexec/rquant-runtime-exec.pyz", "5" * 64, 0o555
            ),
            "inbox_root": "/var/lib/rquant/runtime-authority/inbox",
            "quarantine_root": "/var/lib/rquant/runtime-authority/quarantine",
            "generation_root": str(self.generation_root),
            "allowed_operations": list(PRODUCTION_ALLOWED_OPERATIONS),
            "roles": {
                entry.name: {
                    "module": entry.module,
                    "environment_allowlist": list(entry.environment_allowlist),
                    "instances": [self.instances[entry.name]] if entry.instanced else [],
                    "service_kind": entry.service_kind,
                    "control_root": entry.control_root,
                    "once": entry.once,
                    "module_arguments": list(entry.module_arguments),
                }
                for entry in policy
            },
            "manifest_schema": {
                key: (list(value) if isinstance(value, tuple) else value)
                for key, value in PRODUCTION_MANIFEST_SCHEMA.items()
            },
        }

    @staticmethod
    def _relative_role(module: str) -> dict[str, Any]:
        return {
            "python_path": "bin/python",
            "module": module,
            "working_directory": "cwd",
            "app_source": "src",
            "site_packages": ["lib/site-packages"],
        }

    def _absolute_role(self, module: str) -> dict[str, Any]:
        return {
            "python_path": str(self.generation / "bin" / "python"),
            "module": module,
            "working_directory": str(self.generation / "cwd"),
            "app_source": str(self.generation / "src"),
            "site_packages": [str(self.generation / "lib" / "site-packages")],
        }

    @staticmethod
    def _write_root_file(path: Path, payload: bytes, mode: int) -> None:
        parent_mode = stat.S_IMODE(path.parent.stat().st_mode)
        path.parent.chmod(0o755)
        path.write_bytes(payload)
        path.chmod(mode)
        path.parent.chmod(parent_mode)

    # -- driving ------------------------------------------------------------------

    def resolve(self, role: str) -> dict[str, Any]:
        from rquant.runtime_exec_wrapper import _verify

        return _verify.resolve_launch(
            role,
            instance=self.instances.get(role),
            profile_path=str(self.profile_path),
            authority_path=str(self.authority_path),
            generation_root=str(self.generation_root),
            trusted_root=str(self.root),
            expected_owner_uid=self.owner_uid,
            source_environment={"LANG": "C", "TZ": "UTC", "SECRET": "leak"},
        )

    def launch(self, role: str) -> subprocess.CompletedProcess[str]:
        """Start the role exactly as the wrapper's child would, through `child_main`."""

        from rquant.runtime_exec_wrapper import _verify

        body = _verify.frozen_bootstrap()[: -len(_verify.CHILD_TRAILER)]
        return subprocess.run(
            [
                sys.executable,
                "-I",
                "-S",
                "-c",
                body + _TP9_TRAILER,
                role,
                self.instances.get(role) or "",
                str(self.profile_path),
                str(self.authority_path),
                str(self.generation_root),
                str(self.root),
                str(self.owner_uid),
                site_packages_path(),
                str(self.finalizer_lock_dir / "deployment.lock"),
            ],
            cwd=str(self.root),
            env=child_environment(),
            capture_output=True,
            text=True,
            check=False,
            timeout=600,
        )

    def unfreeze(self) -> None:
        for path in sorted(self.root.rglob("*"), reverse=True):
            if path.is_dir() and not path.is_symlink():
                path.chmod(0o755)
            elif not path.is_symlink():
                path.chmod(0o644)


def _records(completed: subprocess.CompletedProcess[str]) -> list[dict[str, Any]]:
    return [
        json.loads(line[len(RECORD_PREFIX) :])
        for line in completed.stdout.splitlines()
        if line.startswith(RECORD_PREFIX)
    ]


@pytest.fixture(scope="module")
def authority_world(tmp_path_factory: pytest.TempPathFactory) -> AuthorityWorld:
    world = AuthorityWorld(tmp_path_factory.mktemp("tp9-authority")).build()
    yield world  # type: ignore[misc]
    world.unfreeze()


@pytest.fixture(scope="module")
def child_outcomes(
    authority_world: AuthorityWorld,
) -> dict[str, subprocess.CompletedProcess[str]]:
    """Every first-gate and legacy-root role, started as a wrapper child, in parallel."""

    roles = (*FIRST_GATE_ROLES, *LEGACY_ROOT_ROLES)
    with ThreadPoolExecutor(max_workers=6) as pool:
        completed = list(pool.map(authority_world.launch, roles))
    return dict(zip(roles, completed, strict=True))


def test_first_gate_partition_covers_the_whole_policy() -> None:
    from rquant.runtime_authority import PRODUCTION_ROLE_POLICY

    groups = (FIRST_GATE_ROLES, LEGACY_ROOT_ROLES, CREDSTORE_ROLES, UNITLESS_ROLES)
    assert len(FIRST_GATE_ROLES) == 16
    assert sum(len(group) for group in groups) == 28
    assert set().union(*groups) == {entry.name for entry in PRODUCTION_ROLE_POLICY}


def test_t9_10_the_world_resolves_every_role_out_of_the_mirrored_layout(
    authority_world: AuthorityWorld,
) -> None:
    """T9-10 (wrapper side): 28/28 resolve, module source is `src/rquant/<leaf>.py`."""

    from rquant.runtime_authority import PRODUCTION_ROLE_POLICY

    covered = {entry["path"] for entry in authority_world.entries if entry["type"] == "file"}
    assert "src/rquant/__init__.py" in covered
    assert "src/rquant/strict_json.py" in covered
    assert "scripts/strict_json.py" in covered
    assert "pyvenv.cfg" in covered

    for entry in PRODUCTION_ROLE_POLICY:
        launch = authority_world.resolve(entry.name)
        leaf = entry.module.rsplit(".", 1)[-1]
        assert launch["module_source"] == f"src/rquant/{leaf}.py", entry.name
        assert launch["module_source"] in covered
        assert launch["app_source"] == str(authority_world.generation / "src")
        argv = launch["module_argv"]
        if entry.service_kind:
            assert argv[-1] == "--authority-runtime", entry.name
            assert argv[argv.index("--expected-commit") + 1] == WORLD_COMMIT
            assert argv[argv.index("--control-root") + 1] == (
                f"{entry.control_root}/{authority_world.instances[entry.name]}"
            )
        else:
            assert "--authority-runtime" not in argv, entry.name


def test_t9_10_the_mirrored_layout_passes_the_publish_side_generation_checks(
    authority_world: AuthorityWorld,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """T9-10 (publisher side): full manifest, tree and semantics — including the
    namespace-package rule for `src/rquant/__init__.py` and the U-1-R pyvenv rules."""

    import rquant.runtime_authority as authority_module

    monkeypatch.setattr(
        authority_module, "PRODUCTION_GENERATION_ROOT", authority_world.generation_root
    )
    monkeypatch.setattr(
        authority_module, "RUNTIME_AUTHORITY_OWNER_UID", authority_world.owner_uid
    )

    profile = authority_module.parse_runtime_closure_profile(
        authority_world.profile_path.read_bytes()
    )
    assert profile.profile_id == authority_world.profile_id
    slot = authority_module._parse_slot(authority_world.record, prefix="current")
    entries = authority_module._validate_generation_manifest(
        authority_world.manifest_bytes, slot, profile
    )
    assert {entry.path for entry in entries} == {entry["path"] for entry in authority_world.entries}

    generation_fd = os.open(
        authority_world.generation, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
    )
    try:
        authority_module._validate_generation_tree(generation_fd, entries, profile)
        authority_module._validate_generation_semantics(generation_fd, entries, slot, profile)
    finally:
        os.close(generation_fd)


def _policy_entry(role: str) -> Any:
    from rquant.runtime_authority import PRODUCTION_ROLE_POLICY

    return next(entry for entry in PRODUCTION_ROLE_POLICY if entry.name == role)


def _expect_degraded() -> bool:
    return not PRODUCTION_RUNTIME_ROOT.is_dir()


@pytest.mark.parametrize(
    "role",
    [role for role in FIRST_GATE_ROLES if role not in ("lab_claim_finalizer", "strategy_live")],
)
def test_t9_2_first_gate_service_role_reaches_its_service_loop(
    role: str,
    authority_world: AuthorityWorld,
    child_outcomes: dict[str, subprocess.CompletedProcess[str]],
) -> None:
    """T9-2: `main()` -> `run()` -> `run_runtime_service_manifest(manifest, registry, …)`.

    Positive evidence, not the absence of exit 78: the child prints the three arguments at
    the loop boundary, the test's seam sends the stop signal there, and the process leaves
    with 0. The oneshot orchestrator arrives with `max_iterations=1`; the fourteen resident
    roles arrive with `None`.
    """

    completed = child_outcomes[role]
    entry = _policy_entry(role)
    assert completed.returncode == 0, completed.stderr[-4000:]
    assert "validation errors for Settings" not in completed.stderr
    (record,) = _records(completed)

    assert record["seam"] == "run_runtime_service_manifest"
    assert record["service_kind"] == entry.service_kind
    assert record["producer_commit"] == WORLD_COMMIT
    assert record["control_root"] == f"{entry.control_root}/{authority_world.instances[role]}"
    assert entry.service_kind in record["registry_kinds"]
    assert record["stop_event_type"] == "Event"
    assert record["max_iterations"] == (1 if entry.once else None)
    assert record["cwd"] == str(authority_world.generation / "cwd")
    assert record["sys_path_head"][0] == str(authority_world.generation / "src")
    assert record["module_file"] == str(
        authority_world.generation / "src" / "rquant" / "runtime_service_main.py"
    )
    if _expect_degraded():
        assert record["registry_type"] == "_StartupDegradedRegistry"
        assert record["startup_degraded_reasons"] == ["runtime_root_unavailable"]
        assert RUNTIME_ROOT_WARNING in completed.stderr
        assert str(PRODUCTION_RUNTIME_ROOT) in completed.stderr
    else:  # pragma: no cover - only on a host that has the legacy runtime root
        assert record["startup_degraded_reasons"] == []


def test_t9_2_the_oneshot_orchestrator_is_the_only_once_role_in_the_first_gate() -> None:
    entries = [_policy_entry(role) for role in FIRST_GATE_ROLES]
    once = {entry.name for entry in entries if entry.once}
    assert once == {"daily_pipeline_orchestrator"}
    assert _policy_entry("artifact_retention").once is True
    assert "artifact_retention" in CREDSTORE_ROLES


def test_t9_2_the_finalizer_reaches_its_capability_open_holding_the_deployment_lock(
    authority_world: AuthorityWorld,
    child_outcomes: dict[str, subprocess.CompletedProcess[str]],
) -> None:
    """`rquant-lab-claim-finalizer`: `main()` parses the frozen literal into the fixed
    bootstrap binding, takes the deployment lock, and asks for the runtime-code capability
    with the root-owned paths — the first step that needs the real host, where the seam stops it."""

    completed = child_outcomes["lab_claim_finalizer"]
    assert completed.returncode == 0, completed.stderr[-4000:]
    (record,) = _records(completed)

    assert record["seam"] == "open_formal_runtime_capability"
    assert record["sys_argv"][1:] == ["lab-claim-finalizer"]
    assert record["configuration_path"] == "/etc/rquant/runtime-code-bootstrap.json"
    assert record["trusted_base"] == "/etc/rquant"
    assert record["expected_authority_uid"] == 0
    assert record["expected_authority_gid"] == 0
    assert record["lock_exists"] is True
    assert record["cwd"] == str(authority_world.generation / "cwd")
    assert record["module_file"] == str(
        authority_world.generation / "src" / "rquant" / "lab_formal_runtime_entry.py"
    )
    assert (authority_world.finalizer_lock_dir / "deployment.lock").is_file()


def test_t9_2_strategy_live_fails_closed_without_the_legacy_runtime_root(
    child_outcomes: dict[str, subprocess.CompletedProcess[str]],
) -> None:
    """Deviation D-3: in the first-gate list, but §10.4 keeps it hard-failing without a root."""

    completed = child_outcomes["strategy_live"]
    if not _expect_degraded():  # pragma: no cover - only on a host with the legacy root
        pytest.fail("this host has the legacy runtime root; the refusal cannot be observed")
    assert completed.returncode != 0
    assert STRATEGY_LIVE_REFUSAL in completed.stderr
    assert "validation errors for Settings" not in completed.stderr
    assert _records(completed) == []
    # It got past manifest admission and the runtime-root decision before refusing.
    assert RUNTIME_ROOT_WARNING not in completed.stderr


@pytest.mark.parametrize("role", LEGACY_ROOT_ROLES)
def test_t9_3_legacy_root_roles_resolve_but_refuse_identifiably_without_data_runtime(
    role: str,
    authority_world: AuthorityWorld,
    child_outcomes: dict[str, subprocess.CompletedProcess[str]],
) -> None:
    """T9-3: profile and placeholder manifest carry them through `resolve_launch`; their
    entry then raises a recognisable `ValueError` for the missing `data/runtime/current`
    rather than dying on an import or going quiet."""

    launch = authority_world.resolve(role)
    assert launch["service_manifest"] == str(
        authority_world.generation / "manifests" / f"{authority_world.instances[role]}.json"
    )

    completed = child_outcomes[role]
    if not _expect_degraded():  # pragma: no cover - only on a host with the legacy root
        pytest.fail("this host has the legacy runtime root; the refusal cannot be observed")
    assert completed.returncode != 0
    assert any(refusal in completed.stderr for refusal in LEGACY_ROOT_REFUSALS), (
        completed.stderr[-2000:]
    )
    assert "load_current_runtime_deployment_profile" in completed.stderr
    assert "validation errors for Settings" not in completed.stderr
    assert "ModuleNotFoundError" not in completed.stderr
    assert _records(completed) == []
