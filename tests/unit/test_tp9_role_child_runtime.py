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
