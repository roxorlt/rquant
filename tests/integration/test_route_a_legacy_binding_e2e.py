"""R207 acceptance: a real bundle, a real staged generation, two real roles in their loops.

Route A restores `<runtime root>/current`, which puts every kind-backed role on the
non-degraded branch and straight into `load_runtime_schema_service_bindings`. The bug this
file is the acceptance for (#207, #187) is that the role handed that loader the authority
generation id while the loader wanted the legacy deployment hash — two id namespaces that
never agree — so all 15 roles failed closed with
`runtime schema service generation is not current`.

`#200` reached the production host because its acceptance was a stub, so nothing here is
stubbed on the path under test:

* `install_runtime_deployment_bundle` really installs a legacy runtime root, with a real
  relative `current -> generations/<64 hex>` symlink, a real `deployment-profile.json`, and
  the real schema bundle the binding loader reads;
* `runtime-authority-stage --legacy-runtime-root` really copies that generation's service
  manifests, really writes `legacy-binding.json`, and the staging is really published into
  a root-owned authority chain;
* the wrapper's own `resolve_launch` derives the argv, hashing the generation's full
  manifest against the chain slot and verifying every manifested entry on disk — including
  the binding document — before anything runs;
* `runtime_service_main.run()` then executes with exactly that argv, loading schema
  bindings through the real `runtime_deployment_bundle` loader, and enters the real service
  loop for `serving_publisher` and `paper_constraint_publisher`.

The only seams are the ones the authority suites already own: the module constants that
name `/etc/rquant`, `/var/lib/rquant` and the system interpreter (`World`), the `os.stat`
hook that lets a non-root test own a 0444 keyring, and the two credential-sealing calls
that need `systemd-creds` under sudo. The production profile itself is the real one, moved
whole onto a temporary runtime root by `_relocate` and revalidated by its own model.
"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import sys
from pathlib import Path
from threading import Event
from typing import Any

import pytest

import rquant.runtime_service_main as service_main
from rquant.runtime_exec_wrapper import _verify
from rquant.runtime_legacy_generation_binding import (
    GENERATION_LEGACY_BINDING_NAME,
    legacy_generation_binding_bytes,
    parse_legacy_generation_binding,
)
from tests.unit.test_runtime_authority_publish import World

pytestmark = pytest.mark.integration

#: The two roles the ruling names. One is a serving-plane singleton whose builder reads six
#: owner authorities; the other is a live-plane publisher with a reference registry and a
#: minute spool. Between them they cover both planes a Route A host runs today.
SERVING_ROLE = "serving_publisher"
PAPER_ROLE = "paper_constraint_publisher"

NOT_CURRENT = "runtime schema service generation is not current"

#: The frozen runtime owner root every `PRODUCTION_ROLE_POLICY` control root sits under.
PRODUCTION_ROOT = Path("/home/lighthouse/rquant/data/runtime")


def _relocate(inputs: Any, *, runtime_root: Path) -> Any:
    """The same inputs with the runtime root and its data parent moved somewhere else.

    Whole-document, so the recovery bindings, the control paths and the external authority
    roots stay consistent with each other — the recovery block insists its backup source is
    the runtime root's own parent, so both prefixes move together. The model revalidates
    the result, which is what says the move did not break one of its cross-field rules.
    """

    document = json.dumps(inputs.model_dump(mode="json"))
    document = document.replace(str(inputs.runtime_root), str(runtime_root))
    document = document.replace(str(inputs.runtime_root.parent), str(runtime_root.parent))
    moved = json.loads(document)
    #: the recovery block hashes its own content, so the moved paths change the digest and
    #: the model recomputes it rather than being handed a stale one
    moved["recovery"].pop("profile_generation", None)
    return type(inputs).model_validate(moved)


def _production_bundle(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    producer_commit: str,
    runtime_root: Path | None = None,
) -> tuple[Any, Any, Any]:
    """Install a real production deployment bundle at a temporary runtime root.

    The recipe is `test_runtime_production_profile`'s
    `test_every_owned_production_manifest_builds_through_the_builtin_registry`, with the
    producer commit moved onto the authority chain's checkout so the staged manifests carry
    the commit the wrapper will forward. Two seams stay: sealing runtime credentials needs
    `systemd-creds` under sudo, which no test can have, and it is not on the path under
    test.
    """

    import base64
    import subprocess
    from datetime import UTC, date, datetime

    import pandas as pd

    from rquant.reference_data_registry import ReferenceRegistry
    from rquant.runtime_definition_bootstrap import plan_builtin_definitions
    from rquant.runtime_deployment_profile import install_runtime_deployment_profile
    from rquant.runtime_market_session import MarketCalendarAuthority
    from rquant.runtime_production_profile import (
        ProductionStrategyBinding,
        build_production_runtime_profile,
        install_production_runtime_prerequisites,
    )
    from rquant.runtime_service_entrypoint import RuntimeServiceKind
    from tests.unit.test_runtime_production_profile import _inputs, _retention_writer_capability

    class _NoCredentialRecovery:
        outcome = "none"
        transaction_id = None

    class _NoCredentialTransaction:
        def commit(self) -> None:
            pass

        def rollback(self) -> None:
            pass

    authority = MarketCalendarAuthority.create(
        schema_version=1,
        exchange="SSE",
        producer_commit=producer_commit,
        coverage_start=date(2026, 1, 1),
        coverage_end=date(2026, 12, 31),
        open_dates=(date(2026, 8, 3),),
        generated_at=datetime(2025, 12, 31, 8, tzinfo=UTC),
    )
    inputs = _inputs(tmp_path)
    inputs.historical_minutes_snapshot_path.parent.mkdir(parents=True)
    pd.DataFrame(
        columns=(
            "ts_code",
            "trade_time",
            "available_at",
            "open",
            "high",
            "low",
            "close",
            "vol",
            "amount",
        )
    ).to_parquet(inputs.historical_minutes_snapshot_path, index=False)
    bindings = tuple(
        ProductionStrategyBinding.model_validate(binding.model_dump(mode="python"))
        for binding in plan_builtin_definitions(producer_commit=producer_commit).strategies
    )
    inputs = inputs.model_copy(
        update={
            "producer_commit": producer_commit,
            "market_calendar_producer_commit": producer_commit,
            "market_calendar_content_sha256": authority.content_sha256,
            "historical_minutes_snapshot_id": hashlib.sha256(
                inputs.historical_minutes_snapshot_path.read_bytes()
            ).hexdigest(),
            "strategies": bindings,
        }
    )
    if runtime_root is not None:
        inputs = _relocate(inputs, runtime_root=runtime_root)
        assert inputs.runtime_root == runtime_root
    inputs.market_calendar_authority_path.parent.mkdir(parents=True)
    inputs.market_calendar_authority_path.write_text(
        authority.model_dump_json(), encoding="utf-8"
    )
    inputs.market_calendar_authority_path.chmod(0o600)
    install_production_runtime_prerequisites(inputs)
    profile = build_production_runtime_profile(inputs)

    private_key = tmp_path / "reference-source-ed25519"
    subprocess.run(
        ("ssh-keygen", "-q", "-t", "ed25519", "-N", "", "-f", str(private_key)), check=True
    )
    capabilities = {
        "TUSHARE_TOKEN_MAIN": "test-token",
        "PUSHDEER_KEYS": "pushdeer-test",
        "PUSHPLUS_TOKENS": "pushplus-test",
        "RQ_REFERENCE_PUBLICATION_HMAC_KEY_ID": "reference-publication-v1",
        "RQ_REFERENCE_PUBLICATION_HMAC_SECRET_HEX": "ab" * 32,
        "RQ_REFERENCE_SOURCE_SIGNING_KEY_ID": "reference-source-v1",
        "RQ_REFERENCE_SOURCE_PRIVATE_KEY_BASE64": base64.b64encode(
            private_key.read_bytes()
        ).decode("ascii"),
        "RQ_REFERENCE_SOURCE_PUBLIC_KEY": private_key.with_suffix(".pub")
        .read_text(encoding="ascii")
        .strip(),
        "RQ_ARTIFACT_RETENTION_WRITER_CREDENTIAL": _retention_writer_capability(),
    }
    monkeypatch.setattr(
        "rquant.runtime_deployment_bundle._recover_runtime_credentials",
        lambda **_kwargs: _NoCredentialRecovery(),
    )
    monkeypatch.setattr(
        "rquant.runtime_deployment_bundle._seal_runtime_credentials",
        lambda _credentials: _NoCredentialTransaction(),
    )
    receipt = install_runtime_deployment_profile(
        profile,
        runtime_root=inputs.runtime_root,
        environ=capabilities,
        schema_bootstrap_reason="route A legacy binding acceptance",
    )
    #: the paper constraint builder opens this registry while it is being built, so the
    #: file has to exist before the role starts — on the host the reference-slow publisher
    #: creates it. Empty is enough: the step's own failure is not what is under test.
    constraint = next(
        manifest
        for manifest in profile.manifests
        if manifest.service_kind is RuntimeServiceKind.PAPER_CONSTRAINT_PUBLISHER
    )
    registry_path = Path(str(constraint.settings["reference_registry_path"]))
    registry_path.parent.mkdir(parents=True, exist_ok=True)
    ReferenceRegistry(registry_path)
    return inputs, profile, receipt


class _StopAfterOneIteration(Event):
    """Let the loop run exactly one step, then stop without waiting out the interval.

    A pre-set event would prove the loop was entered but never that a step ran; waiting the
    manifest's real interval would make the case a sleep. So the first `is_set()` says no,
    and the loop's own post-iteration `wait()` is what sets the flag.
    """

    def __init__(self) -> None:
        super().__init__()
        self.iterations = 0

    def is_set(self) -> bool:
        return super().is_set() or self.iterations > 0

    def wait(self, timeout: float | None = None) -> bool:  # noqa: ARG002 - the point is not to
        self.iterations += 1
        self.set()
        return True


class RouteAWorld:
    """A `World`, plus the legacy `data/runtime` a real bundle install leaves behind."""

    def __init__(self, world: World, runtime_root: Path) -> None:
        self.world = world
        self.runtime_root = runtime_root
        self.profile: Any = None
        self.receipt: Any = None
        self.plan: Any = None

    # -- construction -------------------------------------------------------------

    def stage_and_publish(self, name: str = "route-a", **overrides: Any) -> Any:
        self.plan = self.world.stage(
            name,
            bootstrap_from_checkout=False,
            legacy_runtime_root=self.runtime_root,
            **overrides,
        )
        self.world.publish(self.plan)
        return self.plan

    # -- driving ------------------------------------------------------------------

    def instance_of(self, role: str) -> str:
        labels = self.world.instances(self.plan)[role]
        assert len(labels) == 1, role
        return labels[0]

    def wrapper_argv(self, role: str) -> list[str]:
        """Exactly what the wrapper would exec, derived out of the published chain."""

        launch = self.world.resolve(role, self.instance_of(role))
        return list(launch["module_argv"])

    def argv(self, role: str) -> list[str]:
        """The wrapper's argv with `--control-root` moved onto this world's runtime root.

        `PRODUCTION_ROLE_POLICY` freezes every control root as a literal under
        `/home/lighthouse/rquant/data/runtime`, and the role derives its runtime root from
        that path by arithmetic (`runtime_root_from_control_root`). A test cannot own
        `/home/lighthouse`, so the frozen prefix is replaced by this world's runtime root —
        the same move `_relocate` makes on the profile inputs, and the only one on this
        path. `test_the_remap_is_only_the_frozen_control_root_prefix` pins that it changes
        nothing else, and the Linux gate below runs the argv verbatim.
        """

        argv = self.wrapper_argv(role)
        index = argv.index("--control-root") + 1
        argv[index] = str(self.runtime_root / Path(argv[index]).relative_to(PRODUCTION_ROOT))
        return argv

    def run_role(self, role: str, *, argv: list[str] | None = None) -> int:
        """`runtime_service_main.run()`, entering the loop for exactly one iteration."""

        arguments = service_main.build_parser().parse_args(argv or self.argv(role))
        stop = _StopAfterOneIteration()
        real_event = service_main.Event
        service_main.Event = lambda: stop  # type: ignore[assignment]
        try:
            code = service_main.run(arguments)
        finally:
            service_main.Event = real_event  # type: ignore[assignment]
        assert stop.iterations == 1, "the service loop was never entered"
        return code

    def generation_document(self, name: str) -> Path:
        return self.world.generation_path(self.plan) / name

    def rewrite_generation_document(self, name: str, payload: bytes) -> None:
        """Replace one 0444 root-owned file inside the published generation."""

        path = self.generation_document(name)
        directory = path.parent
        directory.chmod(0o755)
        try:
            path.chmod(0o644)
            path.write_bytes(payload)
            path.chmod(0o444)
        finally:
            directory.chmod(0o555)


def _route_a_world(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, *, runtime_root: Path | None = None
) -> RouteAWorld:
    world = World(tmp_path / "root", monkeypatch).build()
    inputs, profile, receipt = _production_bundle(
        tmp_path, monkeypatch, producer_commit=world.commit, runtime_root=runtime_root
    )
    route = RouteAWorld(world, inputs.runtime_root)
    route.profile = profile
    route.receipt = receipt
    return route


@pytest.fixture
def route_a(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> RouteAWorld:
    """A published Route A chain over a really installed production deployment."""

    route = _route_a_world(tmp_path, monkeypatch)
    route.stage_and_publish()
    return route


# ---------------------------------------------------------------------------------------
# The world itself is real: assert that before asserting anything about the roles
# ---------------------------------------------------------------------------------------


def test_the_legacy_root_is_a_real_installed_bundle(route_a: RouteAWorld) -> None:
    """`current` is a relative symlink into `generations/<64 hex>`, beside a real profile."""

    current = route_a.runtime_root / "current"
    assert current.is_symlink()
    target = Path(os.readlink(current))
    assert not target.is_absolute()
    assert target.parts[0] == "generations"
    assert len(target.parts) == 2
    assert target.parts[1] == route_a.receipt.generation_hash
    assert (route_a.runtime_root / target).is_dir()
    assert (current / "deployment-profile.json").is_file()
    assert (current / "manifests").is_dir()
    installed = sorted(path.name for path in (current / "manifests").glob("svc-*.json"))
    assert len(installed) == len(route_a.profile.manifests)
    assert len(installed) > 20


def test_the_staged_generation_copies_the_bundle_manifests_and_names_its_generation(
    route_a: RouteAWorld,
) -> None:
    """The authority generation's manifests are the legacy bundle's own bytes, and the
    document beside them names the deployment they came out of."""

    generation = route_a.world.generation_path(route_a.plan)
    legacy = route_a.runtime_root / "current" / "manifests"
    staged = {path.name for path in (generation / "manifests").glob("svc-*.json")}
    installed = {path.name for path in legacy.glob("svc-*.json")}
    #: the staged generation adds the orphan roles the bundle never holds (page control and
    #: the recovery units), and copies every bundle manifest byte for byte
    from rquant.runtime_authority import PRODUCTION_ROLE_POLICY

    mapping = route_a.world.instances(route_a.plan)
    orphans = {
        f"{label}.json"
        for entry in PRODUCTION_ROLE_POLICY
        if entry.instanced and not entry.service_kind
        for label in mapping.get(entry.name, ())
    }
    assert orphans
    assert installed < staged
    assert staged - installed == orphans
    for name in sorted(installed):
        assert (generation / "manifests" / name).read_bytes() == (legacy / name).read_bytes()

    binding = parse_legacy_generation_binding(
        (generation / GENERATION_LEGACY_BINDING_NAME).read_bytes()
    )
    assert binding.mode == "legacy"
    assert binding.runtime_root == str(route_a.runtime_root)
    assert binding.generation_id == route_a.receipt.generation_hash
    assert binding.generation_id != route_a.plan.plan["generation_id"]


def test_the_wrapper_verifies_the_binding_document_as_part_of_the_code_identity(
    route_a: RouteAWorld,
) -> None:
    """The document is hash-bound: it is a manifested entry, and tampering is refused before
    any role process could read it."""

    entries = {
        str(entry["path"]): entry
        for entry in json.loads(
            (route_a.world.generation_path(route_a.plan) / "full-manifest.json").read_bytes()
        )["entries"]
    }
    assert GENERATION_LEGACY_BINDING_NAME in entries
    assert entries[GENERATION_LEGACY_BINDING_NAME]["type"] == "file"

    route_a.argv(SERVING_ROLE)  # the untampered chain resolves
    route_a.rewrite_generation_document(
        GENERATION_LEGACY_BINDING_NAME,
        legacy_generation_binding_bytes(
            mode="legacy",
            runtime_root=str(route_a.runtime_root),
            generation_id="d" * 64,
        ),
    )
    with pytest.raises(
        _verify.RuntimeExecError, match="manifested generation node changed: legacy-binding"
    ):
        route_a.argv(SERVING_ROLE)


# ---------------------------------------------------------------------------------------
# The acceptance: both roles reach their service loop over a real `current`
# ---------------------------------------------------------------------------------------


@pytest.mark.parametrize("role", [SERVING_ROLE, PAPER_ROLE])
def test_a_kind_backed_role_reaches_its_service_loop_over_a_real_current(
    route_a: RouteAWorld, role: str
) -> None:
    """The bug, gone: the role loads schema bindings and enters `run_runtime_service_manifest`.

    The stop event is pre-set, so the loop is entered and left without executing a step —
    what is under test is the binding, and a step failure would say nothing about it either
    way. `run()` returning 0 means the schema bindings loaded, the registry was built and
    the loop ran.
    """

    assert route_a.run_role(role) == 0


@pytest.mark.parametrize("role", [SERVING_ROLE, PAPER_ROLE])
def test_the_loader_is_given_the_legacy_id_and_returns_real_bindings(
    route_a: RouteAWorld, role: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Not "no exception": the real loader is called with the legacy deployment hash, and it
    is the real one — the observer wraps it rather than replacing it."""

    observed: dict[str, object] = {}
    real = service_main.load_runtime_schema_service_bindings

    def observe(runtime_root: Path, **kwargs: Any) -> Any:
        observed["runtime_root"] = runtime_root
        observed["generation_id"] = kwargs["generation_id"]
        return real(runtime_root, **kwargs)

    monkeypatch.setattr(service_main, "load_runtime_schema_service_bindings", observe)
    assert route_a.run_role(role) == 0
    assert observed["runtime_root"] == route_a.runtime_root
    assert observed["generation_id"] == route_a.receipt.generation_hash
    assert observed["generation_id"] != route_a.plan.plan["generation_id"]


def test_the_control_tree_the_loop_leaves_behind_is_under_the_legacy_root(
    route_a: RouteAWorld,
) -> None:
    """Evidence the loop really ran: its own control directory and heartbeat exist."""

    instance = route_a.instance_of(SERVING_ROLE)
    assert route_a.run_role(SERVING_ROLE) == 0
    control = route_a.runtime_root / "control" / _control_directory(SERVING_ROLE) / instance
    assert control.is_dir()
    assert (control / "heartbeats").is_dir()
    assert (control / "locks").is_dir()


# ---------------------------------------------------------------------------------------
# The reverse cases the ruling asks for, each on the same real world
# ---------------------------------------------------------------------------------------


def test_a_current_pointer_moved_to_a_copied_generation_is_refused(
    route_a: RouteAWorld,
) -> None:
    """The pointer moved, so the generation's claim about its deployment is no longer true.

    A `copytree` copy is the cheap version of the case: it is refused by the binding, but
    the schema loader would have caught it too, because a copied `generation-basis.json`
    hashes to the directory it came from and not to the one it now sits in. The case below
    is the one only the binding can see.
    """

    other = "b" * 64
    shutil.copytree(
        route_a.runtime_root / "generations" / route_a.receipt.generation_hash,
        route_a.runtime_root / "generations" / other,
    )
    _swing_current(route_a, other)

    with pytest.raises(ValueError, match="does not match the current pointer"):
        route_a.run_role(SERVING_ROLE)


def _swing_current(route_a: RouteAWorld, generation: str) -> None:
    current = route_a.runtime_root / "current"
    current.unlink()
    current.symlink_to(Path("generations") / generation, target_is_directory=True)


@pytest.fixture
def recorded_install(monkeypatch: pytest.MonkeyPatch) -> tuple[dict[str, Any], Any]:
    """Capture the real install call so a case can repeat it verbatim."""

    import rquant.runtime_deployment_profile as profile_module

    captured: dict[str, Any] = {}
    real = profile_module.install_runtime_deployment_profile

    def recorder(profile: Any, **kwargs: Any) -> Any:
        captured.update(kwargs)
        captured["profile"] = profile
        return real(profile, **kwargs)

    monkeypatch.setattr(profile_module, "install_runtime_deployment_profile", recorder)
    return captured, real


def _second_install(captured: dict[str, Any], real: Any) -> str:
    """A second real install on the same runtime root: one rotated notify capability.

    The schema registry is already bootstrapped, so no bootstrap reason may be passed.
    Service manifests carry no capability values, so they come out byte-identical while
    `generation-basis.json` — and with it the legacy generation id — changes.
    """

    kwargs = dict(captured)
    profile = kwargs.pop("profile")
    kwargs.pop("schema_bootstrap_reason", None)
    environ = dict(kwargs["environ"])
    environ["PUSHDEER_KEYS"] = "pushdeer-rotated"
    kwargs["environ"] = environ
    return real(profile, **kwargs).generation_hash


def test_a_sibling_generation_with_the_same_manifests_is_refused(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, recorded_install: tuple[dict[str, Any], Any]
) -> None:
    """`current` swung to a *legitimately installed* sibling — the case only the document sees.

    A second real `install_runtime_deployment_profile` on the same root (here: one rotated
    notify credential, an ordinary redeploy) produces a generation whose `manifests/` are
    byte-for-byte the first one's, because manifests carry no capability values, while
    `generation-basis.json` — and with it the generation id — differs. Every check the
    schema loader makes then passes on the sibling: its basis hashes to its own directory
    name, its manifest fingerprints and producer commit are the same ones, and
    `_current_target(root)` agrees with the id it was handed. `runtime_service_main.py`'s
    cross-check is the only thing that knows the authority generation was staged from the
    other one.
    """

    captured, real = recorded_install
    route = _route_a_world(tmp_path, monkeypatch)
    first = route.receipt.generation_hash
    route.stage_and_publish()
    second = _second_install(captured, real)
    assert second != first

    left = route.runtime_root / "generations" / first / "manifests"
    right = route.runtime_root / "generations" / second / "manifests"
    installed = sorted(path.name for path in left.iterdir())
    assert installed == sorted(path.name for path in right.iterdir())
    for name in installed:
        assert (left / name).read_bytes() == (right / name).read_bytes()

    #: the sibling really is self-consistent: its basis names itself
    basis = json.loads((right.parent / "generation-basis.json").read_text(encoding="utf-8"))
    assert basis["manifest_sha256"] == json.loads(
        (left.parent / "generation-basis.json").read_text(encoding="utf-8")
    )["manifest_sha256"]

    _swing_current(route, second)
    with pytest.raises(ValueError, match="does not match the current pointer"):
        route.run_role(SERVING_ROLE)


def test_the_schema_loader_alone_would_accept_the_sibling(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, recorded_install: tuple[dict[str, Any], Any]
) -> None:
    """Why the cross-check is load-bearing and not belt-and-braces.

    Same world, same sibling, with only `resolve_legacy_schema_generation` neutralised into
    "return whatever the pointer says". The role starts, and the real loader accepts the
    sibling's id without complaint — which is exactly the silent acceptance the cross-check
    exists to turn into a refusal.
    """

    captured, real = recorded_install
    route = _route_a_world(tmp_path, monkeypatch)
    route.stage_and_publish()
    second = _second_install(captured, real)
    _swing_current(route, second)

    monkeypatch.setattr(
        service_main,
        "resolve_legacy_schema_generation",
        lambda *_a, legacy_generation, **_k: legacy_generation,
    )
    observed: dict[str, object] = {}
    loader = service_main.load_runtime_schema_service_bindings

    def observe(runtime_root: Path, **kwargs: Any) -> Any:
        observed["generation_id"] = kwargs["generation_id"]
        return loader(runtime_root, **kwargs)

    monkeypatch.setattr(service_main, "load_runtime_schema_service_bindings", observe)

    assert route.run_role(SERVING_ROLE) == 0
    assert observed["generation_id"] == second


def test_a_bootstrap_staged_generation_over_a_real_current_is_refused(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Route B's generation on a Route A host: the manifests came from the checkout, so
    nothing says they describe this bundle. Before the fix this combination is exactly what
    produced `not current`; it must now be a refusal that names the reason."""

    route = _route_a_world(tmp_path, monkeypatch)
    route.plan = route.world.stage_and_publish("bootstrap-over-current")

    with pytest.raises(ValueError, match="staged from the checkout"):
        route.run_role(SERVING_ROLE)


def test_a_manifest_from_another_authority_generation_is_refused(
    route_a: RouteAWorld,
) -> None:
    """The authority half still stands on its own: `:292` refuses a manifest whose
    generation directory is not the one the chain slot names."""

    argv = route_a.argv(SERVING_ROLE)
    manifest = Path(argv[argv.index("--manifest") + 1])
    stranger = route_a.world.generations / ("c" * 64) / "manifests"
    stranger.mkdir(parents=True)
    shutil.copyfile(manifest, stranger / manifest.name)
    shutil.copyfile(
        route_a.generation_document(GENERATION_LEGACY_BINDING_NAME),
        stranger.parent / GENERATION_LEGACY_BINDING_NAME,
    )
    argv[argv.index("--manifest") + 1] = str(stranger / manifest.name)

    with pytest.raises(ValueError, match="manifest generation does not match runtime environment"):
        route_a.run_role(SERVING_ROLE, argv=argv)


def test_a_removed_binding_document_is_refused_rather_than_assumed(
    route_a: RouteAWorld,
) -> None:
    """A generation staged before the document existed makes no claim, and gets none."""

    argv = route_a.argv(SERVING_ROLE)
    document = route_a.generation_document(GENERATION_LEGACY_BINDING_NAME)
    document.parent.chmod(0o755)
    try:
        document.chmod(0o644)
        document.unlink()
    finally:
        document.parent.chmod(0o555)

    with pytest.raises(ValueError, match="carries no legacy-binding.json"):
        route_a.run_role(SERVING_ROLE, argv=argv)


# ---------------------------------------------------------------------------------------
# The one substitution, pinned; and the same case again with no substitution at all
# ---------------------------------------------------------------------------------------


def test_the_remap_is_only_the_frozen_control_root_prefix(route_a: RouteAWorld) -> None:
    """Everything the wrapper derived is used verbatim except the control root's prefix,
    and the prefix that is replaced is exactly the runtime root the role derives from it."""

    wrapper = route_a.wrapper_argv(SERVING_ROLE)
    used = route_a.argv(SERVING_ROLE)
    assert len(wrapper) == len(used)
    differing = [index for index, value in enumerate(wrapper) if value != used[index]]
    assert differing == [wrapper.index("--control-root") + 1]

    frozen = Path(wrapper[differing[0]])
    moved = Path(used[differing[0]])
    assert service_main.runtime_root_from_control_root(frozen) == PRODUCTION_ROOT
    assert service_main.runtime_root_from_control_root(moved) == route_a.runtime_root
    assert frozen.relative_to(PRODUCTION_ROOT) == moved.relative_to(route_a.runtime_root)


@pytest.mark.parametrize("role", [SERVING_ROLE, PAPER_ROLE])
def test_the_iteration_the_loop_ran_never_reports_the_binding_failure(
    route_a: RouteAWorld, role: str
) -> None:
    """The step itself has no data to work on and records a failure; what matters is which
    failure. Reading the heartbeat back is the direct form of "no `not current`"."""

    assert route_a.run_role(role) == 0
    heartbeat = json.loads(_heartbeat(route_a, role).read_text(encoding="utf-8"))
    #: one iteration really ran
    assert heartbeat["total_failures"] + heartbeat["total_successes"] == 1
    assert heartbeat["status"] == "stopped"
    assert heartbeat["stop_reason"] == "loop completed"
    text = json.dumps(heartbeat)
    assert NOT_CURRENT not in text
    assert "schema service generation" not in text
    #: the step failed on the data a fresh host has none of, which is a different sentence
    #: from the one #207 is about
    assert heartbeat["last_error"] in {
        None,
        "RuntimeError: signals reader failed: ServingSourceAuthorityUnavailableError: "
        "current authority is unavailable",
        "RuntimeError: paper constraints require a visible market-minute batch",
    }


def _control_directory(role: str) -> str:
    """The kind directory the frozen policy's control root for this role ends in."""

    from rquant.runtime_authority import PRODUCTION_ROLE_POLICY

    entry = next(item for item in PRODUCTION_ROLE_POLICY if item.name == role)
    return Path(entry.control_root).name


def _heartbeat(route_a: RouteAWorld, role: str, *, root: Path | None = None) -> Path:
    """`<control root>/<instance>/heartbeats/<identity>.json`, the one the loop published."""

    control = (
        (root or route_a.runtime_root)
        / "control"
        / _control_directory(role)
        / route_a.instance_of(role)
    )
    published = sorted((control / "heartbeats").glob("*.json"))
    assert len(published) == 1, published
    return published[0]


@pytest.mark.linux_exact
@pytest.mark.skipif(
    not sys.platform.startswith("linux"),
    reason=(
        "the frozen control roots name /home/lighthouse/rquant/data/runtime, which only a "
        "Linux container can own; the portable cases above cover the same path with the "
        "prefix moved"
    ),
)
def test_the_wrapper_argv_runs_verbatim_against_the_frozen_production_root(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The whole case again with nothing substituted: the bundle is installed at the frozen
    `/home/lighthouse/rquant/data/runtime`, and the argv is the wrapper's own.

    This is the shape a Route A host really has, and the reason the acceptance is a Linux
    gate: `PRODUCTION_ROLE_POLICY` freezes that path, and no test can own it anywhere else.
    """

    if PRODUCTION_ROOT.exists():  # pragma: no cover - a real host, not a container
        raise AssertionError(f"{PRODUCTION_ROOT} already exists; refusing to touch a real host")
    PRODUCTION_ROOT.parent.mkdir(parents=True, exist_ok=True)
    try:
        route = _route_a_world(tmp_path, monkeypatch, runtime_root=PRODUCTION_ROOT)
        route.stage_and_publish()
        for role in (SERVING_ROLE, PAPER_ROLE):
            argv = route.wrapper_argv(role)
            assert PRODUCTION_ROOT.as_posix() in argv[argv.index("--control-root") + 1]
            assert route.run_role(role, argv=argv) == 0
            heartbeat = _heartbeat(route, role, root=PRODUCTION_ROOT).read_text(encoding="utf-8")
            assert NOT_CURRENT not in heartbeat
            assert json.loads(heartbeat)["service_id"]
    finally:
        shutil.rmtree(Path("/home/lighthouse/rquant"), ignore_errors=True)
