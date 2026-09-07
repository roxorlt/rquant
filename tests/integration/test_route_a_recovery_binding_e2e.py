"""#218 B/C acceptance: both recovery oneshots over a real Route A deployment.

On the 2026-09-07 Route A window `rquant-runtime-recovery@` and
`rquant-runtime-recovery-rehearsal@` both stopped at `recovery unit profile generation is
stale`, because the pass compared the recovery block's content hash with the authority
chain's `sha256(full-manifest.json)` (#218 B), and behind that wall sat two documents no
tool in this repository had ever written (#218 C).

Everything up to the recovery payload is real here: a real installed bundle, a real
`runtime-authority-stage --legacy-runtime-root` generation published into a root-owned
chain, the wrapper's own derived argv, the real
`scripts/provision_runtime_recovery_credentials.py` producing both documents out of the
installed profile, and the real `load_recovery_backup_config` /
`validate_runtime_recovery_backup_config` reading them back.

The payload itself — `cli.cmd_runtime_recovery` — needs a published backup generation under
`/var/lib/rquant/runtime-recovery/backups`, which is a different subsystem's acceptance. It
is observed rather than replaced: the recorder captures the arguments the profile resolved
and returns, and one case runs with no recorder at all to show that the failure that remains
is that payload and never the binding.
"""

from __future__ import annotations

import shutil
import sys
from argparse import Namespace
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import pytest

import rquant.cli as cli_module
import rquant.runtime_recovery_service as recovery_service
from tests.integration.test_route_a_legacy_binding_e2e import (
    PRODUCTION_ROOT,
    RouteAWorld,
    _route_a_world,
    _second_install,
    recorded_install,
)

#: re-exported so pytest resolves the fixture in this module
__all__ = ["recorded_install"]

_SCRIPTS = Path(__file__).resolve().parents[2] / "scripts"
if str(_SCRIPTS) not in sys.path:  # pragma: no cover - import bootstrap
    sys.path.insert(0, str(_SCRIPTS))

from provision_runtime_recovery_credentials import provision  # noqa: E402

pytestmark = pytest.mark.integration

RECOVERY_ROLE = "runtime_recovery"
REHEARSAL_ROLE = "runtime_recovery_rehearsal"
STALE = "recovery unit profile generation is stale"

AS_OF = datetime(2026, 9, 7, 8, 0, tzinfo=UTC)
REPLAY_START = date(2026, 7, 1)
REPLAY_END = date(2026, 7, 31)


@pytest.fixture
def route_a(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> RouteAWorld:
    route = _route_a_world(tmp_path, monkeypatch)
    route.stage_and_publish()
    return route


def _provision(route: RouteAWorld) -> dict[str, object]:
    """Run the real producer against the installed profile, into the paths it names."""

    recovery = route.profile.recovery
    Path(recovery.credential_file).parent.mkdir(parents=True, exist_ok=True)
    Path(recovery.backup_config_path).parent.mkdir(parents=True, exist_ok=True)
    return provision(
        route.runtime_root,
        as_of=AS_OF,
        replay_start_date=REPLAY_START,
        replay_end_date=REPLAY_END,
    )


def _argv(route: RouteAWorld, role: str) -> list[str]:
    argv = list(route.world.resolve(role, route.instance_of(role))["module_argv"])
    index = argv.index("--control-root") + 1
    argv[index] = str(route.runtime_root / Path(argv[index]).relative_to(PRODUCTION_ROOT))
    return argv


def _record_payload(monkeypatch: pytest.MonkeyPatch) -> dict[str, object]:
    """Observe the arguments the pass resolved out of the profile, and stop there."""

    observed: dict[str, object] = {}

    def record(resolved: Namespace) -> int:
        observed.update(vars(resolved))
        return 0

    monkeypatch.setattr(cli_module, "cmd_runtime_recovery", record)
    return observed


def _swing_current(route: RouteAWorld, generation: str) -> None:
    current = route.runtime_root / "current"
    current.unlink()
    current.symlink_to(Path("generations") / generation, target_is_directory=True)


# ---------------------------------------------------------------------------------------
# The world the two roles actually get
# ---------------------------------------------------------------------------------------


def test_the_two_generation_ids_the_old_comparison_used_are_different_documents(
    route_a: RouteAWorld,
) -> None:
    """The premise, measured rather than argued: the wrapper's id is not the recovery hash."""

    argv = _argv(route_a, RECOVERY_ROLE)
    authority_generation = argv[argv.index("--expected-generation") + 1]
    recovery_generation = route_a.profile.recovery.profile_generation

    assert authority_generation == route_a.plan.plan["generation_id"]
    assert recovery_generation != authority_generation
    assert recovery_generation != route_a.receipt.generation_hash


def test_the_producer_writes_both_documents_the_units_read(route_a: RouteAWorld) -> None:
    """#218 C: the two files that had no producer, produced from the installed profile."""

    summary = _provision(route_a)
    recovery = route_a.profile.recovery

    credential = Path(recovery.credential_file)
    backup = Path(recovery.backup_config_path)
    assert credential.is_file()
    assert backup.is_file()
    assert summary["recovery_profile_generation"] == recovery.profile_generation
    assert summary["backup_config"]["target_profile_generation"] == recovery.profile_generation
    assert summary["credential"]["key_id"] == recovery.signer_key_id


# ---------------------------------------------------------------------------------------
# The acceptance
# ---------------------------------------------------------------------------------------


@pytest.mark.parametrize("role", [RECOVERY_ROLE, REHEARSAL_ROLE])
def test_a_recovery_oneshot_reaches_its_payload_over_a_real_current(
    route_a: RouteAWorld,
    monkeypatch: pytest.MonkeyPatch,
    role: str,
) -> None:
    """Both units get past the binding, both config walls, and hand over real arguments."""

    _provision(route_a)
    observed = _record_payload(monkeypatch)

    assert recovery_service.main(_argv(route_a, role)) == 0

    recovery = route_a.profile.recovery
    assert observed["recovery_action"] == "execute"
    assert observed["credential_file"] == Path(recovery.credential_file)
    assert observed["publication_root"] == Path(recovery.backup_publication_root)
    assert observed["deadline_seconds"] == recovery.recovery_deadline_seconds
    assert observed["worker_id"].endswith(recovery.profile_generation[:12])


def test_the_unstubbed_pass_fails_on_its_payload_and_never_on_the_binding(
    route_a: RouteAWorld,
) -> None:
    """Nothing before the payload is being hidden by the recorder.

    With no recorder at all the pass runs into the recovery subsystem, which on a host with
    no published backup generation has nothing to restore. What matters is which failure it
    is: not `stale`, not a generation binding, not a missing document.
    """

    _provision(route_a)

    with pytest.raises(Exception) as raised:  # noqa: PT011 - the type is the finding
        recovery_service.main(_argv(route_a, RECOVERY_ROLE))

    message = str(raised.value)
    assert STALE not in message
    assert "legacy generation binding" not in message
    assert "recovery backup config" not in message


def test_without_the_produced_documents_the_pass_stops_on_the_missing_document(
    route_a: RouteAWorld,
) -> None:
    """The wall behind the binding, and the reason #218 C had to ship with #218 B."""

    with pytest.raises(Exception) as raised:  # noqa: PT011 - the type is the finding
        recovery_service.main(_argv(route_a, RECOVERY_ROLE))

    assert STALE not in str(raised.value)


# ---------------------------------------------------------------------------------------
# The refusals: the binding that replaced the impossible comparison still refuses
# ---------------------------------------------------------------------------------------


def test_a_current_pointer_moved_to_a_copied_generation_is_refused(
    route_a: RouteAWorld,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A `copytree` copy is the cheap case, and the profile loader alone already sees it: a
    copied `generation-basis.json` hashes to the directory it came from. The case only the
    binding can see is the sibling below."""

    _provision(route_a)
    _record_payload(monkeypatch)
    other = "b" * 64
    shutil.copytree(
        route_a.runtime_root / "generations" / route_a.receipt.generation_hash,
        route_a.runtime_root / "generations" / other,
    )
    _swing_current(route_a, other)

    with pytest.raises(ValueError, match="generation hash mismatch") as raised:
        recovery_service.main(_argv(route_a, RECOVERY_ROLE))
    assert STALE not in str(raised.value)


def test_a_sibling_generation_with_the_same_manifests_is_refused(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    recorded_install: tuple[dict[str, Any], Any],
) -> None:
    """`current` swung to a *legitimately installed* sibling — the case only the document sees.

    A second real `install_runtime_deployment_profile` on the same root (one rotated notify
    credential, an ordinary redeploy) leaves a self-consistent generation with byte-identical
    manifests and the same deployment profile, so the profile loader accepts it and the
    recovery namespace check agrees with itself. `legacy-binding.json` is the only thing that
    knows the authority generation was staged from the other one.
    """

    captured, real = recorded_install
    route = _route_a_world(tmp_path, monkeypatch)
    first = route.receipt.generation_hash
    route.stage_and_publish()
    _provision(route)
    second = _second_install(captured, real)
    assert second != first
    _swing_current(route, second)
    _record_payload(monkeypatch)

    with pytest.raises(ValueError, match="does not match the current pointer"):
        recovery_service.main(_argv(route, RECOVERY_ROLE))


def test_a_manifest_from_another_authority_generation_is_refused(
    route_a: RouteAWorld,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The authority half: the manifest has to sit in the generation the chain slot names."""

    from rquant.runtime_legacy_generation_binding import GENERATION_LEGACY_BINDING_NAME

    _provision(route_a)
    _record_payload(monkeypatch)
    argv = _argv(route_a, RECOVERY_ROLE)
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
        recovery_service.main(argv)


def test_a_bootstrap_staged_generation_over_a_real_current_is_refused(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Route B's generation on a Route A host: its manifests describe no legacy bundle."""

    route: Any = _route_a_world(tmp_path, monkeypatch)
    route.plan = route.world.stage_and_publish("bootstrap-over-current")
    _provision(route)
    _record_payload(monkeypatch)

    with pytest.raises(ValueError, match="staged from the checkout"):
        recovery_service.main(_argv(route, RECOVERY_ROLE))
