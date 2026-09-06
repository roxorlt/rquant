"""#215 acceptance: the credstore roles, over a real bundle and a real sealed credential.

Seven roles carry `LoadCredentialEncrypted=`. In the first Route A window every one of them
failed to start, and because `reference_slow_publisher` is one of them no serving generation
could be assembled at all. The three defects were code, not configuration, and #215's own
closing note says what an acceptance for them has to be: "a Linux end-to-end test that starts
each credstore role under a real LoadCredentialEncrypted unit, since none of this had ever run
before this window." Nothing on that path is stubbed here:

* the deployment bundle is really installed — `install_runtime_deployment_profile` over the
  real production profile, a real `current -> generations/<64 hex>` pointer, real service
  manifests — and the credential plaintexts it built are the real ones, stamped with that
  bundle's own generation (the fixture is package A's, which now keeps them);
* the authority chain is really staged and published, and the wrapper's own `resolve_launch`
  derives both the argv and the child environment out of the root-owned profile, so the
  environment the role runs in is the one the allowlist actually produces;
* `runtime_service_main.run()` is what runs, with `os.environ` replaced by that child
  environment and nothing else, and it enters the real service loop for one iteration;
* the Linux gate additionally seals those plaintexts with the real root helper and the real
  `/usr/bin/systemd-creds`, then decrypts `current.cred` back and lays the plaintext out the
  way `LoadCredentialEncrypted` does, so the roles read bytes that made the full round trip.

Two seams, both off the path under test and both package A's already: the module constants
that name `/etc/rquant`, `/var/lib/rquant` and the system interpreter, and the `os.stat` hook
that lets a non-root test own a 0444 keyring. A third is added here and is only a clock: the
builders take one, and freezing it to a date the bundle's calendar does not open keeps every
source role's step inside its own session gate instead of reaching for the network.
"""

from __future__ import annotations

import hashlib
import os
import runpy
import shutil
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from unittest import mock

import pytest

import rquant.runtime_service_builtin as builtin_module
import rquant.runtime_service_main as service_main
from rquant.runtime_capabilities import (
    RUNTIME_CAPABILITY_CREDENTIAL_NAME,
    RuntimeCapabilityCredential,
)
from rquant.runtime_exec_wrapper import _verify
from rquant.runtime_service_control import RuntimeServiceControl, RuntimeServiceStatus
from rquant.runtime_service_entrypoint import RuntimeServiceKind
from rquant.strict_json import strict_model_validate_json
from tests.integration.test_route_a_legacy_binding_e2e import (
    PRODUCTION_ROOT,
    RouteAWorld,
    _route_a_world,
    _StopAfterOneIteration,
)
from tests.unit.test_runtime_authority_publish import UID

pytestmark = pytest.mark.integration

REPO_ROOT = Path(__file__).resolve().parents[2]
SEALER = REPO_ROOT / "deploy" / "libexec" / "rquant-runtime-credential-sealer"

#: The six roles #215 lists, in the order the issue reports them. `artifact_retention` is the
#: seventh credstore role but is not a runtime template unit, and its own unit was never
#: started in that window; its credential delivery is covered by the allowlist tests.
CREDSTORE_ROLES: tuple[str, ...] = (
    "reference_slow_source",
    "reference_slow_publisher",
    "market_minute_source",
    "auction_match_source",
    "daily_close_source",
    "notifier",
)

#: 09:30 Shanghai on 2026-08-04. The bundle's calendar opens exactly one date, 2026-08-03, so
#: every source role's step takes its "not an open date" branch and no adapter call is made.
FROZEN_NOW = datetime(2026, 8, 4, 1, 30, tzinfo=UTC)

#: The failures #215 recorded. None of them may be what a role reports after these fixes.
FORBIDDEN_ERRORS = (
    "validation errors for Settings",
    "capability is required",
    "requires its isolated",
    "requires the source verification key",
    "capability credential",
)


def _instance_of(service_id: str) -> str:
    return "svc-" + hashlib.sha256(service_id.encode("utf-8")).hexdigest()


def _manifest_for(route: RouteAWorld, role: str) -> Any:
    kind = RuntimeServiceKind(role)
    manifests = [
        manifest for manifest in route.profile.manifests if manifest.service_kind is kind
    ]
    assert len(manifests) == 1, role
    return manifests[0]


def _launch(route: RouteAWorld, role: str, credentials_directory: Path | None) -> dict[str, Any]:
    """The wrapper's own answer for this role, including the child environment it builds."""

    source_environment = {"LANG": "C", "TZ": "UTC", "SECRET": "leak"}
    if credentials_directory is not None:
        source_environment["CREDENTIALS_DIRECTORY"] = str(credentials_directory)
    return _verify.resolve_launch(
        role,
        instance=_instance_of(_manifest_for(route, role).service_id),
        profile_path=str(route.world.profile_path),
        authority_path=str(route.world.authority_path),
        generation_root=str(route.world.generations),
        trusted_root=str(route.world.root),
        expected_owner_uid=UID,
        source_environment=source_environment,
    )


def _relocated_argv(route: RouteAWorld, module_argv: list[str]) -> list[str]:
    """The wrapper's argv with the frozen control-root prefix moved onto this world's root.

    Exactly package A's move and no other: `PRODUCTION_ROLE_POLICY` freezes every control
    root under `/home/lighthouse/rquant/data/runtime`, and no test can own that path. The
    Linux gate below runs the argv verbatim against the real prefix.
    """

    argv = list(module_argv)
    index = argv.index("--control-root") + 1
    argv[index] = str(route.runtime_root / Path(argv[index]).relative_to(PRODUCTION_ROOT))
    return argv


def _deliver(directory: Path, plaintext: bytes) -> Path:
    """Lay a decrypted credential out the way `LoadCredentialEncrypted` does.

    systemd puts each loaded credential in the unit's own credentials directory as a regular
    file named by the credential id, owned by the unit's user, mode 0400, one link. Those are
    the four properties `_read_private_credential` insists on, so they are the four this
    fixture reproduces.
    """

    directory.mkdir(mode=0o700, parents=True, exist_ok=True)
    directory.chmod(0o700)
    path = directory / RUNTIME_CAPABILITY_CREDENTIAL_NAME
    path.write_bytes(plaintext)
    path.chmod(0o400)
    observed = path.lstat()
    assert observed.st_uid == os.geteuid()
    assert observed.st_nlink == 1
    assert observed.st_mode & 0o077 == 0
    return path


def _credentials_root(route: RouteAWorld, root: Path) -> dict[str, Path]:
    """One credentials directory per credstore role, each holding that role's plaintext."""

    directories: dict[str, Path] = {}
    for role in CREDSTORE_ROLES:
        instance = _instance_of(_manifest_for(route, role).service_id)
        plaintext = route.sealed_credentials[instance]
        directories[role] = _deliver(root / role, plaintext).parent
    return directories


def _run_role(route: RouteAWorld, role: str, *, environment: dict[str, str]) -> Any:
    """`runtime_service_main.run()` under this environment, one loop iteration, then stop."""

    argv = _relocated_argv(route, _launch(route, role, None)["module_argv"])
    arguments = service_main.build_parser().parse_args(argv)
    stop = _StopAfterOneIteration()
    real_event = service_main.Event
    real_registry = builtin_module.build_builtin_registry
    service_main.Event = lambda: stop  # type: ignore[assignment]
    builtin_module.build_builtin_registry = (  # type: ignore[assignment]
        lambda **kwargs: real_registry(clock=lambda: FROZEN_NOW, **kwargs)
    )
    try:
        with mock.patch.dict(os.environ, environment, clear=True):
            code = service_main.run(arguments)
    finally:
        service_main.Event = real_event  # type: ignore[assignment]
        builtin_module.build_builtin_registry = real_registry  # type: ignore[assignment]
    assert code == 0
    assert stop.iterations == 1, f"{role} never entered its service loop"
    return arguments


def _heartbeat(route: RouteAWorld, role: str) -> Any:
    manifest = _manifest_for(route, role)
    argv = _relocated_argv(route, _launch(route, role, None)["module_argv"])
    control_root = Path(argv[argv.index("--control-root") + 1])
    return RuntimeServiceControl.read_heartbeat(control_root, manifest.service_spec)


def _create_the_routers_spool(route: RouteAWorld) -> Path:
    """The notifier opens the routed-signal spool while it is being built, so it must exist.

    On the host the signal router creates it, and #218 is exactly the report that it had not:
    the notifier's failure that window was `route spool is unavailable`, which is a missing
    producer and not a missing capability. Package A does the same for the reference registry
    the paper-constraint publisher opens. Empty is enough — what is under test here is that
    the notifier gets that far at all, which before these fixes it could not, because
    importing its provider loader built `Settings`.
    """

    from rquant.signal_route_spool import SignalRouteSpool

    manifest = _manifest_for(route, "notifier")
    root = Path(str(manifest.settings["signal_spool_root"]))
    root.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    SignalRouteSpool(root)
    return root


@pytest.fixture
def credstore(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> RouteAWorld:
    """A published Route A chain over a really installed bundle, with its plaintexts kept."""

    route = _route_a_world(tmp_path, monkeypatch)
    route.stage_and_publish()
    _create_the_routers_spool(route)
    return route


# ---------------------------------------------------------------------------------------
# The credential the bundle sealed really is the one the role is asked for
# ---------------------------------------------------------------------------------------


def test_every_credstore_role_has_a_plaintext_bound_to_the_bundle_generation(
    credstore: RouteAWorld,
) -> None:
    """And that generation is not the authority chain's, which is the whole of defect two."""

    legacy = service_main.legacy_current_generation(credstore.runtime_root)
    authority = str(credstore.plan.plan["generation_id"])
    assert legacy is not None
    assert legacy != authority

    for role in CREDSTORE_ROLES:
        manifest = _manifest_for(credstore, role)
        instance = _instance_of(manifest.service_id)
        credential = strict_model_validate_json(
            RuntimeCapabilityCredential, credstore.sealed_credentials[instance]
        )
        assert credential.service_id == manifest.service_id
        assert credential.service_kind is manifest.service_kind
        assert credential.instance_name == instance
        assert credential.bundle_generation == legacy
        assert credential.capabilities


def test_the_wrapper_hands_the_credential_address_to_the_credstore_roles(
    credstore: RouteAWorld,
    tmp_path: Path,
) -> None:
    """The published profile's allowlist, read back through the wrapper that enforces it."""

    directory = tmp_path / "creds"
    for role in CREDSTORE_ROLES:
        environment = _launch(credstore, role, directory)["environment"]
        assert environment["CREDENTIALS_DIRECTORY"] == str(directory), role
        assert set(environment) == {"CREDENTIALS_DIRECTORY", "LANG", "TZ", "PWD"}, role


def test_a_role_outside_the_credstore_group_is_still_denied_the_address(
    credstore: RouteAWorld,
    tmp_path: Path,
) -> None:
    """Least privilege, through the real profile rather than the constant."""

    environment = _launch(credstore, "feature_live", tmp_path / "creds")["environment"]

    assert "CREDENTIALS_DIRECTORY" not in environment
    assert set(environment) == {"LANG", "TZ", "PWD"}


# ---------------------------------------------------------------------------------------
# The six roles, in their loops, under the wrapper's environment
# ---------------------------------------------------------------------------------------


@pytest.mark.parametrize("role", CREDSTORE_ROLES)
def test_a_credstore_role_reaches_its_loop_with_its_delivered_credential(
    credstore: RouteAWorld,
    tmp_path: Path,
    role: str,
) -> None:
    """One iteration, and none of #215's five failures on the way there."""

    directories = _credentials_root(credstore, tmp_path / "credentials")
    environment = _launch(credstore, role, directories[role])["environment"]

    _run_role(credstore, role, environment=environment)

    heartbeat = _heartbeat(credstore, role)
    assert heartbeat is not None
    assert heartbeat.status is RuntimeServiceStatus.STOPPED
    assert heartbeat.total_successes + heartbeat.total_failures == 1
    for forbidden in FORBIDDEN_ERRORS:
        assert forbidden not in (heartbeat.last_error or ""), heartbeat.last_error


def test_the_six_roles_run_one_after_another_over_the_same_generation(
    credstore: RouteAWorld,
    tmp_path: Path,
) -> None:
    """The window's real question: can the credstore group come up together, not one by one."""

    directories = _credentials_root(credstore, tmp_path / "credentials")
    for role in CREDSTORE_ROLES:
        _run_role(credstore, role, environment=_launch(credstore, role, directories[role])[
            "environment"
        ])

    reached = {role: _heartbeat(credstore, role) for role in CREDSTORE_ROLES}
    assert all(heartbeat is not None for heartbeat in reached.values())
    assert len(reached) == 6


# ---------------------------------------------------------------------------------------
# Reverse: without the credential, each role refuses, and says why
# ---------------------------------------------------------------------------------------


#: Five of the six refuse while their builder runs. The notifier is the exception by design:
#: its provider loader is called at delivery time, so with nothing routed there is nothing it
#: needs a credential for yet — and it refuses the moment there is (see the case below).
BUILD_TIME_REFUSERS = tuple(role for role in CREDSTORE_ROLES if role != "notifier")


@pytest.mark.parametrize("role", BUILD_TIME_REFUSERS)
def test_a_credstore_role_without_its_credential_refuses(
    credstore: RouteAWorld,
    role: str,
) -> None:
    """No credential directory at all: the role must never reach a loop, silently or not."""

    environment = _launch(credstore, role, None)["environment"]
    assert "CREDENTIALS_DIRECTORY" not in environment

    with pytest.raises((ValueError, RuntimeError)) as raised:
        _run_role(credstore, role, environment=environment)

    assert _heartbeat(credstore, role) is None
    message = str(raised.value)
    assert "validation errors for Settings" not in message
    assert any(
        token in message for token in ("capability is required", "credential")
    ), message


def test_the_notifier_without_its_credential_delivers_nothing_and_refuses_to_try(
    credstore: RouteAWorld,
) -> None:
    """Deferred, not absent: the loader is what holds the line, and it is still closed."""

    from rquant.runtime_notification_providers import (
        build_environment_notification_provider_loader,
    )

    environment = _launch(credstore, "notifier", None)["environment"]
    assert "CREDENTIALS_DIRECTORY" not in environment

    _run_role(credstore, "notifier", environment=environment)
    heartbeat = _heartbeat(credstore, "notifier")
    assert heartbeat is not None
    assert heartbeat.processed_count == 0

    loader = build_environment_notification_provider_loader(environment={})
    with pytest.raises(RuntimeError, match="at least one notification capability is required"):
        loader()


@pytest.mark.parametrize("role", CREDSTORE_ROLES)
def test_a_credential_sealed_for_another_generation_is_refused(
    credstore: RouteAWorld,
    tmp_path: Path,
    role: str,
) -> None:
    """The bundle binding is load-bearing, not decoration: a foreign generation fails closed."""

    manifest = _manifest_for(credstore, role)
    instance = _instance_of(manifest.service_id)
    plaintext = credstore.sealed_credentials[instance]
    legacy = service_main.legacy_current_generation(credstore.runtime_root)
    assert legacy is not None
    foreign = plaintext.replace(legacy.encode("ascii"), b"c" * 64)
    assert foreign != plaintext
    directory = _deliver(tmp_path / "foreign" / role, foreign).parent
    environment = _launch(credstore, role, directory)["environment"]

    with pytest.raises(ValueError, match="generation does not match"):
        _run_role(credstore, role, environment=environment)


@pytest.mark.parametrize("role", CREDSTORE_ROLES)
def test_another_roles_credential_is_refused(
    credstore: RouteAWorld,
    tmp_path: Path,
    role: str,
) -> None:
    """Delivery to the wrong door is not a way in either."""

    other = next(name for name in CREDSTORE_ROLES if name != role)
    instance = _instance_of(_manifest_for(credstore, other).service_id)
    directory = _deliver(
        tmp_path / "crossed" / role, credstore.sealed_credentials[instance]
    ).parent
    environment = _launch(credstore, role, directory)["environment"]

    with pytest.raises(ValueError, match="does not match runtime"):
        _run_role(credstore, role, environment=environment)


# ---------------------------------------------------------------------------------------
# Linux gate: the real sealer, the real systemd-creds, the real round trip
# ---------------------------------------------------------------------------------------


def _systemd_creds_available() -> bool:
    if sys.platform != "linux" or os.geteuid() != 0:
        return False
    return shutil.which("systemd-creds") is not None


@pytest.mark.linux_exact
@pytest.mark.skipif(
    not _systemd_creds_available(),
    reason="needs root on Linux with systemd-creds, as the production host has",
)
def test_the_roles_run_off_a_credential_the_real_sealer_encrypted(
    credstore: RouteAWorld,
    tmp_path: Path,
) -> None:
    """Seal with the root helper, decrypt with systemd-creds, then start the six roles.

    The helper is the file that is installed on the host, run through `runpy` the way its own
    unit test runs it, with its store root moved to a temporary directory and its owner uid
    set to this process's — the two things a test cannot have are `/etc/credstore.encrypted`
    and uid 0's `sudo`, and neither is what this is checking. Everything else is the
    production article: the same request shape `runtime_credential_sealer_client` sends, the
    same `/usr/bin/systemd-creds encrypt --name=capabilities.json` the helper calls, the same
    `<instance>/generations/<generation>.cred` layout and the same `current.cred` pointer.
    """

    import base64
    import json

    helper = runpy.run_path(str(SEALER))
    store_root = tmp_path / "credstore"
    request = json.dumps(
        {
            "schema_version": 2,
            "operation": "begin",
            "credentials": {
                instance: base64.b64encode(plaintext).decode("ascii")
                for instance, plaintext in credstore.sealed_credentials.items()
            },
        },
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")

    receipt = helper["process_request"](
        request, store_root=store_root, owner_uid=os.geteuid()
    )
    assert receipt["operation"] == "begin"
    assert set(receipt["sealed_instances"]) == set(credstore.sealed_credentials)

    legacy = service_main.legacy_current_generation(credstore.runtime_root)
    delivered_root = tmp_path / "run-credentials"
    directories: dict[str, Path] = {}
    for role in CREDSTORE_ROLES:
        instance = _instance_of(_manifest_for(credstore, role).service_id)
        pointer = store_root / "instances" / instance / "current.cred"
        assert pointer.is_symlink()
        assert os.readlink(pointer) == f"generations/{legacy}.cred"
        sealed = pointer.read_bytes()
        assert credstore.sealed_credentials[instance] not in sealed

        unsealed = subprocess.run(
            (
                "/usr/bin/systemd-creds",
                "decrypt",
                f"--name={RUNTIME_CAPABILITY_CREDENTIAL_NAME}",
                str(pointer),
                "-",
            ),
            capture_output=True,
            check=True,
        ).stdout
        assert unsealed == credstore.sealed_credentials[instance]
        directories[role] = _deliver(delivered_root / role, unsealed).parent

    for role in CREDSTORE_ROLES:
        environment = _launch(credstore, role, directories[role])["environment"]
        _run_role(credstore, role, environment=environment)
        heartbeat = _heartbeat(credstore, role)
        assert heartbeat is not None, role
        for forbidden in FORBIDDEN_ERRORS:
            assert forbidden not in (heartbeat.last_error or ""), (role, heartbeat.last_error)
