"""#215: the sealed capability has to reach the role, and say so when it does not.

Seven roles carry `LoadCredentialEncrypted=capabilities.json:…/instances/%i/current.cred`.
systemd decrypts that into `$CREDENTIALS_DIRECTORY/capabilities.json` for the unit's
ExecStart — which is the runtime-exec wrapper, not the role. The wrapper then builds the
role child's environment from an empty dictionary and copies only the names the root-owned
profile allowlists for that role, and `CREDENTIALS_DIRECTORY` was not one of them, so the
credential was dropped in transit without a word: `load_systemd_runtime_capabilities` saw no
credential directory, returned an empty mapping, and the role died further down with
`TUSHARE_TOKEN_MAIN capability is required` — a message about a capability that had in fact
been sealed, delivered and decrypted.

Three things are held here:

* the allowlist now carries `CREDENTIALS_DIRECTORY`, for exactly the seven capability roles
  and no others;
* a capability role that starts under a systemd unit with no credential directory refuses,
  and its message names which of the two links broke;
* the credential is checked against the **deployment bundle** generation, which is the only
  namespace it is ever sealed in — not the authority chain generation the wrapper forwards.
"""

from __future__ import annotations

from pathlib import Path

import pytest

import rquant.runtime_capabilities as capabilities_module
from rquant.runtime_authority import PRODUCTION_ROLE_POLICY
from rquant.runtime_capabilities import (
    CAPABILITY_KEYS,
    RUNTIME_CAPABILITY_CREDENTIAL_NAME,
    load_systemd_runtime_capabilities,
    serialize_runtime_credential,
)
from rquant.runtime_exec_wrapper import _verify
from rquant.runtime_service_entrypoint import RuntimeServiceKind
from tests.support.systemd_credential_delivery import Delivery, deliver

BUNDLE_GENERATION = "b" * 64
AUTHORITY_GENERATION = "e" * 64
SERVICE_ID = "source.daily-close"
INSTANCE = "svc-" + "a" * 64
KIND = RuntimeServiceKind.DAILY_CLOSE_SOURCE
UNIT = "rquant-runtime-daily-close@" + INSTANCE + ".service"
CAPABILITY_ROLE_NAMES = frozenset(
    {
        "artifact_retention",
        "auction_match_source",
        "daily_close_source",
        "market_minute_source",
        "notifier",
        "reference_slow_publisher",
        "reference_slow_source",
    }
)


# ---------------------------------------------------------------------------------------
# The wrapper allowlist
# ---------------------------------------------------------------------------------------


def test_the_seven_capability_roles_are_exactly_the_kinds_that_need_a_credential() -> None:
    """The role names below are not a second list to keep in step; they are derived."""

    kinds = {kind.value for kind in CAPABILITY_KEYS}
    named = {entry.service_kind for entry in PRODUCTION_ROLE_POLICY if entry.service_kind}

    assert kinds == CAPABILITY_ROLE_NAMES
    assert named >= CAPABILITY_ROLE_NAMES


def test_only_the_capability_roles_receive_the_credentials_directory() -> None:
    """Least privilege: the other twenty-one roles' allowlists are unchanged."""

    with_credentials = {
        entry.name
        for entry in PRODUCTION_ROLE_POLICY
        if "CREDENTIALS_DIRECTORY" in entry.environment_allowlist
    }

    assert with_credentials == CAPABILITY_ROLE_NAMES
    for entry in PRODUCTION_ROLE_POLICY:
        if entry.name in CAPABILITY_ROLE_NAMES:
            assert entry.environment_allowlist == ("CREDENTIALS_DIRECTORY", "LANG", "LC_ALL", "TZ")
        elif entry.name == "lab_claim_finalizer":
            assert entry.environment_allowlist == (
                "APP_ENV",
                "LANG",
                "LC_ALL",
                "RQUANT_DISABLE_DOTENV",
                "TZ",
            )
        else:
            assert entry.environment_allowlist == ("LANG", "LC_ALL", "TZ"), entry.name


def test_every_role_allowlist_stays_sorted_deduplicated_and_within_its_bounds() -> None:
    """The profile schema and the code attestation both reject anything else."""

    for entry in PRODUCTION_ROLE_POLICY:
        names = entry.environment_allowlist
        assert names == tuple(sorted(set(names))), entry.name
        assert len(names) <= 32
        for name in names:
            assert name.isascii() and name.isupper() and len(name.encode("utf-8")) <= 64


def test_the_wrapper_carries_the_credentials_directory_through_to_the_child() -> None:
    """`build_child_environment` accepts the name and copies the value verbatim."""

    environment = _verify.build_child_environment(
        profile={
            "roles": {
                "daily_close_source": {
                    "environment_allowlist": ["CREDENTIALS_DIRECTORY", "LANG", "LC_ALL", "TZ"]
                }
            }
        },
        role="daily_close_source",
        spec={"working_directory": "/run"},
        source_environment={
            "CREDENTIALS_DIRECTORY": f"/run/credentials/{UNIT}",
            "LANG": "C",
            "APP_ENV": "prod",
        },
    )

    assert environment["CREDENTIALS_DIRECTORY"] == f"/run/credentials/{UNIT}"
    assert "APP_ENV" not in environment


def test_a_role_without_the_name_still_has_it_dropped() -> None:
    """The mechanism that broke #215 is unchanged for every role that must not receive it."""

    environment = _verify.build_child_environment(
        profile={"roles": {"feature_live": {"environment_allowlist": ["LANG", "LC_ALL", "TZ"]}}},
        role="feature_live",
        spec={"working_directory": "/run"},
        source_environment={"CREDENTIALS_DIRECTORY": "/run/credentials/x.service", "LANG": "C"},
    )

    assert "CREDENTIALS_DIRECTORY" not in environment


# ---------------------------------------------------------------------------------------
# The refusal, and which of the two links it names
# ---------------------------------------------------------------------------------------


@pytest.fixture
def under_a_unit(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Make this process look like it is running under `UNIT`, with an empty credential root."""

    cgroup = tmp_path / "cgroup"
    cgroup.write_text(f"0::/system.slice/system-rquant.slice/{UNIT}\n", encoding="utf-8")
    credentials_root = tmp_path / "run-credentials"
    credentials_root.mkdir()
    monkeypatch.setattr(capabilities_module, "_SYSTEMD_CGROUP_PATH", cgroup)
    monkeypatch.setattr(capabilities_module, "_SYSTEMD_CREDENTIALS_ROOT", credentials_root)
    return credentials_root


def _load(environ: dict[str, str], *, generation: str | None = BUNDLE_GENERATION):
    return load_systemd_runtime_capabilities(
        KIND,
        expected_service_id=SERVICE_ID,
        expected_instance=INSTANCE,
        expected_generation=generation,
        environ=environ,
    )


def test_a_dropped_credentials_directory_names_the_profile_allowlist(under_a_unit: Path) -> None:
    """systemd did decrypt it; the wrapper is what did not pass the address on."""

    delivered = under_a_unit / UNIT
    delivered.mkdir()
    (delivered / RUNTIME_CAPABILITY_CREDENTIAL_NAME).write_bytes(b"{}")

    with pytest.raises(ValueError) as raised:
        _load({})

    message = str(raised.value)
    assert "was not delivered to daily_close_source" in message
    assert "systemd did load it for unit " + UNIT in message
    assert "environment allowlist" in message
    assert "LoadCredentialEncrypted" not in message


def test_an_unloaded_credential_names_the_unit_and_the_credstore(under_a_unit: Path) -> None:
    """Nothing was decrypted for this unit, so the repair is on the unit, not the profile."""

    with pytest.raises(ValueError) as raised:
        _load({})

    message = str(raised.value)
    assert "was not delivered to daily_close_source" in message
    assert f"systemd loaded no {RUNTIME_CAPABILITY_CREDENTIAL_NAME} for unit {UNIT}" in message
    assert "LoadCredentialEncrypted" in message
    assert "environment allowlist" not in message


def test_outside_a_systemd_unit_nothing_is_accused(tmp_path: Path, monkeypatch) -> None:
    """A bare diagnostic run has no delivery mechanism to blame, so it degrades as before."""

    monkeypatch.setattr(capabilities_module, "_SYSTEMD_CGROUP_PATH", tmp_path / "absent")

    assert dict(_load({})) == {}


def test_a_role_that_needs_no_capability_never_refuses(under_a_unit: Path) -> None:
    """Eighteen of the twenty-five kinds carry no capability at all."""

    assert (
        dict(
            load_systemd_runtime_capabilities(
                RuntimeServiceKind.FEATURE_LIVE,
                expected_service_id="feature.live",
                expected_instance=INSTANCE,
                expected_generation=BUNDLE_GENERATION,
                environ={},
            )
        )
        == {}
    )


def test_a_credential_directory_without_the_named_credential_says_so(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """systemd made the directory but loaded some other id into it."""

    delivery = deliver(
        monkeypatch,
        root=tmp_path / "run-credentials",
        payload=b"{}",
        unit=UNIT,
        name="something-else.json",
    )

    with pytest.raises(ValueError) as raised:
        _load({"CREDENTIALS_DIRECTORY": str(delivery.directory)})

    assert "carries no capabilities.json" in str(raised.value)
    assert "LoadCredentialEncrypted= name does not match" in str(raised.value)


# ---------------------------------------------------------------------------------------
# Which generation the credential is bound to
# ---------------------------------------------------------------------------------------


def _sealed(
    monkeypatch: pytest.MonkeyPatch,
    root: Path,
    *,
    generation: str = BUNDLE_GENERATION,
) -> Delivery:
    """The credential in the shape systemd delivers it: root-owned 0440 under `/run/credentials`.

    Package E wrote it as a file this process owns, mode 0400, in a private directory. That
    passed the reader as it then was and matched nothing systemd does, which is why five
    units still refused to start with a credential that had been sealed, delivered and
    decrypted (#215, third break).
    """

    return deliver(
        monkeypatch,
        root=root,
        unit=UNIT,
        mode=0o440,
        payload=serialize_runtime_credential(
            service_id=SERVICE_ID,
            service_kind=KIND,
            instance_name=INSTANCE,
            bundle_generation=generation,
            values={"TUSHARE_TOKEN_MAIN": "sealed-token"},
        ),
    )


def test_the_credential_is_read_against_the_deployment_bundle_generation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The namespace the sealer used is the namespace the reader has to use (#215, #207)."""

    delivery = _sealed(monkeypatch, tmp_path / "run-credentials")

    loaded = _load(
        {"CREDENTIALS_DIRECTORY": str(delivery.directory)}, generation=BUNDLE_GENERATION
    )

    assert dict(loaded) == {"TUSHARE_TOKEN_MAIN": "sealed-token"}


def test_the_authority_chain_generation_is_still_refused(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Passing the wrong namespace must keep failing closed, not start being tolerated."""

    delivery = _sealed(monkeypatch, tmp_path / "run-credentials")

    with pytest.raises(ValueError, match="generation does not match"):
        _load(
            {"CREDENTIALS_DIRECTORY": str(delivery.directory)}, generation=AUTHORITY_GENERATION
        )


def test_without_a_deployment_generation_a_present_credential_is_refused(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Route B has no bundle, so a credential in reach is one nothing can bind."""

    delivery = _sealed(monkeypatch, tmp_path / "run-credentials")

    with pytest.raises(ValueError, match="without a deployment generation"):
        _load({"CREDENTIALS_DIRECTORY": str(delivery.directory)}, generation=None)


def test_without_a_deployment_generation_a_capability_role_refuses_outright(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Route B and a kind that needs a credential: refuse, do not degrade.

    On that route no deployment bundle exists, so no credential for this instance can have
    been sealed and none could be bound if one were handed over. That is structural rather
    than diagnosable, which is why it refuses even outside a systemd unit — where the
    delivery diagnosis has nothing to accuse and stays quiet.
    """

    monkeypatch.setattr(capabilities_module, "_SYSTEMD_CGROUP_PATH", tmp_path / "absent")

    with pytest.raises(ValueError, match="without a deployment generation"):
        _load({}, generation=None)


def test_without_a_deployment_generation_a_role_needing_nothing_still_degrades() -> None:
    """T9-6's accepted degradation survives for the eighteen kinds that carry no capability."""

    assert (
        dict(
            load_systemd_runtime_capabilities(
                RuntimeServiceKind.WATCHLIST_QUOTE_SOURCE,
                expected_service_id="source.watchlist-quote",
                expected_instance=INSTANCE,
                expected_generation=None,
                environ={},
            )
        )
        == {}
    )


def test_the_delivery_diagnosis_is_reachable_on_route_b_too(under_a_unit: Path) -> None:
    """Ordering: the two messages of the delivery check may not be shadowed by Route B.

    Putting the `expected_generation is None` branch first made both of them unreachable
    there, so a Route B capability role fell back to the silent degradation #215 is about.
    """

    delivered = under_a_unit / UNIT
    delivered.mkdir()
    (delivered / RUNTIME_CAPABILITY_CREDENTIAL_NAME).write_bytes(b"{}")

    with pytest.raises(ValueError) as raised:
        _load({}, generation=None)

    message = str(raised.value)
    assert "was not delivered to daily_close_source" in message
    assert "environment allowlist" in message


def test_the_unloaded_credential_diagnosis_is_reachable_on_route_b_too(
    under_a_unit: Path,
) -> None:
    """The other of the two, same ordering question."""

    with pytest.raises(ValueError) as raised:
        _load({}, generation=None)

    message = str(raised.value)
    assert f"systemd loaded no {RUNTIME_CAPABILITY_CREDENTIAL_NAME} for unit {UNIT}" in message
    assert "LoadCredentialEncrypted" in message


# ---------------------------------------------------------------------------------------
# The one capability the production profile deliberately does not seal
# ---------------------------------------------------------------------------------------


def test_the_minute_source_treats_its_backup_token_as_optional() -> None:
    """`market_minute_source` may hold two tokens; the profile seals only the primary.

    `CAPABILITY_KEYS` allows `TUSHARE_TOKEN_BACKUP` for this one kind, and
    `build_production_runtime_profile` declares only `TUSHARE_TOKEN_MAIN`, so nothing seals a
    backup and the role has to run without one. It does: the factory passes the empty string
    it read rather than `None`, which keeps the adapter off the settings fallback, and the
    failover branch is guarded on the token being truthy, so an absent backup is simply never
    switched to. Were that not so, the missing key would be a seventh way for the credstore
    group to fail closed, and it belongs on the record either way.
    """

    from rquant.adapter.tushare import TushareAdapter
    from rquant.runtime_deployment_profile import _REQUIRED_CAPABILITIES
    from rquant.runtime_service_builtin import _default_adapter_factory

    kind = RuntimeServiceKind.MARKET_MINUTE_SOURCE
    assert CAPABILITY_KEYS[kind] == frozenset({"TUSHARE_TOKEN_MAIN", "TUSHARE_TOKEN_BACKUP"})
    assert _REQUIRED_CAPABILITIES[kind] == frozenset({"TUSHARE_TOKEN_MAIN"})

    adapter = _default_adapter_factory({"TUSHARE_TOKEN_MAIN": "primary-only"})

    assert isinstance(adapter, TushareAdapter)
    assert adapter._primary_token == "primary-only"
    assert adapter._backup_token == ""
    assert adapter._switch_to_backup() is False


def test_the_primary_token_is_still_required(under_a_unit: Path) -> None:
    """Optional is only the backup: without the primary the factory refuses, as it always did."""

    from rquant.runtime_service_builtin import _default_adapter_factory

    with pytest.raises(RuntimeError, match="TUSHARE_TOKEN_MAIN capability is required"):
        _default_adapter_factory({})
