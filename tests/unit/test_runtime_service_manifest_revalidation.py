"""#218 A: the profile manifests the strategy signer revalidates are already frozen models.

`build_runtime_strategy_completion_attestation_signer` used to hand every entry of
`profile.manifests` straight to `RuntimeServiceManifest.model_validate`. Those entries are
already validated model instances, `RuntimeContractModel` sets
`revalidate_instances="always"`, and the manifest's own `freeze_settings` has turned every
nested list into a tuple and every nested dict into a `MappingProxyType` — neither of which
is a `JsonValue`. Nine of the production profile's manifests carry nested settings, so the
step failed closed for all three `strategy_live` services on the 2026-09-07 Route A host
with `strategy completion signer profile contains invalid manifests`.

The trap itself is pinned here rather than assumed, so a future pydantic that stops
re-validating instances makes this file say so instead of quietly removing the cover.
"""

from __future__ import annotations

import ast
import hashlib
from pathlib import Path
from types import MappingProxyType, SimpleNamespace

import pytest

from rquant.runtime_deployment_profile import PRODUCTION_SHADOW_SIGNER_COMMAND
from rquant.runtime_service_entrypoint import RuntimeServiceKind, RuntimeServiceManifest
from rquant.runtime_service_main import build_runtime_strategy_completion_attestation_signer
from tests.shadow_ed25519_support import create_shadow_ed25519_test_authority

COMMIT = "a" * 40

#: The shape `runtime_production_profile` gives `reference_slow_source.limits` and
#: `signal_router.sources`: a mapping and a list one level down from `settings`.
NESTED_SETTINGS: dict[str, object] = {
    "limits": {"max_rows": 1000, "window": ["09:30", "11:30"]},
    "sources": [{"service_id": "svc-a", "weight": 1}],
    "scalar": "kept",
}


def _instance_id(service_id: str) -> str:
    return "svc-" + hashlib.sha256(service_id.encode()).hexdigest()


def _manifest(
    service_id: str,
    kind: RuntimeServiceKind,
    settings: dict[str, object],
) -> RuntimeServiceManifest:
    return RuntimeServiceManifest(
        service_id=service_id,
        service_kind=kind,
        plane="live",
        interval_seconds=2,
        stale_after_seconds=30,
        producer_commit=COMMIT,
        settings=settings,
    )


def _strategy_manifest(tmp_path: Path) -> RuntimeServiceManifest:
    """A strategy manifest whose own settings are all scalars, as the real one's are."""

    return _manifest(
        "strategy.n_shape.v1",
        RuntimeServiceKind.STRATEGY_LIVE,
        {
            "feature_spool_root": str(tmp_path / "live" / "features"),
            "runner_state_path": str(tmp_path / "live" / "runner.sqlite3"),
            "strategy_id": "n_shape",
            "strategy_version": 1,
            "producer_instance_id": _instance_id("strategy.n_shape.v1"),
        },
    )


def _profile(
    manifests: tuple[object, ...],
    *,
    public_key_pem: str,
) -> SimpleNamespace:
    return SimpleNamespace(
        producer_commit=COMMIT,
        profile_id="p" * 64,
        manifests=manifests,
        shadow=SimpleNamespace(
            completion_active_key_id="shadow-test-v1",
            completion_active_public_key_pem=public_key_pem,
            completion_previous_public_key_pems={},
            signer_command=PRODUCTION_SHADOW_SIGNER_COMMAND,
            timeout_seconds=5.0,
        ),
    )


@pytest.fixture
def public_key_pem(tmp_path: Path) -> str:
    authority = create_shadow_ed25519_test_authority(tmp_path / "shadow-keys")
    return authority.keyring._keys[authority.keyring.active_key_id].decode("utf-8")


def _bind_profile(monkeypatch: pytest.MonkeyPatch, profile: object) -> None:
    monkeypatch.setattr(
        "rquant.runtime_service_main.load_current_runtime_deployment_profile",
        lambda _root: profile,
        raising=False,
    )


# ---------------------------------------------------------------------------------------
# The trap, pinned
# ---------------------------------------------------------------------------------------


def test_a_manifest_freezes_its_nested_settings_into_non_json_containers() -> None:
    """The premise: what the loader hands back is not a JSON-shaped mapping any more."""

    manifest = _manifest("svc-nested", RuntimeServiceKind.SIGNAL_ROUTER, NESTED_SETTINGS)

    assert isinstance(manifest.settings, MappingProxyType)
    assert isinstance(manifest.settings["limits"], MappingProxyType)
    assert isinstance(manifest.settings["limits"]["window"], tuple)
    assert isinstance(manifest.settings["sources"], tuple)


def test_revalidating_a_frozen_manifest_instance_is_what_failed_on_the_host() -> None:
    """`model_validate(<instance>)` still refuses the frozen containers, and says why."""

    manifest = _manifest("svc-nested", RuntimeServiceKind.SIGNAL_ROUTER, NESTED_SETTINGS)

    with pytest.raises(ValueError) as raised:
        RuntimeServiceManifest.model_validate(manifest)
    message = str(raised.value)
    assert "invalid-json-value" in message
    assert "settings.limits" in message

    #: and the thaw the fix performs is both accepted and identity preserving
    thawed = RuntimeServiceManifest.model_validate(manifest.model_dump(mode="json"))
    assert thawed.manifest_fingerprint == manifest.manifest_fingerprint
    assert thawed.settings == manifest.settings


# ---------------------------------------------------------------------------------------
# The regression: the signer opens over a profile shaped like the production one
# ---------------------------------------------------------------------------------------


def test_the_signer_opens_over_a_profile_whose_other_manifests_carry_nested_settings(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    public_key_pem: str,
) -> None:
    """The host's failure, reproduced in a unit: the strategy's own settings are scalars and
    it is a *sibling* manifest that used to abort the loop."""

    strategy = _strategy_manifest(tmp_path)
    siblings = (
        _manifest("reference.slow.source", RuntimeServiceKind.REFERENCE_SLOW_SOURCE, {
            "limits": NESTED_SETTINGS["limits"],
        }),
        _manifest("signal.router", RuntimeServiceKind.SIGNAL_ROUTER, {
            "sources": NESTED_SETTINGS["sources"],
        }),
    )
    _bind_profile(monkeypatch, _profile((*siblings, strategy), public_key_pem=public_key_pem))

    signer, key_id = build_runtime_strategy_completion_attestation_signer(
        tmp_path, manifest=strategy
    )

    assert key_id == "shadow-test-v1"
    assert signer.key_id == "shadow-test-v1"


def test_the_fingerprint_binding_after_the_thaw_is_the_manifest_that_was_asked_for(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    public_key_pem: str,
) -> None:
    """The thaw must not smuggle in a different manifest: the binding check still refuses a
    profile whose entry for this service id has another fingerprint."""

    strategy = _strategy_manifest(tmp_path)
    impostor = strategy.model_copy(
        update={"settings": {**dict(strategy.settings), "strategy_version": 2}}
    )
    impostor = RuntimeServiceManifest.model_validate(impostor.model_dump(mode="json"))
    assert impostor.manifest_fingerprint != strategy.manifest_fingerprint
    _bind_profile(monkeypatch, _profile((impostor,), public_key_pem=public_key_pem))

    with pytest.raises(ValueError, match="does not bind this manifest"):
        build_runtime_strategy_completion_attestation_signer(tmp_path, manifest=strategy)


def test_a_profile_entry_that_is_not_a_manifest_is_still_revalidated(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    public_key_pem: str,
) -> None:
    """The defensive re-validation is kept for anything the loader did not already type."""

    strategy = _strategy_manifest(tmp_path)
    junk = {"service_id": "svc-broken"}
    _bind_profile(monkeypatch, _profile((junk, strategy), public_key_pem=public_key_pem))

    with pytest.raises(ValueError, match="contains invalid manifests"):
        build_runtime_strategy_completion_attestation_signer(tmp_path, manifest=strategy)


# ---------------------------------------------------------------------------------------
# The static guard: nobody may reintroduce the call shape
# ---------------------------------------------------------------------------------------

#: `(file, argument)` sites allowed to pass a bare name that no visible `model_dump`
#: produced, each with the reason it is not the trap. The one entry takes a row decoded out
#: of a canonical JSON document, never a model this process already built, so no frozen
#: container can reach the validator.
_ALLOWED_BARE_ARGUMENTS = {
    ("signal_family_root_verifier.py", "row"),
}


def _names_bound_to_a_thaw(tree: ast.Module) -> set[str]:
    """Names assigned somewhere in the file from an expression that calls `model_dump`."""

    thawed: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Assign):
            targets, value = node.targets, node.value
        elif isinstance(node, ast.AnnAssign) and node.value is not None:
            targets, value = [node.target], node.value
        else:
            continue
        if ".model_dump(" not in ast.unparse(value):
            continue
        for target in targets:
            if isinstance(target, ast.Name):
                thawed.add(target.id)
    return thawed


def test_no_module_hands_a_frozen_manifest_straight_to_model_validate() -> None:
    """A repository-wide scan for the call shape that produced #218.

    `RuntimeServiceManifest.model_validate(x)` is safe only when `x` came out of JSON or out
    of `model_dump`. A bare name that no `model_dump` in the file produced is the shape that
    let an already-frozen instance through, so every such site has to be named above with
    its reason.
    """

    source_root = Path(__file__).resolve().parents[2] / "src" / "rquant"
    assert source_root.is_dir()
    offenders: list[str] = []
    scanned = 0
    for path in sorted(source_root.rglob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        thawed = _names_bound_to_a_thaw(tree)
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Attribute):
                continue
            if node.func.attr != "model_validate" or len(node.args) != 1:
                continue
            if ast.unparse(node.func.value).split(".")[-1] != "RuntimeServiceManifest":
                continue
            scanned += 1
            argument = node.args[0]
            if isinstance(argument, ast.Call):
                continue  # `.model_dump(...)` or another explicit thaw
            text = ast.unparse(argument)
            if isinstance(argument, ast.Name) and argument.id in thawed:
                continue
            if (path.name, text) in _ALLOWED_BARE_ARGUMENTS:
                continue
            offenders.append(f"{path}:{node.lineno} RuntimeServiceManifest.model_validate({text})")
    assert scanned >= 2, "the scan found no call sites at all; the shape moved"
    assert offenders == [], offenders
