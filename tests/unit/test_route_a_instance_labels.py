"""Route A and Route B must derive the same instance-label set (package 0 evidence).

`profile_id` is `canonical_sha256(body)` over, among other things, the `instances` labels of
every `PRODUCTION_ROLE_POLICY` entry (`runtime_authority_publish.py:597-616`). Route B takes
those labels from the checkout's frozen constants (`derive_bootstrap_services`); Route A takes
them verbatim off the manifest file names of a `data/runtime` generation
(`legacy_services`), and those file names are what
`install_runtime_deployment_bundle` writes for the manifests
`build_production_runtime_profile` produced (`runtime_deployment_bundle.py:3040`,
`:2993-2995`).

If the two label sets differ, moving production onto Route A changes `profile_id` and the
publication transition hits the "profile id is not active" refusal (#190). If they agree, it
is the already-exercised "same profile, new generation" path. The scope report reasoned that
they agree; this pins it by construction instead.
"""

from __future__ import annotations

from pathlib import Path

import pytest

import rquant.runtime_authority_stage as stage_module
from rquant.runtime_deployment_bundle import _canonical_manifest, _instance_name
from rquant.runtime_deployment_profile import RuntimeDeploymentProfile
from rquant.runtime_production_profile import build_production_runtime_profile
from tests.unit.test_runtime_authority_publish import BOOTSTRAP_SERVICE_IDS, _trusted_keyring
from tests.unit.test_runtime_production_profile import COMMIT, _inputs

#: `legacy_generation_directory` accepts any 64-hex directory name; the content address of the
#: real generation plays no part in the labels, so the fixture pins a literal one.
LEGACY_GENERATION = "0" * 64
#: `instance_label(RECOVERY_SERVICE_ID)`. Both routes reach the recovery orphan through the
#: same `_orphan_services`, so a drift in that constant cancels out of the comparison below
#: and moves `profile_id` under both routes at once; the literal is what catches it.
RECOVERY_LABEL = "svc-8ba7cc292e1abf07d8dea5f9253de4fc5e7daa468ec0cac527942fb0cf165c85"


def _write_legacy_generation(root: Path, profile: RuntimeDeploymentProfile) -> Path:
    """Lay out `<root>/generations/<64hex>/manifests/` the way the bundle installer does.

    Only the two facts Route A reads are reproduced — the file name
    (`manifests/<_instance_name(service_id)>.json`, `runtime_deployment_bundle.py:3040`) and
    the canonical manifest payload (`_canonical_manifest`, `:2905-2906`) — because those are
    the two the label derivation consumes. Everything else the installer writes
    (generation-basis, runtime.env, schema contracts, credstore) is irrelevant to labels and
    would drag root-owned closure discovery into a unit test.
    """

    generation = root / "generations" / LEGACY_GENERATION
    manifests = generation / "manifests"
    manifests.mkdir(parents=True)
    for entry in profile.manifests:
        manifest, payload = _canonical_manifest(entry)
        (manifests / f"{_instance_name(manifest.service_id)}.json").write_bytes(payload)
    (root / "current").symlink_to(Path("generations") / LEGACY_GENERATION)
    return generation


@pytest.fixture
def route_a_services(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> stage_module.ServiceSet:
    _trusted_keyring(monkeypatch, tmp_path / "keyring")
    profile = build_production_runtime_profile(_inputs(tmp_path))
    legacy_root = tmp_path / "data" / "runtime"
    legacy_root.mkdir(parents=True)
    _write_legacy_generation(legacy_root, profile)
    return stage_module.legacy_services(
        legacy_root=legacy_root,
        generation="current",
        page_control_unit=stage_module.PAGE_CONTROL_UNIT,
        commit=COMMIT,
    )


@pytest.fixture
def route_b_services(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> stage_module.ServiceSet:
    _trusted_keyring(monkeypatch, tmp_path / "keyring")
    return stage_module.derive_bootstrap_services(
        page_control_unit=stage_module.PAGE_CONTROL_UNIT,
        commit=COMMIT,
    )


def test_route_a_and_route_b_agree_on_every_instance_label(
    route_a_services: stage_module.ServiceSet,
    route_b_services: stage_module.ServiceSet,
) -> None:
    """The `profile_id` input — role → sorted labels — is identical on both routes."""

    assert route_a_services.instances == route_b_services.instances


def test_route_a_and_route_b_agree_on_every_label_to_service_id_binding(
    route_a_services: stage_module.ServiceSet,
    route_b_services: stage_module.ServiceSet,
) -> None:
    """Same labels for the same service ids, so no label is a coincidental collision."""

    assert route_a_services.service_ids == route_b_services.service_ids


def test_the_shared_label_set_is_the_frozen_twenty_six_plus_two_orphans(
    route_a_services: stage_module.ServiceSet,
) -> None:
    """Route A's labels are exactly the S1 §9.3 table plus the page-control and recovery
    orphans, so the agreement above is not two derivations that are equally wrong."""

    expected = {
        stage_module.instance_label(service_id)
        for service_ids in BOOTSTRAP_SERVICE_IDS.values()
        for service_id in service_ids
    }
    assert stage_module.instance_label(stage_module.RECOVERY_SERVICE_ID) == RECOVERY_LABEL
    orphans = {
        stage_module.read_page_control_instance(stage_module.PAGE_CONTROL_UNIT),
        RECOVERY_LABEL,
    }
    assert len(expected) == 26
    assert set(route_a_services.service_ids) == expected | orphans
