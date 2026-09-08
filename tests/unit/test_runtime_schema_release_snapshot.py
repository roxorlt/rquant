"""Cross-release gate: the contracts a shipped generation published must stay readable.

Every other schema test in this repository builds both sides of a transition from the
code in the working tree, so a change that rewrites a payload's shape rewrites the
"previous" bundle too and the transition looks clean. The installer does not work that
way: `rquant runtime-deployment-profile` compares the bundle built from the *new* code
against the `schema-contracts.json` the *installed* generation wrote. On 2026-09-08 that
comparison refused the v0.33.2 installer over the generation published by v0.33.1,
because three fields added to `RuntimeServiceHeartbeat` -- a model that had been embedded
whole into the serving health payload -- moved all nine field hashes of
`runtime.serving.runtime-health` without anybody touching that channel (#237).

The fixture below is the real `schema-contracts.json` of that production generation. It
is the only "previous" side in the suite that a code change cannot silently rewrite.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from rquant.runtime_schema_registry import (
    _SUPPORTED_KINDS,
    RuntimeSchemaCompatibilityError,
    RuntimeSchemaContractBundle,
    build_runtime_schema_contract_bundle,
    parse_runtime_schema_contract_bundle,
    validate_runtime_schema_transition,
)
from rquant.runtime_service_control import RuntimeServicePlane
from rquant.runtime_service_entrypoint import RuntimeServiceKind, RuntimeServiceManifest

SNAPSHOT_PATH = (
    Path(__file__).resolve().parents[1] / "fixtures" / "runtime-schema-contracts" / "v0.33.1.json"
)
HEALTH_CHANNEL_ID = "runtime.serving.runtime-health"
HARNESS_COMMIT = "c" * 40

_SERVING_KINDS = frozenset(
    {
        RuntimeServiceKind.RUNTIME_HEALTH_PUBLISHER,
        RuntimeServiceKind.SERVING_PUBLISHER,
    }
)
_RESEARCH_KINDS = frozenset(
    {
        RuntimeServiceKind.LAB_JOBS_PUBLISHER,
        RuntimeServiceKind.LAB_ARTIFACT_CATALOG,
        RuntimeServiceKind.PROMOTIONS_PUBLISHER,
    }
)

_REFRESH_HINT = """
Two -- and only two -- changes are allowed to move a published field hash:

  1. The channel's payload started embedding a model that grows on its own -- one written
     for a service's own use rather than for publication. A field added to that model
     rewrites the whole channel, because a field's hash covers the payload's entire
     $defs. Restore (or add) a frozen projection so the published shape stops tracking
     it. This is what #237 was; it is the likeliest thing to have just happened.
  2. The channel's schema_version was deliberately bumped and the change is being driven
     through a real rollout (PREPARE -> DUAL_WRITE -> ...), with the installed generation
     able to read what the new producer writes. In that case the integrator refreshes
     tests/fixtures/runtime-schema-contracts/ with the schema-contracts.json of the
     generation that is actually installed in production, as part of that rollout.

Editing the fixture to make this test green is neither of those. The fixture is a record
of what production published; it is not an expected value to be updated.
"""


def _plane(kind: RuntimeServiceKind) -> RuntimeServicePlane:
    if kind in _SERVING_KINDS:
        return RuntimeServicePlane.SERVING
    if kind in _RESEARCH_KINDS:
        return RuntimeServicePlane.RESEARCH
    return RuntimeServicePlane.LIVE


def _harness_manifests() -> tuple[RuntimeServiceManifest, ...]:
    """One manifest per service kind, so every channel gets producers and consumers.

    The snapshot's own manifest fingerprints cannot be reused: a bundle carries service
    *ids*, and the code cannot recover the kind behind a production id. Covering every
    kind is stronger than reproducing production's roster -- it makes every channel's
    producer and consumer bindings non-empty, so no compatibility branch in
    `validate_runtime_schema_transition` is skipped for want of a participant.
    """

    return tuple(
        RuntimeServiceManifest(
            service_id=f"harness-{kind.value.replace('_', '-')}",
            service_kind=kind,
            plane=_plane(kind),
            interval_seconds=1,
            stale_after_seconds=30,
            producer_commit=HARNESS_COMMIT,
            settings={},
        )
        for kind in sorted(_SUPPORTED_KINDS, key=lambda item: item.value)
    )


@pytest.fixture(scope="module")
def released_bundle() -> RuntimeSchemaContractBundle:
    return parse_runtime_schema_contract_bundle(SNAPSHOT_PATH.read_bytes())


@pytest.fixture(scope="module")
def head_bundle() -> RuntimeSchemaContractBundle:
    return build_runtime_schema_contract_bundle(
        _harness_manifests(),
        producer_commit=HARNESS_COMMIT,
    )


def test_released_snapshot_is_the_third_production_generation() -> None:
    raw = json.loads(SNAPSHOT_PATH.read_text())
    assert raw["schema_version"] == 2
    assert raw["producer_commit"] == "a0bbb4c291797eb086fb2f2a9fc50a91cc264095"
    assert len(raw["channels"]) == 21


def test_head_serving_health_fields_match_the_released_snapshot(
    released_bundle: RuntimeSchemaContractBundle,
    head_bundle: RuntimeSchemaContractBundle,
) -> None:
    released = {
        field.name: field.type_name
        for field in released_bundle.channel(HEALTH_CHANNEL_ID).declaration.fields
    }
    current = {
        field.name: field.type_name
        for field in head_bundle.channel(HEALTH_CHANNEL_ID).declaration.fields
    }
    moved = sorted(name for name in released if released[name] != current.get(name))
    assert not moved, (
        f"{HEALTH_CHANNEL_ID} field hashes moved without a schema version bump: "
        + ", ".join(moved)
        + "\n\nThe hash of one field covers the payload's whole $defs, so an unrelated "
        "nested model that the payload embeds moves every field at once -- which is "
        "why this list is probably all nine of them.\n" + _REFRESH_HINT
    )
    assert current == released


def test_every_channel_transitions_from_the_released_snapshot(
    released_bundle: RuntimeSchemaContractBundle,
    head_bundle: RuntimeSchemaContractBundle,
) -> None:
    """What the installer does: released generation as producer of record, HEAD as candidate.

    A failure here is the failure an operator sees as
    `RuntimeSchemaCompatibilityError` from `rquant runtime-deployment-profile`, before
    anything is installed.
    """

    try:
        validate_runtime_schema_transition(
            previous=released_bundle,
            candidate=head_bundle,
        )
    except RuntimeSchemaCompatibilityError as exc:
        # The registry names the channel and lists the hashes; it has no way to say what
        # a developer should do about it. The nine-hash test above carries that guidance
        # for the one channel it watches -- the other twenty get it here.
        pytest.fail(f"{exc}\n{_REFRESH_HINT}")


def test_released_snapshot_covers_the_whole_channel_catalog(
    released_bundle: RuntimeSchemaContractBundle,
    head_bundle: RuntimeSchemaContractBundle,
) -> None:
    released = {channel.channel_id for channel in released_bundle.channels}
    current = {channel.channel_id for channel in head_bundle.channels}
    assert released == current, (
        "the released snapshot no longer describes the current channel catalog; a new "
        "channel needs a snapshot taken from a generation that already publishes it"
    )
