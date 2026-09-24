"""Package AJ: a release that changes no channel's shape stages no rollout plan (#228).

`changed_runtime_schema_channels` compared the declarations' `schema_fingerprint`, which is
`semantic_fingerprint + producer_commit`. Every release changes `producer_commit`, so every
channel with a producer and a consumer was "changed" on every install, and the host staged
sixteen plans per release — 208 by 2026-09-25 — although no shape had moved since the first.
Each of them bound `market_minute_source` and `feature_live` to a dual-write window that had
closed before the next trading session (#304).

What decides now is `RuntimeSchemaContractBundle.channel_shape_fingerprint`: the schema facts
a plan binds (declaration semantics, the channel's physical schema, and the serving physical
schema for a channel the serving publisher consumes), and nothing about the release.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from rquant.runtime_contracts import canonical_sha256
from rquant.runtime_deployment_bundle import (
    acknowledge_runtime_schema_rollout_preparation,
    load_runtime_schema_service_bindings,
)
from rquant.runtime_deployment_profile import install_runtime_deployment_profile
from rquant.runtime_schema_registry import (
    RuntimeSchemaConsumerBinding,
    RuntimeSchemaContractBundle,
    SchemaField,
    changed_runtime_schema_channel_ids,
)
from rquant.schema_compatibility import RolloutPhase
from tests.unit.test_runtime_deployment_profile import (
    COMMIT,
    _disable_test_credential_sealer,
    _schema_rollout_profile,
)
from tests.unit.test_runtime_schema_registry import (
    NEW_COMMIT,
    OLD_COMMIT,
    _bundle,
    _replace_channel,
    _replace_declaration,
)
from tests.unit.test_runtime_schema_rollout_acknowledge import (
    Rollout,
    _run_cli,
    isolated_root_credential_sealer,  # noqa: F401 - autouse fixture, imported to apply here
    rollout,  # noqa: F401 - the host's 09-24 generation, with its pre-#228 plans
)

MARKET_MINUTE = "runtime.market_minute.batch-envelope"


def _two_sided(bundle: RuntimeSchemaContractBundle) -> tuple[str, ...]:
    return tuple(
        channel.channel_id
        for channel in bundle.channels
        if channel.producer_service_ids and channel.consumers
    )


def _with_serving_physical_schema(
    bundle: RuntimeSchemaContractBundle, fingerprint: str
) -> RuntimeSchemaContractBundle:
    return RuntimeSchemaContractBundle.create(
        producer_commit=bundle.producer_commit,
        manifest_fingerprints=bundle.manifest_fingerprints,
        channels=bundle.channels,
        serving_physical_schema_fingerprint=fingerprint,
    )


def test_a_release_that_changes_no_shape_changes_no_channel() -> None:
    previous = _bundle(OLD_COMMIT, include_serving_publisher=True)
    candidate = _bundle(NEW_COMMIT, include_serving_publisher=True)
    two_sided = _two_sided(candidate)
    assert len(two_sided) >= 10

    #: the comparison #228 was about: every two-sided declaration's `schema_fingerprint`
    #: differs across the two commits, which is what staged a plan for each of them
    assert all(
        candidate.channel(channel_id).declaration.schema_fingerprint
        != previous.channel(channel_id).declaration.schema_fingerprint
        for channel_id in two_sided
    )
    assert changed_runtime_schema_channel_ids(previous=previous, candidate=candidate) == ()
    assert all(
        candidate.channel_shape_fingerprint(channel.channel_id)
        == previous.channel_shape_fingerprint(channel.channel_id)
        for channel in candidate.channels
    )


def test_a_changed_declaration_is_reported_on_its_channel_only() -> None:
    previous = _bundle(OLD_COMMIT)
    candidate = _bundle(NEW_COMMIT)
    channel = candidate.channel(MARKET_MINUTE)
    optional = SchemaField(
        name="new_optional",
        type_name="json-schema-sha256:" + "e" * 64,
        required=False,
        introduced_in=2,
        nullable=True,
    )
    changed = _replace_declaration(
        channel,
        commit=NEW_COMMIT,
        fields=(*channel.declaration.fields, optional),
        version=2,
    )

    assert changed_runtime_schema_channel_ids(
        previous=previous, candidate=_replace_channel(candidate, changed)
    ) == (MARKET_MINUTE,)


def test_a_changed_serving_read_model_is_reported_on_the_channels_serving_consumes() -> None:
    previous = _bundle(OLD_COMMIT, include_serving_publisher=True)
    candidate = _with_serving_physical_schema(
        _bundle(NEW_COMMIT, include_serving_publisher=True),
        canonical_sha256({"serving": "one more column"}),
    )
    serving_consumed = tuple(
        channel.channel_id
        for channel in candidate.channels
        if any(binding.requires_serving_generation_ack for binding in channel.consumers)
    )
    assert serving_consumed

    assert changed_runtime_schema_channel_ids(previous=previous, candidate=candidate) == (
        serving_consumed
    )


def test_a_new_participant_is_not_a_schema_change() -> None:
    """A release that adds a consumer (a new strategy, say) moves no schema.

    The new consumer reads the declaration the old ones read, and
    `validate_runtime_schema_transition` still checks it can; a plan would only bind the
    producers to a dual-write window for nothing.
    """

    previous = _bundle(OLD_COMMIT)
    candidate = _bundle(NEW_COMMIT)
    channel = candidate.channel(MARKET_MINUTE)
    extra = RuntimeSchemaConsumerBinding(
        service_id="zz-new-consumer",
        requirement=channel.consumers[0].requirement.model_copy(
            update={"consumer_id": "zz-new-consumer"}
        ),
    )
    widened = channel.model_copy(update={"consumers": (*channel.consumers, extra)})
    channels = tuple(
        widened if item.channel_id == MARKET_MINUTE else item for item in candidate.channels
    )
    with_new_consumer = RuntimeSchemaContractBundle.create(
        producer_commit=candidate.producer_commit,
        manifest_fingerprints={**candidate.manifest_fingerprints, "zz-new-consumer": "c" * 64},
        channels=channels,
    )

    assert changed_runtime_schema_channel_ids(previous=previous, candidate=with_new_consumer) == ()


def test_installing_a_release_with_unchanged_shapes_stages_zero_plans(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The profile carries a rollout policy for the channel; the new generation gets no plan."""

    _disable_test_credential_sealer(monkeypatch)
    root = tmp_path / "runtime"
    first = install_runtime_deployment_profile(
        _schema_rollout_profile(root, commit=COMMIT),
        runtime_root=root,
        environ={"TUSHARE_TOKEN_MAIN": "secret"},
        schema_bootstrap_reason="reviewed profile bootstrap",
    )
    second = install_runtime_deployment_profile(
        _schema_rollout_profile(root, commit="b" * 40),
        runtime_root=root,
        environ={"TUSHARE_TOKEN_MAIN": "secret"},
    )

    assert second.previous_generation_hash == first.generation_hash
    assert second.schema_rollout_plan_ids == ()
    rollouts = root / "control" / "schema-rollouts"
    assert not rollouts.exists() or not any(rollouts.iterdir())


def test_close_unchanged_previews_then_closes_the_current_generation_s_old_plans(
    rollout: Rollout,  # noqa: F811 - the fixture imported above
    capsys: pytest.CaptureFixture[str],
) -> None:
    """The command the coordinator runs on the host: `--dry-run` first, then the apply.

    The fixture is the host's 09-24 generation: every two-sided channel carries a plan the
    pre-#228 installer staged, the installer's acknowledgement has carried them to
    DUAL_WRITE, and the generation is current — the one case where closing a plan reaches a
    running producer, which is why the report says so per plan.
    """

    #: the CLI stamps its own clock, so the acknowledgement has to precede it
    acknowledge_runtime_schema_rollout_preparation(rollout.root, now=datetime.now(UTC))
    before = rollout.digest()
    root = str(rollout.root)

    preview = _run_cli(
        ["runtime-schema-rollout", "close-unchanged", "--runtime-root", root, "--dry-run"],
        capsys,
    )
    assert preview["status"] == "dry_run"
    assert preview["plans"] == len(rollout.plan_ids) == preview["closed"]
    assert preview["closed_current_generation"] == len(rollout.plan_ids)
    assert preview["schema_changed"] == 0
    assert rollout.digest() == before
    assert all(rollout.phase(plan_id) is RolloutPhase.DUAL_WRITE for plan_id in rollout.plan_ids)

    applied = _run_cli(
        ["runtime-schema-rollout", "close-unchanged", "--runtime-root", root], capsys
    )
    assert applied["status"] == "applied"
    assert applied["closed"] == len(rollout.plan_ids)
    assert {item["phase_after"] for item in applied["closures"]} == {"rollback"}
    assert all(rollout.phase(plan_id) is RolloutPhase.ROLLBACK for plan_id in rollout.plan_ids)
    assert (rollout.root / "current").readlink() == Path("generations") / rollout.generation_id

    repeated = _run_cli(
        ["runtime-schema-rollout", "close-unchanged", "--runtime-root", root], capsys
    )
    assert repeated["closed"] == 0
    assert {item["skipped_reason"] for item in repeated["closures"]} == {"terminal"}

    #: and the generation's producers are bound to nothing any more
    for manifest in rollout.manifests:
        assert (
            load_runtime_schema_service_bindings(
                rollout.root,
                manifest=manifest,
                generation_id=rollout.generation_id,
                observed_at=datetime.now(UTC) + timedelta(days=3),
            )
            == ()
        )
