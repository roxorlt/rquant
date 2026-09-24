"""Package AJ acceptance: an install done days before Monday cannot wedge the Route A chain.

Issue #304, as the host has it on 2026-09-25: every install window since 09-07 acknowledged a
full set of sixteen rollout plans into DUAL_WRITE (#228), after the close, and each expired
ten minutes later. On the next session the first market-minute capture publishes to the spool
and then `commit_payload` raises `rollout deadline has expired`; every later iteration meets
its own publish (`immutable sequence already contains different content`), and `feature_live`
fails every iteration. Three things answer it, and each case here pins one of them against
the real installer, the real admission path and the real market-minute gateway:

* (a) #228 — a release whose channels keep their shape stages no plan at all;
* (b) a DUAL_WRITE plan's window opens at its producers' first dual-write record, and the
  window is checked before the producer publishes, so a closed window refuses cleanly;
* (c) the plans older installs left behind bind nothing in a newer generation, and
  `close-unchanged` closes them (store rollback only, `current` untouched) so a generation
  that *is* bound to them — the one installed on 09-24, should `current` go back to it —
  publishes on Monday too.

A real schema change still goes the whole way: dual write, consumer receipt, cutover.
"""

from __future__ import annotations

import sqlite3
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pandas as pd
import pytest

from rquant.live_contracts import LiveChannel
from rquant.live_spool import LiveBatchSpool
from rquant.market_minute_gateway import MarketMinuteGateway, MarketMinuteGatewayConfig
from rquant.runtime_builder_serving import serving_publisher_builder
from rquant.runtime_deployment_bundle import (
    SCHEMA_ROLLOUT_UNCHANGED_CLOSE_REASON,
    acknowledge_runtime_schema_rollout_preparation,
    activate_runtime_deployment_generation,
    advance_runtime_schema_rollout,
    close_unchanged_runtime_schema_rollouts,
    load_runtime_schema_rollout,
    load_runtime_schema_service_bindings,
)
from rquant.runtime_deployment_profile import (
    RuntimeDeploymentProfile,
    install_runtime_deployment_profile,
)
from rquant.runtime_schema_registry import runtime_schema_dual_write_context
from rquant.runtime_service_entrypoint import RuntimeServiceKind
from rquant.runtime_serving_snapshot import SignalDeliveryPayload
from rquant.schema_compatibility import RolloutPhase
from tests.integration.test_schema_rollout_e2e import (
    _production_environment,
    _production_profile,
    _publish_serving_owners,
    _serving_read_model_changes,
)
from tests.schema_rollout_legacy_plans import stage_pre_228_rollout_plans
from tests.unit.test_runtime_deployment_profile import (
    _disable_test_credential_sealer,
    _schema_rollout_profile,
)

pytestmark = pytest.mark.integration

MARKET_MINUTE = "runtime.market_minute.batch-envelope"
COMMITS = ("a" * 40, "b" * 40, "c" * 40)
#: the host's own timeline: plans staged and acknowledged into DUAL_WRITE after the close on
#: Thursday 2026-09-24 (deadline 16:31:32 local), the next session opening Monday 09:30
STAGED_AT = datetime(2026, 9, 24, 8, 21, 32, tzinfo=UTC)
ACKNOWLEDGED_AT = STAGED_AT + timedelta(seconds=38)
MONDAY_OPEN = datetime(2026, 9, 28, 1, 30, tzinfo=UTC)
STAGE_TIMEOUT = timedelta(seconds=600)


def _install(root: Path, commit: str, *, bootstrap: bool = False) -> tuple[object, object]:
    profile = _schema_rollout_profile(root, commit=commit)
    receipt = install_runtime_deployment_profile(
        profile,
        runtime_root=root,
        environ={"TUSHARE_TOKEN_MAIN": "secret"},
        schema_bootstrap_reason="package AJ bootstrap" if bootstrap else None,
    )
    return profile, receipt


def _minute_manifest(profile: RuntimeDeploymentProfile) -> object:
    return next(
        item
        for item in profile.manifests
        if item.service_kind is RuntimeServiceKind.MARKET_MINUTE_SOURCE
    )


def _feature_manifest(profile: RuntimeDeploymentProfile) -> object:
    return next(
        item for item in profile.manifests if item.service_kind is RuntimeServiceKind.FEATURE_LIVE
    )


def _bar(at: datetime) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "ts_code": "600000.SH",
                "trade_time": at,
                "open": 10.0,
                "high": 10.2,
                "low": 9.9,
                "close": 10.1,
                "vol": 1000.0,
                "amount": 10100.0,
            }
        ]
    )


class _Minute:
    """One market-minute source process: admitted once, then capturing bar after bar."""

    def __init__(self, root: Path, profile: RuntimeDeploymentProfile, generation_id: str) -> None:
        manifest = _minute_manifest(profile)
        self.bindings = load_runtime_schema_service_bindings(
            root,
            manifest=manifest,
            generation_id=generation_id,
            observed_at=MONDAY_OPEN - timedelta(minutes=10),
        )
        self.spool = LiveBatchSpool(root / "live" / "market-minute")
        self.bar_at = MONDAY_OPEN
        self.gateway = MarketMinuteGateway(
            spool=self.spool,
            fetcher=lambda: _bar(self.bar_at),
            config=MarketMinuteGatewayConfig(
                producer_version="package-aj",
                producer_commit=manifest.producer_commit,
            ),
        )

    def capture(self, bar_at: datetime) -> object:
        self.bar_at = bar_at
        with runtime_schema_dual_write_context(self.bindings):
            return self.gateway.capture_once(received_at=bar_at + timedelta(seconds=5))


def _stage_host_plans(root: Path, profile: RuntimeDeploymentProfile, previous, target) -> str:
    """The 09-24 window: one pre-#228 plan per policy channel, acknowledged into DUAL_WRITE."""

    plan_ids = stage_pre_228_rollout_plans(
        root,
        profile=profile,
        previous_generation_id=previous.generation_hash,
        target_generation_id=target.generation_hash,
        started_at=STAGED_AT,
    )
    acknowledge_runtime_schema_rollout_preparation(root, now=ACKNOWLEDGED_AT)
    (plan_id,) = plan_ids
    _authority, store = load_runtime_schema_rollout(root, plan_id=plan_id, read_only=True)
    assert store.get_state(plan_id).phase is RolloutPhase.DUAL_WRITE
    return plan_id


# ---------------------------------------------------------------------------------------
# (a) + (c): the v0.33.22 install over a host that carries the old plans
# ---------------------------------------------------------------------------------------


def test_a_new_generation_over_expired_superseded_plans_publishes_on_monday(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _disable_test_credential_sealer(monkeypatch)
    root = tmp_path / "runtime"
    _first_profile, first = _install(root, COMMITS[0], bootstrap=True)
    thursday_profile, thursday = _install(root, COMMITS[1])
    legacy = _stage_host_plans(root, thursday_profile, first, thursday)
    #: one store of the superseded generation in the layout an older build left behind:
    #: admission of the new generation must not even open it
    legacy_state = root / "control" / "schema-rollouts" / legacy / "state.sqlite3"
    connection = sqlite3.connect(legacy_state, isolation_level=None)
    connection.execute("PRAGMA journal_mode = WAL")
    connection.close()
    for leftover in legacy_state.parent.glob("state.sqlite3-*"):
        leftover.unlink()

    weekend_profile, weekend = _install(root, COMMITS[2])

    assert weekend.previous_generation_hash == thursday.generation_hash
    assert weekend.schema_rollout_plan_ids == ()
    minute = _Minute(root, weekend_profile, weekend.generation_hash)
    assert minute.bindings == ()
    assert (
        load_runtime_schema_service_bindings(
            root,
            manifest=_feature_manifest(weekend_profile),
            generation_id=weekend.generation_hash,
            observed_at=MONDAY_OPEN,
        )
        == ()
    )
    for index in range(15):
        capture = minute.capture(MONDAY_OPEN + timedelta(minutes=index))
        assert capture.published is True
    assert minute.spool.current(LiveChannel.MARKET_MINUTE).sequence == 14


def test_the_generation_the_old_plans_bind_publishes_after_close_unchanged(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The fallback: `current` back at the 09-24 generation, whose plan is expired DUAL_WRITE."""

    _disable_test_credential_sealer(monkeypatch)
    root = tmp_path / "runtime"
    _first_profile, first = _install(root, COMMITS[0], bootstrap=True)
    thursday_profile, thursday = _install(root, COMMITS[1])
    legacy = _stage_host_plans(root, thursday_profile, first, thursday)
    _weekend_profile, weekend = _install(root, COMMITS[2])

    preview = close_unchanged_runtime_schema_rollouts(root, now=MONDAY_OPEN, dry_run=True)
    (item,) = preview
    assert item.plan_id == legacy
    assert item.shape_unchanged is True
    assert item.closed is True
    assert item.phase_before is RolloutPhase.DUAL_WRITE
    assert item.target_is_current is False
    _authority, store = load_runtime_schema_rollout(root, plan_id=legacy, read_only=True)
    assert store.get_state(legacy).phase is RolloutPhase.DUAL_WRITE  # the preview wrote nothing

    applied = close_unchanged_runtime_schema_rollouts(root, now=MONDAY_OPEN - timedelta(hours=60))
    assert [entry.phase_after for entry in applied] == [RolloutPhase.ROLLBACK]
    assert (root / "current").readlink() == Path("generations") / weekend.generation_hash
    rollback = store.receipts(legacy)[-1]
    assert rollback.event_type == "rollback"
    assert SCHEMA_ROLLOUT_UNCHANGED_CLOSE_REASON in rollback.payload_json
    assert store.get_state(legacy).authority_declaration_fingerprint == (
        _authority.plan.old_declaration_fingerprint
    )
    #: idempotent
    repeated = close_unchanged_runtime_schema_rollouts(root, now=MONDAY_OPEN - timedelta(hours=59))
    assert [entry.skipped_reason for entry in repeated] == ["terminal"]
    assert [entry.closed for entry in repeated] == [False]

    activate_runtime_deployment_generation(
        root,
        generation_hash=thursday.generation_hash,
        expected_commit=COMMITS[1],
        expected_profile_id=str(thursday_profile.profile_id),
    )
    minute = _Minute(root, thursday_profile, thursday.generation_hash)
    assert minute.bindings == ()
    #: the channel's consumer is not asked for anything either: no receipt, no error
    assert (
        load_runtime_schema_service_bindings(
            root,
            manifest=_feature_manifest(thursday_profile),
            generation_id=thursday.generation_hash,
            observed_at=MONDAY_OPEN,
        )
        == ()
    )
    assert store.consumer_capability_receipts(legacy) == ()
    for index in range(15):
        assert minute.capture(MONDAY_OPEN + timedelta(minutes=index)).published is True


# ---------------------------------------------------------------------------------------
# (b): a plan that is still bound opens its window at the first publish, and refuses cleanly
# ---------------------------------------------------------------------------------------


def test_a_bound_plan_accepts_monday_s_first_publish_and_then_refuses_before_publishing(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The host's 09-24 generation exactly, with nobody closing its plan."""

    _disable_test_credential_sealer(monkeypatch)
    root = tmp_path / "runtime"
    _first_profile, first = _install(root, COMMITS[0], bootstrap=True)
    thursday_profile, thursday = _install(root, COMMITS[1])
    plan_id = _stage_host_plans(root, thursday_profile, first, thursday)
    _authority, store = load_runtime_schema_rollout(root, plan_id=plan_id, read_only=True)
    assert store.effective_deadline(plan_id) is None  # past 16:31:32, and still not expired

    minute = _Minute(root, thursday_profile, thursday.generation_hash)
    assert len(minute.bindings) == 1
    first_capture = minute.capture(MONDAY_OPEN)
    assert first_capture.published is True
    assert len(store.dual_write_records(plan_id)) == 1
    opened = store.dual_write_window_opened_at(plan_id)
    assert opened is not None and opened >= MONDAY_OPEN
    assert store.effective_deadline(plan_id) == opened + STAGE_TIMEOUT
    for index in range(1, 10):
        assert minute.capture(MONDAY_OPEN + timedelta(minutes=index)).published is True
    published = minute.spool.current(LiveChannel.MARKET_MINUTE).sequence
    assert published == 9

    #: past the window nobody advanced: refused before the spool is touched, every time,
    #: and never as the spool's own integrity error
    for index in (11, 12, 13):
        with pytest.raises(ValueError, match="rollout deadline has expired"):
            minute.capture(MONDAY_OPEN + timedelta(minutes=index))
        assert minute.spool.current(LiveChannel.MARKET_MINUTE).sequence == published

    #: closing it is what lets the producer go on; a producer still holding the binding is
    #: told to stop, and after its restart it is bound to nothing
    close_unchanged_runtime_schema_rollouts(root, now=MONDAY_OPEN + timedelta(minutes=13))
    with pytest.raises(RuntimeError, match="rolled-back schema producer must stop"):
        minute.capture(MONDAY_OPEN + timedelta(minutes=14))
    restarted = _Minute(root, thursday_profile, thursday.generation_hash)
    assert restarted.bindings == ()
    assert restarted.capture(MONDAY_OPEN + timedelta(minutes=15)).published is True


# ---------------------------------------------------------------------------------------
# A real schema change still takes the whole protocol, across a weekend
# ---------------------------------------------------------------------------------------


def test_a_real_schema_change_stages_one_plan_that_waits_for_monday_and_needs_every_step(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from rquant import runtime_deployment_bundle as deployment_module

    class _Transaction:
        sealed_instances: tuple[str, ...] = ()

        def commit(self) -> None:
            pass

        def rollback(self) -> None:
            pass

    class _Recovery:
        outcome = "none"
        transaction_id = None

    monkeypatch.setattr(deployment_module, "_seal_runtime_credentials", lambda _i: _Transaction())
    monkeypatch.setattr(deployment_module, "_recover_runtime_credentials", lambda **_k: _Recovery())
    root = tmp_path / "source" / "runtime"
    old_profile = _production_profile(tmp_path, commit=COMMITS[0])
    new_profile = _production_profile(tmp_path, commit=COMMITS[1])
    install_runtime_deployment_profile(
        old_profile,
        runtime_root=root,
        environ=_production_environment(),
        schema_bootstrap_reason="package AJ real-change bootstrap",
    )
    _serving_read_model_changes(monkeypatch)
    candidate = install_runtime_deployment_profile(
        new_profile,
        runtime_root=root,
        environ=_production_environment(),
        schema_rollout_started_at=STAGED_AT,
    )
    (plan_id,) = candidate.schema_rollout_plan_ids
    acknowledge_runtime_schema_rollout_preparation(root, now=ACKNOWLEDGED_AT)
    _authority, store = load_runtime_schema_rollout(root, plan_id=plan_id)
    assert store.get_state(plan_id).phase is RolloutPhase.DUAL_WRITE
    assert store.effective_deadline(plan_id) is None

    #: close-unchanged leaves a plan that protects a real change alone
    (closure,) = close_unchanged_runtime_schema_rollouts(root, now=ACKNOWLEDGED_AT, dry_run=True)
    assert closure.skipped_reason == "schema_changed"
    assert closure.closed is False

    notifier = next(
        item for item in new_profile.manifests if item.service_kind is RuntimeServiceKind.NOTIFIER
    )
    serving = next(
        item
        for item in new_profile.manifests
        if item.service_kind is RuntimeServiceKind.SERVING_PUBLISHER
    )
    #: the producer's first publish, the next session's open: accepted, and it opens the window
    (binding,) = load_runtime_schema_service_bindings(
        root, manifest=notifier, generation_id=candidate.generation_hash, observed_at=MONDAY_OPEN
    )
    prepared = binding.prepare_payload(
        SignalDeliveryPayload().model_dump(mode="json"), observed_at=MONDAY_OPEN
    )
    assert prepared is not None
    binding.commit_payload(prepared, operation_id="package-aj-monday-first-publish")
    assert store.effective_deadline(plan_id) == MONDAY_OPEN + STAGE_TIMEOUT

    state = advance_runtime_schema_rollout(
        root,
        plan_id=plan_id,
        expected_revision=store.get_state(plan_id).revision,
        target_phase=RolloutPhase.CONSUMER_ACK,
        now=MONDAY_OPEN + timedelta(seconds=3),
        operation_id="package-aj-consumer-ack",
    )
    #: the consumer's receipt is still required
    with pytest.raises(ValueError, match="cutover lacks required production consumer ACK"):
        advance_runtime_schema_rollout(
            root,
            plan_id=plan_id,
            expected_revision=state.revision,
            target_phase=RolloutPhase.CUTOVER,
            now=MONDAY_OPEN + timedelta(seconds=4),
            operation_id="package-aj-cutover-too-early",
        )
    _publish_serving_owners(serving, observed_at=MONDAY_OPEN + timedelta(seconds=4))
    consumer_bindings = load_runtime_schema_service_bindings(
        root,
        manifest=serving,
        generation_id=candidate.generation_hash,
        observed_at=MONDAY_OPEN + timedelta(seconds=5),
    )
    with runtime_schema_dual_write_context(consumer_bindings):
        serving_publisher_builder(
            snapshot_loader=None,
            clock=lambda: MONDAY_OPEN + timedelta(seconds=5),
        )(serving)()
    state = advance_runtime_schema_rollout(
        root,
        plan_id=plan_id,
        expected_revision=store.get_state(plan_id).revision,
        target_phase=RolloutPhase.CUTOVER,
        now=MONDAY_OPEN + timedelta(seconds=6),
        operation_id="package-aj-cutover",
    )
    assert state.phase is RolloutPhase.CUTOVER
    assert len(store.consumer_capability_receipts(plan_id)) == 1


def test_the_production_profile_s_sixteen_policies_stage_no_plan_for_a_commit_only_release(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The host's own profile: sixteen rollout policies, a release that changes only the commit."""

    from rquant import runtime_deployment_bundle as deployment_module
    from rquant.runtime_definition_bootstrap import plan_builtin_definitions
    from rquant.runtime_production_profile import build_production_runtime_profile
    from tests.unit.test_runtime_production_profile import _inputs as production_inputs

    class _Transaction:
        sealed_instances: tuple[str, ...] = ()

        def commit(self) -> None:
            pass

        def rollback(self) -> None:
            pass

    class _Recovery:
        outcome = "none"
        transaction_id = None

    monkeypatch.setattr(deployment_module, "_seal_runtime_credentials", lambda _i: _Transaction())
    monkeypatch.setattr(deployment_module, "_recover_runtime_credentials", lambda **_k: _Recovery())

    def full_profile(commit: str) -> RuntimeDeploymentProfile:
        payload = production_inputs(tmp_path).model_dump(mode="python")
        payload["producer_commit"] = commit
        payload["strategies"] = tuple(
            binding.model_dump(mode="python")
            for binding in plan_builtin_definitions(producer_commit=commit).strategies
        )
        return build_production_runtime_profile(payload)

    root = tmp_path / "source" / "runtime"
    previous = full_profile(COMMITS[0])
    assert len(previous.schema_rollout_policies) == 16
    install_runtime_deployment_profile(
        previous,
        runtime_root=root,
        environ=_production_environment(),
        schema_bootstrap_reason="package AJ production-profile bootstrap",
    )
    candidate = install_runtime_deployment_profile(
        full_profile(COMMITS[1]),
        runtime_root=root,
        environ=_production_environment(),
        schema_rollout_started_at=STAGED_AT,
    )

    assert candidate.previous_generation_hash is not None
    assert candidate.schema_rollout_plan_ids == ()
    rollouts = root / "control" / "schema-rollouts"
    assert not rollouts.exists() or not any(rollouts.iterdir())
