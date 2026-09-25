"""Rehearsal of 2026-09-28: the Route A notifier leaves shadow for real delivery, in place.

The owner's ruling (2026-09-25): when the 09:31 and 09:52 checkpoints pass, the notifier
starts sending the same day. The window that does it (DEPLOY.md, "2026-09-28 · 待执行 ·
notifier 切正式推送") is **not** a code release -- the producer commit stays the installed
v0.33.22 -- and it restarts **one** unit, not twenty. This file is that window, run against
the trading-day world of `test_route_a_trading_day_full_chain_e2e.py` (two installed
bundle generations, a real published authority chain, every role inside its own unit's
`ReadWritePaths`), in the order the operator types it:

1. the day runs in `shadow` until a signal has been routed and consumed -- `shadow:`
   receipts, `notifier:shadow_transport`, nothing on the wire;
2. `scripts/notifier_delivery_cutover.py set-mode --from shadow --to live` rewrites the one
   key of a real canonical inputs document, which `load_production_runtime_profile_inputs`
   then accepts (the recipe it replaces in DEPLOY.md did not produce canonical JSON);
3. `runtime-production-prerequisites` / `runtime-production-profile` build the next
   profile from it, `diff-profiles` finds exactly one difference, `runtime-deployment-
   profile` installs the third bundle generation over the second, `diff-generations` finds
   exactly one differing manifest, `acknowledge` finds nothing to do;
4. `runtime-authority-stage --legacy-generation current` stages sequence 2 under **the same
   authority profile id**, and the root publish commits it -- #190 compares the installed
   closure profile, whose id depends on the interpreter closure and the instance labels,
   and the delivery mode moves neither;
5. only the notifier restarts. It comes up live, with the same service spec (the spec is
   `(service_id, plane, stale_after, producer_commit)`, not the settings, so the #270
   refusal of a stale heartbeat cannot fire), leaves every row it delivered in shadow alone,
   and delivers the first signal routed after the switch through the real
   `RecipientScopedNotificationProvider` -> `ExistingClientNotificationTransport` ->
   `PushDeerClient` -- with `requests.post` answered by a recorder;
6. and back: the authority's single-level rollback plus re-applying the Friday profile
   return the notifier to shadow, and a later release still publishes over that.

**Nothing here may reach a provider.** Two independent layers make sure of it: the
recorder that answers `requests.post` inside `rquant.notify.client`, and
`tests.support.outbound_network_guard`, an audit hook that refuses and records every DNS
lookup and every non-`AF_UNIX` connect the process attempts while the test runs. The guard
is asserted empty at the end of every case, and the last case is its negative control: the
real client, let through, is caught by the guard before a byte leaves.
"""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import subprocess
import sys
from collections.abc import Iterator
from contextlib import closing
from datetime import datetime, timedelta
from datetime import time as clock_time
from pathlib import Path
from typing import Any

import pandas as pd
import pytest

import rquant.notify.client as push_client_module
from rquant.live_spool import LiveBatchSpool
from rquant.market_minute_gateway import MarketMinuteGateway, MarketMinuteGatewayConfig
from rquant.runtime_authority_publish import rollback_authority
from rquant.runtime_deployment_bundle import acknowledge_runtime_schema_rollout_preparation
from rquant.runtime_deployment_profile import install_runtime_deployment_profile
from rquant.runtime_production_profile import (
    build_production_runtime_profile,
    install_production_runtime_prerequisites,
    load_production_runtime_profile_inputs,
    publish_production_runtime_profile,
)
from rquant.strategy_candidate_snapshot import StrategyCandidateSnapshotSpool
from rquant.strict_json import canonical_json_bytes
from tests.integration.test_route_a_all_roles_sandbox_e2e import instance_of
from tests.integration.test_route_a_legacy_binding_e2e import RouteAWorld
from tests.integration.test_route_a_trading_day_full_chain_e2e import (
    CANDIDATES_CAPTURED_AT,
    CHAIN_ORDER,
    CODES,
    HOP_CLOCKS,
    N_SHAPE_SERVICE_ID,
    NOTIFIER_ROLE,
    SIGNAL_CODE,
    TRADE_DATE,
    Hop,
    _at,
    _candidate_rows,
    build_trading_day_chain,
    deliver_credentials,
    manifest_of,
    minute_history_parquet,  # noqa: F401 -- the session-scoped fixture, reused verbatim
    run_at,
    runner_signal_count,
    setting_of,
)
from tests.support import outbound_network_guard

pytestmark = pytest.mark.integration

REPO_ROOT = Path(__file__).resolve().parents[2]
CUTOVER_SCRIPT = REPO_ROOT / "scripts" / "notifier_delivery_cutover.py"
NOTIFIER_SERVICE_ID = "notifier.admin.shadow.v1"
#: the code that only trades after the switch: it has no bar before the switch, and its
#: first bars clear the candidate's rebased reference high (a session low under the
#: reference close would disqualify it for the rest of the session)
SECOND_CODE = next(code for code in CODES if code != SIGNAL_CODE)
assert SECOND_CODE == "000001.SZ"

#: The switch runs after the 09:52 checkpoint, as the owner ruled.
SWITCH_AT = _at(9, 55, 7)
#: The first round of the live notifier, before anything new has been routed.
LIVE_FIRST_ROUND = _at(9, 56, 13)
#: The post-switch minute: three bars from 09:57, received once the last one is complete.
POST_SWITCH_BARS_FROM = clock_time(9, 57)
POST_SWITCH_RECEIVED_AT = _at(9, 59, 11)
POST_SWITCH_CLOCKS: dict[str, datetime] = {
    "runtime_health_publisher": _at(9, 59, 17),
    "paper_constraint_publisher": _at(9, 59, 29),
    "feature_live": _at(9, 59, 37),
    "strategy_live": _at(9, 59, 51),
    "signal_router": _at(10, 0, 3),
    "paper_broker": _at(10, 0, 19),
    "notifier": _at(10, 0, 37),
    "serving_publisher": _at(10, 0, 53),
}
ROLLBACK_AT = _at(10, 7, 41)
SHADOW_AGAIN_ROUND = _at(10, 9, 3)

#: The host's `PUSHDEER_KEYS` carries two devices (CLAUDE.md: the owner's iPhone and Mac)
#: and no `PUSHDEER_RECIPIENT_IDS`, so the notifier infers `admin.device-01/-02`, migrates
#: every routed `admin` row onto both, and says so on its heartbeat in every mode.
PUSHDEER_DEVICE_KEYS = ("pushdeer-iphone-rehearsal", "pushdeer-mac-rehearsal")
DEVICE_KEY = {
    f"admin.device-{index:02d}": key for index, key in enumerate(PUSHDEER_DEVICE_KEYS, start=1)
}
INFERRED = "notifier:recipient_ids_inferred:pushdeer"
#: What a live notifier's heartbeat may carry: the two informational reasons of a
#: two-device credential. Anything else -- above all `shadow_transport`,
#: `confirmed_failures:*`, `unknown_outcomes:*`, `not_attempted:*` -- is a stop condition.
LIVE_REASON_PREFIXES = (INFERRED, "notifier:recipient_migration:")
TITLES = {"watch": "重点观察", "b_intent": "买入观察"}


def assert_live_reasons(reasons: tuple[str, ...] | None) -> None:
    reasons = tuple(reasons or ())
    assert INFERRED in reasons, reasons
    assert all(reason.startswith(LIVE_REASON_PREFIXES) for reason in reasons), reasons


# ---------------------------------------------------------------------------------------
# The two layers that keep a live notifier off the network
# ---------------------------------------------------------------------------------------


class _PushRecorder:
    """Answers `requests.post` inside `rquant.notify.client` the way PushDeer answers."""

    def __init__(self) -> None:
        self.posts: list[dict[str, Any]] = []

    def post(self, url: str, *, data: Any = None, json: Any = None, timeout: Any = None) -> Any:
        body = dict(data or json or {})
        self.posts.append({"url": url, "body": body, "timeout": timeout})
        code = 200 if "pushplus" in url else 0

        class _Response:
            @staticmethod
            def json() -> dict[str, Any]:
                return {"code": code, "content": {"result": ["recorded"]}}

        return _Response()


@pytest.fixture
def network_guard() -> Iterator[outbound_network_guard.OutboundNetworkGuard]:
    guard = outbound_network_guard.arm()
    try:
        yield guard
    finally:
        outbound_network_guard.disarm(guard)
    assert guard.attempts == [], f"the rehearsal attempted outbound network: {guard.attempts}"


@pytest.fixture
def push_recorder(
    monkeypatch: pytest.MonkeyPatch,
    network_guard: outbound_network_guard.OutboundNetworkGuard,
) -> _PushRecorder:
    del network_guard  # armed first, so nothing below runs unguarded
    recorder = _PushRecorder()
    monkeypatch.setattr(push_client_module, "requests", recorder)
    return recorder


# ---------------------------------------------------------------------------------------
# What the harness puts on disk (two codes, two minutes)
# ---------------------------------------------------------------------------------------


def publish_two_candidates(route: RouteAWorld) -> None:
    """Today's candidate documents, as the trading-day file writes them, for two codes."""

    for manifest in route.profile.manifests:
        if manifest.service_kind.value != "candidate_publisher":
            continue
        strategy_id = str(manifest.settings["strategy_id"])
        schema = dict(manifest.settings["static_feature_schema"])
        (first,) = _candidate_rows(
            strategy_id=strategy_id,
            strategy_version=str(manifest.settings["strategy_version"]),
            schema=schema,
            trade_date=TRADE_DATE,
            captured_at=CANDIDATES_CAPTURED_AT,
        )
        rows = (first, first.model_copy(update={"candidate_id": SECOND_CODE}))
        StrategyCandidateSnapshotSpool(
            Path(str(manifest.settings["snapshot_root"]))
        ).publish_strategy_records(
            strategy_id=strategy_id,
            strategy_version=str(manifest.settings["strategy_version"]),
            definition_fingerprint=str(manifest.settings["definition_fingerprint"]),
            executable_fingerprint=str(manifest.settings["executable_fingerprint"]),
            candidate_schema_fingerprint=str(manifest.settings["candidate_schema_fingerprint"]),
            static_feature_schema=schema,
            source_snapshot_ids={"candidate_input": "1" * 64},
            trade_date=TRADE_DATE,
            captured_at=CANDIDATES_CAPTURED_AT,
            producer_commit=manifest.producer_commit,
            rows=rows,
        )


def publish_minute_batch_at(
    route: RouteAWorld,
    *,
    bars_from: clock_time,
    received_at: datetime,
    closes: dict[str, float],
) -> Any:
    """`publish_minute_batch` of the trading-day file, with the bars placed at `bars_from`."""

    spool_root = setting_of(route, "feature.intraday-pit.v1", "raw_spool_root")

    def fetch() -> pd.DataFrame:
        rows: list[dict[str, Any]] = []
        for code, close in closes.items():
            for step, price in enumerate((close - 0.06, close - 0.03, close)):
                stamp = datetime.combine(TRADE_DATE, bars_from) + timedelta(minutes=step)
                volume = 5_000.0 + 100.0 * step
                rows.append(
                    {
                        "ts_code": code,
                        "trade_time": stamp.strftime("%Y-%m-%d %H:%M:%S"),
                        "open": round(price - 0.01, 4),
                        "high": round(price + 0.01, 4),
                        "low": round(price - 0.02, 4),
                        "close": round(price, 4),
                        "vol": volume,
                        "amount": round(volume * price, 4),
                    }
                )
        return pd.DataFrame(rows)

    capture = MarketMinuteGateway(
        spool=LiveBatchSpool(spool_root),
        fetcher=fetch,
        config=MarketMinuteGatewayConfig(
            producer_version="market-minute-source-v1",
            producer_commit=route.world.commit,
        ),
    ).capture_once(received_at=received_at)
    assert capture.published is True, capture
    return capture


def drive(
    route: RouteAWorld,
    credentials: dict[str, Path],
    clocks: dict[str, datetime],
    *,
    roles: tuple[tuple[str, str], ...] = CHAIN_ORDER,
) -> dict[str, Any]:
    hops: list[Hop] = []
    runs: dict[str, Any] = {}
    for role, label in roles:
        for instance in instance_of(route, role):
            run = run_at(
                route,
                role,
                instance=instance,
                now=clocks[role],
                credentials=credentials.get(instance),
                label=label,
                hops=hops,
            )
            assert run.refusal is None, (role, run.traceback)
            runs.setdefault(label, []).append(run)
    return runs


def run_notifier(route: RouteAWorld, credentials: dict[str, Path], now: datetime) -> Any:
    (instance,) = instance_of(route, NOTIFIER_ROLE)
    run = run_at(
        route, NOTIFIER_ROLE, instance=instance, now=now, credentials=credentials[instance]
    )
    assert run.refusal is None, run.traceback
    assert run.entered, run
    return run


# ---------------------------------------------------------------------------------------
# Reading the notifier's own store
# ---------------------------------------------------------------------------------------


def notifier_outbox(route: RouteAWorld) -> dict[str, dict[str, Any]]:
    path = setting_of(route, NOTIFIER_SERVICE_ID, "notification_state_path")
    with closing(sqlite3.connect(f"file:{path}?mode=ro", uri=True)) as connection:
        connection.row_factory = sqlite3.Row
        rows = connection.execute(
            "SELECT outbox_id, signal_id, recipient_id, channel, status, attempt_count, "
            "created_at FROM delivery_outbox"
        ).fetchall()
        attempts = connection.execute(
            "SELECT outbox_id, attempt_no, provider_receipt FROM delivery_attempt"
        ).fetchall()
        signals = {
            str(row["signal_id"]): json.loads(row["payload_json"])
            for row in connection.execute("SELECT signal_id, payload_json FROM signal_envelope")
        }
    receipts: dict[str, list[str | None]] = {}
    for attempt in attempts:
        receipts.setdefault(str(attempt["outbox_id"]), []).append(attempt["provider_receipt"])
    return {
        str(row["outbox_id"]): {
            **dict(row),
            "receipts": receipts.get(str(row["outbox_id"]), []),
            "candidate_id": signals.get(str(row["signal_id"]), {}).get("candidate_id"),
            "strategy_id": signals.get(str(row["signal_id"]), {}).get("strategy_id"),
            "action": signals.get(str(row["signal_id"]), {}).get("action"),
        }
        for row in rows
    }


# ---------------------------------------------------------------------------------------
# The operator's steps
# ---------------------------------------------------------------------------------------


def cutover(*arguments: str) -> tuple[int, dict[str, Any] | None, str]:
    """`scripts/notifier_delivery_cutover.py`, run the way the operator runs it."""

    completed = subprocess.run(
        [sys.executable, str(CUTOVER_SCRIPT), *arguments],
        capture_output=True,
        text=True,
        check=False,
        env={"PATH": os.environ.get("PATH", "/usr/bin:/bin"), "LANG": "C"},
    )
    summary = json.loads(completed.stdout) if completed.stdout.strip() else None
    return completed.returncode, summary, completed.stderr


def write_inputs_document(inputs: Any, path: Path) -> Path:
    """The document the generator writes: canonical bytes of `inputs`, private to its owner."""

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(canonical_json_bytes(inputs.model_dump(mode="json")))
    path.chmod(0o600)
    return path


def publish_profile(profile: Any, inputs: Any, directory: Path) -> Path:
    directory.mkdir(parents=True, exist_ok=True)
    directory.chmod(0o700)
    return publish_production_runtime_profile(
        profile,
        directory / f"{profile.profile_id}.json",
        production_runtime_root=inputs.runtime_root,
    )


class Cutover:
    """What the switch produced, for the cases to assert on."""

    def __init__(self) -> None:
        self.inputs_path: Path | None = None
        self.shadow_profile: Any = None
        self.live_profile: Any = None
        self.shadow_profile_path: Path | None = None
        self.live_profile_path: Path | None = None
        self.set_mode: dict[str, Any] | None = None
        self.profile_diff: dict[str, Any] | None = None
        self.generation_diff: dict[str, Any] | None = None
        self.shadow_plan: Any = None
        self.live_plan: Any = None
        self.receipt: Any = None
        self.acknowledged: tuple[Any, ...] = ()
        self.published: dict[str, object] | None = None


def switch_to_live(route: RouteAWorld, bundle: dict[str, Any], tmp_path: Path) -> Cutover:
    """Steps 2-4 of the DEPLOY entry, with the same code, over the running shadow world."""

    result = Cutover()
    inputs = bundle["inputs"]
    commit = route.world.commit
    result.shadow_profile = route.profile
    result.shadow_plan = route.plan
    profiles = tmp_path / "host" / "data" / "runtime-profiles"
    result.shadow_profile_path = publish_profile(route.profile, inputs, profiles)

    # -- ① the one key ------------------------------------------------------------------
    document = write_inputs_document(inputs, tmp_path / "host" / "data" / "inputs.json")
    result.inputs_path = document
    code, summary, stderr = cutover(
        "set-mode", "--inputs", str(document), "--from", "shadow", "--to", "live"
    )
    assert code == 0, stderr
    assert summary is not None and summary["changed_keys"] == ["notifier_delivery_mode"]
    assert summary["written"] is True
    result.set_mode = summary
    live_inputs = load_production_runtime_profile_inputs(
        document, expected_commit=commit, expected_runtime_mode=inputs.runtime_mode
    )
    assert live_inputs.notifier_delivery_mode == "live"
    assert live_inputs.model_copy(update={"notifier_delivery_mode": "shadow"}) == inputs

    # -- ② prerequisites + profile, from the rewritten document -------------------------
    install_production_runtime_prerequisites(live_inputs)
    result.live_profile = build_production_runtime_profile(live_inputs)
    result.live_profile_path = publish_profile(result.live_profile, live_inputs, profiles)
    code, result.profile_diff, stderr = cutover(
        "diff-profiles", str(result.shadow_profile_path), str(result.live_profile_path)
    )
    assert code == 0, (result.profile_diff, stderr)

    # -- ③ the third bundle generation, the same capability environment ----------------
    result.receipt = install_runtime_deployment_profile(
        result.live_profile,
        runtime_root=route.runtime_root,
        environ=bundle["capabilities"],
        schema_bootstrap_reason=None,
        schema_rollout_started_at=SWITCH_AT,
    )
    assert result.receipt.previous_generation_hash == route.receipt.generation_hash
    generations = route.runtime_root / "generations"
    code, result.generation_diff, stderr = cutover(
        "diff-generations",
        str(generations / route.receipt.generation_hash),
        str(generations / result.receipt.generation_hash),
    )
    assert code == 0, (result.generation_diff, stderr)
    result.acknowledged = acknowledge_runtime_schema_rollout_preparation(
        route.runtime_root, now=SWITCH_AT + timedelta(seconds=41)
    )

    # -- ④ stage from `current`, publish as root ----------------------------------------
    result.live_plan = route.world.stage(
        "cutover-live",
        bootstrap_from_checkout=False,
        legacy_runtime_root=route.runtime_root,
    )
    result.published = route.world.publish(result.live_plan)
    route.plan = result.live_plan
    route.profile = result.live_profile
    route.receipt = result.receipt
    return result


# ---------------------------------------------------------------------------------------
# The world: a shadow day that has already delivered one signal (in shadow)
# ---------------------------------------------------------------------------------------


class ShadowDay:
    def __init__(
        self,
        route: RouteAWorld,
        bundle: dict[str, Any],
        credentials: dict[str, Path],
        runs: dict[str, Any],
        outbox: dict[str, dict[str, Any]],
    ) -> None:
        self.route = route
        self.bundle = bundle
        self.credentials = credentials
        self.runs = runs
        self.outbox = outbox


@pytest.fixture
def shadow_day(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    minute_history_parquet: bytes,  # noqa: F811
    push_recorder: _PushRecorder,
) -> ShadowDay:
    bundle: dict[str, Any] = {}
    route = build_trading_day_chain(
        tmp_path,
        monkeypatch,
        minute_history_parquet,
        notifier_delivery_mode="shadow",
        bundle_out=bundle,
        capability_overrides={"PUSHDEER_KEYS": ",".join(PUSHDEER_DEVICE_KEYS)},
    )
    assert manifest_of(route, NOTIFIER_SERVICE_ID).settings["suppress_delivery"] is True
    credentials = deliver_credentials(route, tmp_path / "credentials-shadow", monkeypatch)
    publish_two_candidates(route)
    publish_minute_batch_at(
        route,
        bars_from=clock_time(9, 45),
        received_at=_at(9, 47, 11),
        closes={SIGNAL_CODE: 11.0},
    )
    runs = drive(route, credentials, HOP_CLOCKS)
    outbox = notifier_outbox(route)
    #: the shadow day delivered every signal it routed -- `auction_gap` watches both
    #: candidates, `n_shape` traded the one code whose close cleared its reference -- and
    #: delivered each of them only in shadow
    assert outbox, "the shadow notifier claimed nothing"
    assert {
        row["candidate_id"] for row in outbox.values() if row["strategy_id"] == "n_shape"
    } == {SIGNAL_CODE}
    assert all(row["status"] == "succeeded" for row in outbox.values()), outbox
    assert all(
        receipt is not None and receipt.startswith("shadow:")
        for row in outbox.values()
        for receipt in row["receipts"]
    ), outbox
    #: both devices got a row for every signal, and the `admin` rows the router routed
    #: were migrated onto them before anything was claimed
    assert {row["recipient_id"] for row in outbox.values()} == set(DEVICE_KEY)
    (notifier,) = runs["notifier"]
    assert "notifier:shadow_transport" in (notifier.heartbeat.degraded_reasons or ())
    assert INFERRED in (notifier.heartbeat.degraded_reasons or ())
    assert push_recorder.posts == []
    return ShadowDay(route, bundle, credentials, runs, outbox)


# ---------------------------------------------------------------------------------------
# The cases
# ---------------------------------------------------------------------------------------


def test_the_switch_changes_one_setting_and_publishes_under_the_same_authority_profile(
    shadow_day: ShadowDay,
    tmp_path: Path,
    push_recorder: _PushRecorder,
) -> None:
    """Steps 2-4: one key, one manifest, one publish, and #190 is not in the way."""

    route = shadow_day.route
    shadow_sequence = route.world.record()["sequence"]
    result = switch_to_live(route, shadow_day.bundle, tmp_path)

    #: ① the document changed in exactly that key, and the loader accepts the rewrite
    assert result.set_mode is not None
    assert result.set_mode["from"] == "shadow" and result.set_mode["to"] == "live"

    #: ② the runtime-production profile id changes (it always does) -- one manifest, one key
    assert result.live_profile.profile_id != result.shadow_profile.profile_id
    assert result.profile_diff is not None
    assert result.profile_diff["unexpected_differences"] == []
    assert result.profile_diff["notifier_differences"] == ["settings.suppress_delivery"]
    assert result.profile_diff["notifier_after"] == {"paused": False, "suppress_delivery": False}
    assert result.profile_diff["manifests_compared"] == 26

    #: the notifier's service spec -- what a heartbeat is checked against -- is unchanged
    shadow_spec = manifest_of_profile(result.shadow_profile, NOTIFIER_SERVICE_ID).service_spec
    live_spec = manifest_of_profile(result.live_profile, NOTIFIER_SERVICE_ID).service_spec
    assert live_spec == shadow_spec
    assert live_spec.identity == shadow_spec.identity

    #: ③ the bundle generation: 25 manifests byte for byte, the notifier's differing in
    #: the one setting, the schema contracts differing only in the notifier's manifest
    #: fingerprint (and their own hash) -- no channel moved, so no rollout plan at all
    assert result.generation_diff is not None
    assert result.generation_diff["unexpected_differences"] == []
    assert result.generation_diff["manifests_byte_identical"] == 25
    assert sorted(result.generation_diff["schema_contract_differences"]) == [
        "content_hash",
        f"manifest_fingerprints.{NOTIFIER_SERVICE_ID}",
    ]
    assert result.receipt.schema_rollout_plan_ids == ()
    assert all(not item.changed for item in result.acknowledged), result.acknowledged

    #: ④ #190: the plan's closure profile is the installed one, so the publish is the
    #: "same profile, next generation" transition every window since 09-07 has made
    assert result.live_plan.plan["profile_id"] == result.shadow_plan.plan["profile_id"]
    installed = json.loads(route.world.profile_path.read_bytes())
    assert installed["profile_id"] == result.live_plan.plan["profile_id"]
    assert result.live_plan.plan["generation_id"] != result.shadow_plan.plan["generation_id"]
    assert result.published is not None
    assert result.published["result"] == "committed"
    record = route.world.record()
    assert record["sequence"] == shadow_sequence + 1
    assert record["state"] == "active"
    assert record["current_generation_id"] == result.live_plan.plan["generation_id"]
    assert record["prior_generation_id"] == result.shadow_plan.plan["generation_id"]
    assert record["prior_lifecycle"] == "rollback_ready"
    assert record["current_profile_id"] == record["prior_profile_id"]

    #: and nothing was sent while all of that happened
    assert push_recorder.posts == []


def manifest_of_profile(profile: Any, service_id: str) -> Any:
    return next(item for item in profile.manifests if item.service_id == service_id)


def test_the_live_notifier_sends_only_what_was_routed_after_the_switch(
    shadow_day: ShadowDay,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    push_recorder: _PushRecorder,
) -> None:
    """Step 5: one unit restarts live; the shadow-delivered row stays delivered."""

    route = shadow_day.route
    before = shadow_day.outbox
    switch_to_live(route, shadow_day.bundle, tmp_path)
    #: systemd decrypts the new `current.cred` when the unit starts; the notifier's copy is
    #: the only one this case needs, and the other roles keep the credentials they have
    credentials = deliver_credentials(route, tmp_path / "credentials-live", monkeypatch)

    #: #270 premise: an unclean stop leaves a heartbeat that never reached `stopped`. The
    #: live notifier must start over it, because its spec did not change.
    heartbeat_path = next(
        (route.runtime_root / "control" / "notifiers").glob("svc-*/heartbeats/*.json")
    )
    left_behind = json.loads(heartbeat_path.read_text(encoding="utf-8"))
    left_behind.update({"status": "running", "stopped_at": None, "stop_reason": None})
    heartbeat_path.write_text(json.dumps(left_behind), encoding="utf-8")
    heartbeat_path.chmod(0o600)

    first = run_notifier(route, credentials, LIVE_FIRST_ROUND)
    #: one pass through the loop, then the harness's stop: `stopped`, clean, no failure,
    #: and no `shadow_transport` -- only the two-device reason it carried in shadow too
    assert first.heartbeat.degraded_reasons == (INFERRED,), first.heartbeat
    assert first.heartbeat.total_failures == 0
    assert first.heartbeat.last_error is None
    assert first.heartbeat.stop_reason == "loop completed", first.heartbeat
    #: nothing new was routed, and nothing old went out
    assert push_recorder.posts == []
    assert notifier_outbox(route) == before

    # -- a signal after the switch travels to the provider, once ------------------------
    publish_minute_batch_at(
        route,
        bars_from=POST_SWITCH_BARS_FROM,
        received_at=POST_SWITCH_RECEIVED_AT,
        closes={SIGNAL_CODE: 11.0, SECOND_CODE: 11.0},
    )
    runs = drive(route, credentials, POST_SWITCH_CLOCKS)
    assert runner_signal_count(route, N_SHAPE_SERVICE_ID) >= 2
    after = notifier_outbox(route)

    #: every row the shadow notifier delivered is exactly as it was
    for outbox_id, row in before.items():
        assert after[outbox_id] == row, outbox_id
    new = {outbox_id: row for outbox_id, row in after.items() if outbox_id not in before}
    assert new, "nothing was routed after the switch"
    #: three signals the chain produced after the switch -- `auction_gap` watching the
    #: second code, `n_shape` trading it, and `auction_gap` moving the first code from its
    #: morning watch to an intent -- every one of them a signal id the shadow day never
    #: saw, one row per device
    assert sorted(
        (row["candidate_id"], row["strategy_id"], row["action"], row["recipient_id"])
        for row in new.values()
    ) == sorted(
        (*signal, device)
        for signal in (
            (SECOND_CODE, "auction_gap", "watch"),
            (SECOND_CODE, "n_shape", "b_intent"),
            (SIGNAL_CODE, "auction_gap", "b_intent"),
        )
        for device in DEVICE_KEY
    )
    shadow_signals = {row["signal_id"] for row in before.values()}
    assert not shadow_signals & {row["signal_id"] for row in new.values()}
    assert all(row["status"] == "succeeded" for row in new.values()), new
    for row in new.values():
        (receipt,) = row["receipts"]
        assert receipt is not None and receipt.startswith("pushdeer:"), receipt

    #: the real client posted exactly once per new row, to PushDeer, with that device's
    #: sealed key, and never once about a signal the shadow day had already delivered
    assert len(push_recorder.posts) == len(new)
    assert sorted(
        (post["body"]["text"], post["body"]["pushkey"]) for post in push_recorder.posts
    ) == sorted(
        (f"[rQuant] {row['candidate_id']} {TITLES[row['action']]}", DEVICE_KEY[row["recipient_id"]])
        for row in new.values()
    )
    for post in push_recorder.posts:
        assert post["url"] == "https://api2.pushdeer.com/message/push"
        assert post["body"]["type"] == "markdown"
        assert not any(signal_id in post["body"]["desp"] for signal_id in shadow_signals)

    (notifier,) = runs["notifier"]
    assert_live_reasons(notifier.heartbeat.degraded_reasons)
    assert "notifier:recipient_migration:3->6" in notifier.heartbeat.degraded_reasons
    #: and serving, whose manifest the switch did not touch, took the live notifier's
    #: `signals` authority in the same pass
    (serving,) = runs["serving"]
    assert serving.refusal is None
    assert serving.heartbeat.last_error is None, serving.heartbeat



#: The roles that stay up through the switch in this case: every role on the signal path
#: from the minute batch to serving, plus health. The two source roles in front of them and
#: the candidate publishers are not run -- their inputs are what the harness writes.
RESIDENT_ROLES = frozenset(
    {
        "paper_constraint_publisher",
        "feature_live",
        "strategy_live",
        "signal_router",
        "paper_broker",
        "notifier",
        "runtime_health_publisher",
        "serving_publisher",
    }
)
#: the order one tick hands out iterations in, as the chain consumes its own outputs
RESIDENT_ORDER = (
    "runtime_health_publisher",
    "paper_constraint_publisher",
    "feature_live",
    "strategy_live",
    "signal_router",
    "paper_broker",
    "notifier",
    "serving_publisher",
)
BEFORE_THE_SWITCH = _at(9, 50, 1)


def test_resident_roles_run_through_the_switch_and_only_the_notifier_restarts(
    shadow_day: ShadowDay,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    push_recorder: _PushRecorder,
) -> None:
    """Step 5 as the host lives it: eight roles are *running* while the switch happens.

    The other cases start every role afresh, which proves a restart after the switch is
    clean. What Monday needs as well is that the roles nobody restarts -- they keep the
    authority generation, the bundle generation and the manifests they started with --
    are not disturbed by `runtime-deployment-profile --apply` moving `data/runtime/current`
    and by the publish moving `current.json`. So here each role is a thread of this process
    that stays inside its own service loop between iterations (`route_a_day_replay`'s
    runner, one iteration per tick), the switch runs while they are all parked, only the
    notifier's loop is stopped and started again from the new generation, and the next
    tick carries a post-switch signal through the roles that were never restarted.
    """

    import scripts.route_a_day_replay as replay

    route = shadow_day.route
    before = shadow_day.outbox
    clock = replay.ReplayClock(BEFORE_THE_SWITCH)
    replay.install_runner_patches(monkeypatch, clock, None)

    def resident_states(credentials: dict[str, Path]) -> dict[str, list[Any]]:
        states: dict[str, list[Any]] = {}
        for state in replay.role_states(route, credentials):
            if state.role in RESIDENT_ROLES:
                states.setdefault(state.role, []).append(state)
        return states

    def tick(runner: Any, states: dict[str, list[Any]], clocks: dict[str, datetime]) -> None:
        for role in RESIDENT_ORDER:
            for state in states[role]:
                assert runner.step(state, clocks[role]), runner.hung
                assert not state.crashes, state.crashes

    def run_ids(states: dict[str, list[Any]]) -> dict[str, str]:
        return {
            state.label: state.last_heartbeat.run_id
            for role_states in states.values()
            for state in role_states
        }

    states = resident_states(shadow_day.credentials)
    runner = replay.RoleRunner(
        [state for role in RESIDENT_ORDER for state in states[role]],
        clock=clock,
        role_timeout_seconds=180,
        max_restarts=0,
        log=lambda _line: None,
    )
    try:
        # -- before the switch: every role enters its loop on the shadow generation ------
        tick(runner, states, dict.fromkeys(RESIDENT_ORDER, BEFORE_THE_SWITCH))
        started = run_ids(states)
        (shadow_notifier,) = states["notifier"]
        assert "notifier:shadow_transport" in shadow_notifier.last_heartbeat.degraded_reasons
        assert INFERRED in shadow_notifier.last_heartbeat.degraded_reasons
        assert push_recorder.posts == []

        # -- the switch, with all eight parked inside their loops ------------------------
        switch_to_live(route, shadow_day.bundle, tmp_path)
        credentials = deliver_credentials(route, tmp_path / "credentials-live", monkeypatch)

        # -- the one restart: `systemctl stop` then `start` of the notifier's unit --------
        assert shadow_notifier.baton is not None and shadow_notifier.thread is not None
        shadow_notifier.baton.set()
        shadow_notifier.thread.join(timeout=60)
        assert not shadow_notifier.thread.is_alive()
        (live_notifier,) = resident_states(credentials)["notifier"]
        states["notifier"] = [live_notifier]
        runner.states = [state for role in RESIDENT_ORDER for state in states[role]]

        # -- the next minute, through roles that were never restarted --------------------
        publish_minute_batch_at(
            route,
            bars_from=POST_SWITCH_BARS_FROM,
            received_at=POST_SWITCH_RECEIVED_AT,
            closes={SIGNAL_CODE: 11.0, SECOND_CODE: 11.0},
        )
        tick(runner, states, POST_SWITCH_CLOCKS)
    finally:
        runner.stop_all()

    after = run_ids(states)
    for label, run_id in started.items():
        if label == NOTIFIER_SERVICE_ID:
            #: the notifier is the one process that was replaced
            assert after[label] != run_id
            continue
        #: every other role is the same process it was before the switch, and its
        #: iteration after the switch did its work without a failure
        assert after[label] == run_id, label
    for role in RESIDENT_ORDER:
        for state in states[role]:
            heartbeat = state.last_heartbeat
            assert heartbeat.consecutive_failures == 0, (state.label, heartbeat.last_error)
            assert heartbeat.last_error is None, (state.label, heartbeat.last_error)
            if role != "notifier":
                assert state.iterations == 2, (state.label, state.iterations)

    assert_live_reasons(live_notifier.last_heartbeat.degraded_reasons)
    outbox = notifier_outbox(route)
    for outbox_id, row in before.items():
        assert outbox[outbox_id] == row, outbox_id
    new = {outbox_id: row for outbox_id, row in outbox.items() if outbox_id not in before}
    assert sorted(
        {(row["candidate_id"], row["strategy_id"], row["action"]) for row in new.values()}
    ) == [
        (SECOND_CODE, "auction_gap", "watch"),
        (SECOND_CODE, "n_shape", "b_intent"),
        (SIGNAL_CODE, "auction_gap", "b_intent"),
    ]
    assert {row["recipient_id"] for row in new.values()} == set(DEVICE_KEY)
    assert all(row["status"] == "succeeded" for row in new.values()), new
    assert len(push_recorder.posts) == len(new) == 6
    (serving,) = states["serving_publisher"]
    assert serving.last_heartbeat.generation_published is True

def test_the_rollback_returns_the_notifier_to_shadow_and_a_later_release_still_publishes(
    shadow_day: ShadowDay,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    push_recorder: _PushRecorder,
) -> None:
    """The rollback in the DEPLOY entry: authority rollback + re-applying Friday's profile.

    Staging shadow again cannot work: every input would be Friday's, so the generation
    would be Friday's generation, which is the chain's `prior` -- "next generation is
    already recorded". The single-level rollback is what returns to it.
    """

    route = shadow_day.route
    result = switch_to_live(route, shadow_day.bundle, tmp_path)
    live_record = route.world.record()

    # -- R2: the root rollback, then R3: `current` back to Friday's bundle generation ----
    rolled = rollback_authority(operation_id=hashlib.sha256(b"rollback").hexdigest()[:32])
    assert rolled["result"] == "committed"
    assert rolled["generation_id"] == result.shadow_plan.plan["generation_id"]
    assert rolled["state"] == "rolled_back"
    #: back to the document Friday wrote, byte for byte, so the profile is Friday's profile
    assert result.inputs_path is not None and result.set_mode is not None
    code, summary, stderr = cutover(
        "set-mode", "--inputs", str(result.inputs_path), "--from", "live", "--to", "shadow"
    )
    assert code == 0, stderr
    assert summary is not None and summary["sha256_after"] == result.set_mode["sha256_before"]
    shadow_inputs = load_production_runtime_profile_inputs(
        result.inputs_path,
        expected_commit=route.world.commit,
        expected_runtime_mode=shadow_day.bundle["inputs"].runtime_mode,
    )
    assert build_production_runtime_profile(shadow_inputs).profile_id == (
        result.shadow_profile.profile_id
    )
    shadow_generation = result.shadow_profile and install_runtime_deployment_profile(
        result.shadow_profile,
        runtime_root=route.runtime_root,
        environ=shadow_day.bundle["capabilities"],
        schema_bootstrap_reason=None,
        schema_rollout_started_at=ROLLBACK_AT,
    )
    assert shadow_generation.generation_hash == result.receipt.previous_generation_hash
    assert os.readlink(route.runtime_root / "current") == (
        f"generations/{shadow_generation.generation_hash}"
    )
    route.plan = result.shadow_plan
    route.profile = result.shadow_profile
    route.receipt = shadow_generation

    # -- R4: the notifier starts in shadow again ----------------------------------------
    credentials = deliver_credentials(route, tmp_path / "credentials-rollback", monkeypatch)
    again = run_notifier(route, credentials, SHADOW_AGAIN_ROUND)
    assert "notifier:shadow_transport" in (again.heartbeat.degraded_reasons or ())
    assert push_recorder.posts == []

    #: the next release (Tuesday's v0.33.23 window, say) still publishes over the rollback
    route.world.bump_checkout("next release after the rollback")
    later = route.world.stage(
        "after-rollback", bootstrap_from_checkout=False, legacy_runtime_root=route.runtime_root
    )
    assert later.plan["profile_id"] == result.live_plan.plan["profile_id"]
    published = route.world.publish(later)
    assert published["result"] == "committed"
    record = route.world.record()
    assert record["sequence"] == live_record["sequence"] + 2
    assert record["state"] == "active"


def test_the_guard_catches_a_real_pushdeer_post(
    network_guard: outbound_network_guard.OutboundNetworkGuard,
) -> None:
    """The negative control: let the real client through, and the guard is what stops it.

    A guard that never fires proves nothing. Here the legacy client that the live notifier
    uses is handed the real endpoint and no recorder: it reaches DNS, the audit hook
    refuses and records it, and the client -- which swallows every exception -- reports
    the push as not delivered. The fixture's own end-of-case check would fail on this
    record, so it is read and cleared here.
    """

    results = push_client_module.PushDeerClient(
        ["rehearsal-key-never-valid"], "https://api2.pushdeer.com/message/push"
    ).push("rehearsal", "must not leave the host")
    assert results == [(False, results[0][1])]
    assert results[0][1] is not None and "outbound network refused" in results[0][1]
    assert network_guard.attempts, "the guard saw nothing"
    assert network_guard.attempts[0][0] == "socket.getaddrinfo"
    assert "api2.pushdeer.com" in network_guard.attempts[0][1]
    network_guard.attempts.clear()

