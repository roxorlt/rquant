#!/usr/bin/env python3
"""Publish the sealed Phase C `strategy-shadow` producer fixture into a generation tree.

The three `strategy-shadow` reader surfaces are only reachable through
`shadow_session_builder`'s default `FilesystemShadowSessionInputLoader`, which loads four
accepted legacy shadow exports under `LegacyShadowFilesystemPolicy(mode="linux-production")`.
That policy is the whole reason this module exists and the whole reason it is Linux-only:
`legacy_shadow_export._validate_mount_policy` refuses the mode outright off Linux.

Two properties shape the design, and both come from production code rather than preference:

* **The export cannot be copied.** `_verify_recovery_marker_batch_at` binds the Ed25519
  recovery marker to the session directory's `st_dev`/`st_ino`, and the finalization receipt
  binds the same pair again. A byte-perfect copy has a different inode, so the export has to
  be *published where it will be read* — inside the generation tree — and read in place. The
  ruling E-1 `generation_files` channel still covers every file of it byte for byte, and the
  root still digests the whole set before and after the child; what changes is that the child
  resolves `@generation/…` paths instead of copying.
* **The signing key is minted here and dies here.** The Ed25519 keypairs below are generated
  in this process, used only to sign this fixture, and never written into the generation,
  never handed to the child, and never checked in. Only the *public* keys travel: into the
  vector's service manifest settings, which the policy hashes. The child can read the export
  and verify it; it cannot mint one.

  **These keys sign fixture evidence only. They are not, and must never become, a signing
  authority for any production object.** Nothing here writes a private key to disk outside a
  scratch directory that is deleted before this function returns.

The producer chain is the one `authority.md` L1214 declares for the pair, in order:
`StrategyRunnerStore.process_batch` → `route_runner_signals` →
`ReadonlySignalRouteAuthority.read_drain_evidence` →
`StrategyRunnerStore.publish_session_close_receipt` →
`ReadonlyStrategyRunnerSignalSource` → `publish_isolated_runner_export`.
"""

from __future__ import annotations

import base64
import hashlib
import json
import os
import shutil
import subprocess
import sys
import tempfile
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any, Final

REPOSITORY_ROOT: Final[Path] = Path(__file__).resolve().parent.parent

#: One frozen trade date. Everything the reader checks against a publish window is derived
#: from `shadow_session_boundaries(TRADE_DATE)`, never from "now", so a fixture built once
#: stays acceptable indefinitely.
TRADE_DATE: Final[date] = date(2026, 8, 24)
PRODUCER_COMMIT: Final[str] = "f" * 40
PRODUCER_VERSION: Final[str] = "signal-family-shadow-fixture-v1"
EVALUATOR_FINGERPRINT: Final[str] = "2" * 64
REGISTRATION_FINGERPRINT: Final[str] = "1" * 64
CANDIDATE_SCHEMA_FINGERPRINT: Final[str] = "3" * 64
FEATURE_REGISTRATION_FINGERPRINT: Final[str] = "4" * 64
FEATURE_CONTRACT_FINGERPRINT: Final[str] = "5" * 64
PRODUCER_MANIFEST_FINGERPRINT: Final[str] = "6" * 64
ROUTING_POLICY_FINGERPRINT: Final[str] = "9" * 64
BOOT_ID: Final[str] = "00000000-0000-4000-8000-000000000001"
RECOVERY_KEY_ID: Final[str] = "signal-family-fixture-recovery-v1"
COMPLETION_KEY_ID: Final[str] = "signal-family-fixture-completion-v1"

#: `(strategy_id, action, candidate_id)`. The pair's two legacy-comparable strategies are
#: fixed by `ShadowSessionSettings.require_legacy_comparison_bindings`.
STRATEGIES: Final[tuple[tuple[str, str, str], ...]] = (
    ("n_shape", "watch", "600001.SH"),
    ("growth_board_surge", "b_intent", "300001.SZ"),
)


class ShadowFixtureUnavailableError(RuntimeError):
    """The fixture cannot be published in this environment."""


@dataclass(frozen=True)
class ShadowFixtureResult:
    """What the offline world and the vector set need to know about the published fixture."""

    relative_paths: tuple[str, ...]
    descriptor: dict[str, Any]


# ---------------------------------------------------------------------------------------
# One-time signing material
# ---------------------------------------------------------------------------------------


def _openssl() -> str:
    executable = shutil.which("openssl")
    if executable is None:  # pragma: no cover - the runner image ships openssl
        raise ShadowFixtureUnavailableError("openssl is required to mint the fixture keypairs")
    return executable


class _OpenSslSigningClient:
    """Signs with a private key that exists only inside this process's scratch directory."""

    def __init__(self, private_key_path: Path) -> None:
        self._private_key_path = private_key_path

    def sign(self, *, namespace: str, payload: bytes) -> str:
        payload_path = self._private_key_path.parent / f"{namespace}.payload"
        signature_path = self._private_key_path.parent / f"{namespace}.signature"
        payload_path.write_bytes(payload)
        completed = subprocess.run(  # noqa: S603 - fixed argv, scratch paths
            (
                _openssl(),
                "pkeyutl",
                "-sign",
                "-inkey",
                str(self._private_key_path),
                "-rawin",
                "-in",
                str(payload_path),
                "-out",
                str(signature_path),
            ),
            check=False,
            capture_output=True,
        )
        if completed.returncode != 0:
            raise ShadowFixtureUnavailableError(
                completed.stderr.decode("utf-8", errors="replace")
            )
        return base64.b64encode(signature_path.read_bytes()).decode("ascii")


def _mint_keypair(scratch: Path, *, key_id: str) -> tuple[_OpenSslSigningClient, bytes]:
    scratch.mkdir(mode=0o700, parents=True, exist_ok=True)
    private_key = scratch / f"{key_id}.private.pem"
    public_key = scratch / f"{key_id}.public.pem"
    for argv in (
        (_openssl(), "genpkey", "-algorithm", "ED25519", "-out", str(private_key)),
        (_openssl(), "pkey", "-in", str(private_key), "-pubout", "-out", str(public_key)),
    ):
        completed = subprocess.run(argv, check=False, capture_output=True)  # noqa: S603
        if completed.returncode != 0:
            raise ShadowFixtureUnavailableError(
                completed.stderr.decode("utf-8", errors="replace")
            )
    private_key.chmod(0o600)
    return _OpenSslSigningClient(private_key), public_key.read_bytes()


class _FixtureEd25519RecoveryAuthority:
    """An Ed25519 legacy-shadow recovery authority for fixture publishing only.

    It mirrors `HmacLegacyShadowRecoveryAuthority` step for step — the same production
    helpers compute the staging directory identity, the artifact digests, the batch digest,
    the transaction identity, and every clock invariant — and differs in exactly one place:
    the signature is a real Ed25519 signature over the production domain-separated payload
    instead of an HMAC, because the production reader's verifier is
    `Ed25519LegacyShadowRecoveryKeyring` and it refuses any other algorithm.

    Verification is not reimplemented at all: it delegates to that same production keyring.
    """

    def __init__(self, *, key_id: str, client: _OpenSslSigningClient, public_key: bytes) -> None:
        from rquant.legacy_shadow_export import Ed25519LegacyShadowRecoveryKeyring

        self.key_id = key_id
        self._client = client
        self._keyring = Ed25519LegacyShadowRecoveryKeyring(
            active_key_id=key_id,
            active_public_key=public_key,
        )
        self._wall_clock: Any = None
        self._monotonic_ns: Any = None
        self._boot_id: Any = None
        self._consumed_capture_tokens: set[str] = set()

    def bind_clock(self, *, wall_clock: Any, monotonic_ns: Any, boot_id: Any) -> None:
        self._wall_clock = wall_clock
        self._monotonic_ns = monotonic_ns
        self._boot_id = boot_id

    # -- signing primitives ----------------------------------------------------------

    def _sign(self, *, namespace: str, payload: bytes) -> str:
        from rquant.runtime_shadow_validation import _ed25519_signing_payload

        return self._client.sign(
            namespace=namespace,
            payload=_ed25519_signing_payload(namespace=namespace, payload=payload),
        )

    # -- the `LegacyShadowRecoverySigner` protocol -------------------------------------

    def capture(self, binding: Any) -> Any:
        from rquant.legacy_shadow_export import (
            LegacyShadowRecoveryCapture,
            LegacyShadowRecoveryCaptureBinding,
            LegacyShadowRecoveryCaptureClaims,
            _in_publish_window,
            _recovery_capture_payload,
        )
        from rquant.runtime_contracts import normalize_aware_utc

        verified = LegacyShadowRecoveryCaptureBinding.model_validate(binding)
        captured_at = normalize_aware_utc(self._wall_clock())
        if not _in_publish_window(trade_date=verified.trade_date, captured_at=captured_at):
            raise ValueError("fixture recovery capture is outside the publish window")
        claims = LegacyShadowRecoveryCaptureClaims(
            **verified.model_dump(mode="python", exclude={"contract"}),
            captured_at=captured_at,
            captured_monotonic_ns=self._monotonic_ns(),
            boot_id=self._boot_id(),
            clock_source="CLOCK_BOOTTIME",
        )
        signed_payload = _recovery_capture_payload(claims)
        return LegacyShadowRecoveryCapture(
            key_id=self.key_id,
            signature_algorithm="ed25519",
            claims=claims,
            signed_claims_base64=base64.b64encode(signed_payload).decode("ascii"),
            signature=self._sign(
                namespace="rquant-legacy-shadow-recovery-capture",
                payload=signed_payload,
            ),
        )

    def issue(self, draft: Any, capture: Any, *, staging_root: Path) -> Any:
        from rquant.legacy_shadow_export import (
            _FINALIZATION_RECEIPT_FILENAME,
            _RECOVERY_MARKER_FILENAME,
            _RECOVERY_WALL_MONOTONIC_TOLERANCE,
            _ROOT_MODE,
            _SESSION_MODE,
            LegacyShadowFinalizationClaims,
            LegacyShadowFinalizationReceipt,
            LegacyShadowRecoveryCapture,
            LegacyShadowRecoveryMarker,
            LegacyShadowRecoveryMarkerClaims,
            LegacyShadowRecoveryMarkerDraft,
            _finalization_receipt_payload,
            _in_publish_window,
            _inspect_complete_marker_artifacts_at,
            _open_child_directory_at,
            _open_directory_fd,
            _recovery_marker_payload,
            _write_new_file_at,
        )
        from rquant.runtime_contracts import canonical_sha256, normalize_aware_utc
        from rquant.strict_json import canonical_model_json_bytes

        verified_draft = LegacyShadowRecoveryMarkerDraft.model_validate(draft)
        verified_capture = LegacyShadowRecoveryCapture.model_validate(capture)
        if verified_capture.key_id != self.key_id or (
            verified_capture.signature_algorithm != "ed25519"
        ):
            raise ValueError("fixture recovery capture is not this authority's")
        capture_token_id = canonical_sha256(verified_capture.model_dump(mode="python"))
        if capture_token_id in self._consumed_capture_tokens:
            raise ValueError("fixture recovery capture token was already consumed")
        for field_name in (
            "trade_date",
            "source_id",
            "producer_commit",
            "producer_version",
            "staging_name",
        ):
            if getattr(verified_draft, field_name) != getattr(
                verified_capture.claims,
                field_name,
            ):
                raise ValueError("fixture recovery capture binding mismatch")
        produced_at = normalize_aware_utc(self._wall_clock())
        produced_monotonic_ns = self._monotonic_ns()
        if not _in_publish_window(
            trade_date=verified_draft.trade_date,
            captured_at=produced_at,
        ):
            raise ValueError("fixture recovery completion is outside the publish window")
        if (
            self._boot_id() != verified_capture.claims.boot_id
            or produced_at < verified_capture.claims.captured_at
            or produced_monotonic_ns < verified_capture.claims.captured_monotonic_ns
        ):
            raise ValueError("fixture recovery clock rollback detected")
        wall_elapsed = produced_at - verified_capture.claims.captured_at
        monotonic_elapsed = timedelta(
            microseconds=(produced_monotonic_ns - verified_capture.claims.captured_monotonic_ns)
            // 1_000
        )
        if abs(wall_elapsed - monotonic_elapsed) > _RECOVERY_WALL_MONOTONIC_TOLERANCE:
            raise ValueError("fixture recovery clock rollback detected")

        staging_root_descriptor = _open_directory_fd(staging_root, label="fixture staging root")
        staging_descriptor = -1
        try:
            staging_descriptor = _open_child_directory_at(
                staging_root_descriptor,
                verified_draft.staging_name,
                label="fixture staging",
                allowed_modes=frozenset({_ROOT_MODE}),
            )
            directory_stat, artifact_digests, observed_batch_digest = (
                _inspect_complete_marker_artifacts_at(
                    staging_descriptor,
                    source_id=verified_draft.source_id,
                )
            )
            if observed_batch_digest != verified_draft.batch_digest:
                raise ValueError("fixture recovery batch digest mismatch")
        finally:
            if staging_descriptor >= 0:
                os.close(staging_descriptor)
            os.close(staging_root_descriptor)

        marker_claims = LegacyShadowRecoveryMarkerClaims(
            **verified_draft.model_dump(mode="python", exclude={"contract"}),
            directory_device=directory_stat.st_dev,
            directory_inode=directory_stat.st_ino,
            artifact_digests=artifact_digests,
            captured_at=verified_capture.claims.captured_at,
            produced_at=produced_at,
            boot_id=verified_capture.claims.boot_id,
            captured_monotonic_ns=verified_capture.claims.captured_monotonic_ns,
            produced_monotonic_ns=produced_monotonic_ns,
            clock_source="CLOCK_BOOTTIME",
        )
        values = {
            "contract": "legacy-shadow-recovery-marker/v1",
            "key_id": self.key_id,
            "signature_algorithm": "ed25519",
            "claims": marker_claims,
            "signature": self._sign(
                namespace="rquant-legacy-shadow-recovery-marker",
                payload=_recovery_marker_payload(marker_claims),
            ),
        }
        marker = LegacyShadowRecoveryMarker(marker_id=canonical_sha256(values), **values)
        marker_payload = canonical_model_json_bytes(marker)

        staging_root_descriptor = _open_directory_fd(staging_root, label="fixture staging root")
        staging_descriptor = -1
        try:
            staging_descriptor = _open_child_directory_at(
                staging_root_descriptor,
                verified_draft.staging_name,
                label="fixture staging",
                allowed_modes=frozenset({_ROOT_MODE}),
            )
            rebound = os.fstat(staging_descriptor)
            if (rebound.st_dev, rebound.st_ino) != (
                directory_stat.st_dev,
                directory_stat.st_ino,
            ):
                raise ValueError("fixture staging changed before signing")
            _write_new_file_at(staging_descriptor, _RECOVERY_MARKER_FILENAME, marker_payload)
            os.fsync(staging_descriptor)
            os.fsync(staging_root_descriptor)
            finalized_at = normalize_aware_utc(self._wall_clock())
            finalized_monotonic_ns = self._monotonic_ns()
            if not _in_publish_window(
                trade_date=verified_draft.trade_date,
                captured_at=finalized_at,
            ):
                raise ValueError("fixture recovery finalization is outside the publish window")
            finalization_claims = LegacyShadowFinalizationClaims(
                trade_date=marker_claims.trade_date,
                source_id=marker_claims.source_id,
                producer_commit=marker_claims.producer_commit,
                producer_version=marker_claims.producer_version,
                staging_name=marker_claims.staging_name,
                transaction_id=canonical_sha256(
                    {
                        "contract": "legacy-shadow-recovery-transaction-identity/v1",
                        "capture_token_id": capture_token_id,
                    }
                ),
                capture_token_id=capture_token_id,
                marker_id=marker.marker_id,
                marker_sha256=hashlib.sha256(marker_payload).hexdigest(),
                batch_digest=marker_claims.batch_digest,
                directory_device=marker_claims.directory_device,
                directory_inode=marker_claims.directory_inode,
                artifact_digests=marker_claims.artifact_digests,
                finalized_at=finalized_at,
                finalized_monotonic_ns=finalized_monotonic_ns,
                boot_id=self._boot_id(),
                clock_source="CLOCK_BOOTTIME",
            )
            receipt_values = {
                "contract": "legacy-shadow-finalization-receipt/v1",
                "key_id": self.key_id,
                "signature_algorithm": "ed25519",
                "claims": finalization_claims,
                "signature": self._sign(
                    namespace="rquant-legacy-shadow-finalization-receipt",
                    payload=_finalization_receipt_payload(finalization_claims),
                ),
            }
            receipt = LegacyShadowFinalizationReceipt(
                receipt_id=canonical_sha256(receipt_values),
                **receipt_values,
            )
            _write_new_file_at(
                staging_descriptor,
                _FINALIZATION_RECEIPT_FILENAME,
                canonical_model_json_bytes(receipt),
            )
            os.fsync(staging_descriptor)
            # The publisher reopens the staging directory under
            # `_signed_session_modes(policy)` immediately after `issue()` returns, which is
            # `_SESSION_MODE` for `linux-production`. Sealing the directory read-only here is
            # what the root-owned production signer does at the same point, and it is what
            # makes the published session immutable before it is renamed into place.
            os.fchmod(staging_descriptor, _SESSION_MODE)
            os.fsync(staging_descriptor)
            os.fsync(staging_root_descriptor)
        finally:
            if staging_descriptor >= 0:
                os.close(staging_descriptor)
            os.close(staging_root_descriptor)
        self._consumed_capture_tokens.add(capture_token_id)
        return marker

    def resume(self, binding: Any, *, staging_root: Path) -> Any:
        raise ShadowFixtureUnavailableError(
            "the fixture authority never resumes a crashed publish; it publishes once"
        )

    # -- the `LegacyShadowRecoveryVerifier` protocol, delegated to production -----------

    def verify(self, marker: Any) -> bool:
        return bool(self._keyring.verify(marker))

    def verify_finalization(self, receipt: Any) -> bool:
        return bool(self._keyring.verify_finalization(receipt))


# ---------------------------------------------------------------------------------------
# The producer chain
# ---------------------------------------------------------------------------------------


def _strategy_spec(strategy_id: str, action: Any) -> Any:
    from rquant.feature_contracts import FeatureRequirement, RequirementLevel
    from rquant.strategy_spec import (
        StateTransition,
        StrategyLifecycleState,
        StrategyRunMode,
        StrategySpec,
    )

    return StrategySpec(
        strategy_id=strategy_id,
        version=1,
        feature_contract_id="intraday-pit",
        min_feature_contract_version=1,
        required_features=(
            FeatureRequirement(
                name="rel_same_minute",
                level=RequirementLevel.REQUIRED,
                min_contract_version=1,
            ),
        ),
        optional_features=(),
        initial_state=StrategyLifecycleState.IDLE,
        transitions=(
            StateTransition(
                from_state=StrategyLifecycleState.IDLE,
                event="entry_ready",
                to_state=StrategyLifecycleState.ARMED,
            ),
        ),
        parameters={},
        allowed_actions=(action.value,),
        run_mode=StrategyRunMode.SHADOW,
        producer_commit=PRODUCER_COMMIT,
    )


def _publish_final_batch(
    store: Any,
    spool: Any,
    *,
    session_close: datetime,
    calendar_generation_id: str,
    candidate_id: str,
    action: Any,
) -> Any:
    """Drive the real `process_batch` and seal the feature session."""

    import pandas as pd

    from rquant.feature_contracts import (
        FeatureAvailability,
        FeatureBatchEnvelope,
        FeatureFieldStatus,
    )
    from rquant.strategy_runner import (
        StrategyDecision,
        StrategySourceBatchReceipt,
        canonical_feature_payload,
    )
    from rquant.strategy_spec import StrategyLifecycleState

    frame = pd.DataFrame({"ts_code": [candidate_id], "rel_same_minute": [2.0]})
    payload = canonical_feature_payload(frame, schema_version=1)
    envelope = FeatureBatchEnvelope(
        schema_version=1,
        batch_id=f"close-{store.spec.strategy_id}",
        contract_id="intraday-pit",
        contract_version=1,
        input_batch_ids=("minute-close",),
        sequence=0,
        event_time=session_close,
        available_at=session_close,
        decision_cutoff=session_close,
        actual_delay_seconds=0.0,
        row_count=1,
        content_hash=hashlib.sha256(payload).hexdigest(),
        field_statuses=(
            FeatureFieldStatus(
                name="rel_same_minute",
                status=FeatureAvailability.AVAILABLE,
                source_event_time=session_close,
                available_at=session_close,
                decision_cutoff=session_close,
                actual_delay_seconds=0.0,
            ),
        ),
        producer_commit=PRODUCER_COMMIT,
    )

    def evaluator(_spec: Any, state: Any, _features: Any) -> Any:
        return StrategyDecision(
            event="entry_ready",
            expected_from_state=state.state,
            expected_to_state=StrategyLifecycleState.ARMED,
            expected_action=action,
            action=action,
            reason_codes=("signal_family_shadow_fixture",),
            expires_after=timedelta(hours=6),
        )

    spool.publish(envelope, payload)
    marker = spool.publish_session_close_marker(
        trade_date=TRADE_DATE,
        session_close_at=session_close,
        produced_at=session_close + timedelta(seconds=1),
        calendar_generation_id=calendar_generation_id,
        complete_through=session_close,
        upstream_source_generation_id="7" * 64,
        upstream_final_sequence=0,
        upstream_final_batch_id="raw-close-0",
        upstream_final_content_hash="8" * 64,
    )
    store.process_batch(
        envelope,
        frame,
        feature_payload=payload,
        source_receipt=StrategySourceBatchReceipt(
            source_generation_id=marker.source_generation_id,
            source_sequence=envelope.sequence,
            source_batch_id=envelope.batch_id,
            source_content_hash=envelope.content_hash,
        ),
        dataset_snapshot_id="d" * 64,
        observed_at=session_close,
        evaluator=evaluator,
    )
    return marker


def build_shadow_fixture(
    generation_root: Path,
    *,
    relative_prefix: str,
    scratch_root: Path,
) -> ShadowFixtureResult:
    """Publish the four accepted exports and the calendar into the generation tree."""

    if sys.platform != "linux":
        raise ShadowFixtureUnavailableError(
            "the strategy-shadow fixture requires Linux: the production reader path runs "
            "under LegacyShadowFilesystemPolicy(mode='linux-production'), which "
            "legacy_shadow_export._validate_mount_policy refuses on any other platform"
        )

    from rquant.feature_spool import FeatureBatchSpool
    from rquant.legacy_shadow_export import (
        LegacyShadowFilesystemPolicy,
        LegacyShadowRunnerManifestBinding,
        LegacySurgeCollectionProof,
        _ProductionLegacyShadowDependencies,
        publish_isolated_runner_export,
        publish_legacy_monitor_export,
        publish_legacy_surge_export,
    )
    from rquant.runtime_market_session import MarketCalendarAuthority
    from rquant.runtime_shadow_validation import (
        Ed25519CompletionAttestationSigner,
        ShadowStrategyBinding,
        shadow_session_boundaries,
    )
    from rquant.signal_bus import SignalBusStore
    from rquant.signal_contracts import SignalAction
    from rquant.signal_router_runtime import (
        ReadonlySignalRouteAuthority,
        ReadonlyStrategyRunnerSignalSource,
        RoutingDecision,
        SignalRouteCursorStore,
        StrategyRunnerSignalSource,
        route_runner_signals,
    )
    from rquant.strategy_runner import StrategyRunnerStore

    _session_open, session_close = shadow_session_boundaries(TRADE_DATE)
    keys = scratch_root / "keys"
    recovery_client, recovery_public_key = _mint_keypair(keys, key_id=RECOVERY_KEY_ID)
    completion_client, completion_public_key = _mint_keypair(keys, key_id=COMPLETION_KEY_ID)
    completion_signer = Ed25519CompletionAttestationSigner(
        key_id=COMPLETION_KEY_ID,
        client=completion_client,
    )

    calendar = MarketCalendarAuthority.create(
        schema_version=1,
        exchange="SSE",
        producer_commit=PRODUCER_COMMIT,
        coverage_start=date(2026, 8, 1),
        coverage_end=date(2026, 8, 31),
        open_dates=(TRADE_DATE,),
        generated_at=datetime(2026, 8, 1, tzinfo=UTC),
    )

    # -- 1. two runner stores, each driven through the declared producer surfaces --------
    work = scratch_root / "producer"
    work.mkdir(mode=0o700, parents=True, exist_ok=True)
    stores: dict[str, Any] = {}
    markers: dict[str, Any] = {}
    bindings: list[Any] = []
    for strategy_id, action_value, candidate_id in STRATEGIES:
        action = SignalAction(action_value)
        source_id = f"strategy.{strategy_id}.v1"
        store = StrategyRunnerStore(
            work / f"{strategy_id}.sqlite3",
            spec=_strategy_spec(strategy_id, action),
            evaluator_contract_fingerprint=EVALUATOR_FINGERPRINT,
        )
        markers[source_id] = _publish_final_batch(
            store,
            FeatureBatchSpool(work / f"{strategy_id}-features"),
            session_close=session_close,
            calendar_generation_id=calendar.content_sha256,
            candidate_id=candidate_id,
            action=action,
        )
        stores[source_id] = store
        bindings.append(
            ShadowStrategyBinding(
                strategy_id=strategy_id,
                strategy_version=1,
                definition_fingerprint=REGISTRATION_FINGERPRINT,
                executable_fingerprint=EVALUATOR_FINGERPRINT,
            )
        )

    # -- 2. route every runner signal, so the close receipt has real drain evidence ------
    bus_path = work / "signal-bus.sqlite3"
    bus = SignalBusStore(bus_path)
    cursors = SignalRouteCursorStore(
        bus_path,
        routing_policy_fingerprint=ROUTING_POLICY_FINGERPRINT,
    )
    routed_at = session_close + timedelta(seconds=2)
    for source_id, store in stores.items():
        route_runner_signals(
            source_id=source_id,
            source=StrategyRunnerSignalSource(source_id=source_id, store=store),
            bus=bus,
            cursors=cursors,
            routed_at=routed_at,
            target_resolver=lambda _signal: RoutingDecision.no_target(
                routing_policy_fingerprint=ROUTING_POLICY_FINGERPRINT,
                reason_code="shadow_only",
            ),
            limit=10,
        )

    # -- 3. seal each session with an Ed25519 completion attestation ---------------------
    route_authority = ReadonlySignalRouteAuthority(
        path=bus_path,
        expected_routing_policy_fingerprint=ROUTING_POLICY_FINGERPRINT,
    )
    produced_at = session_close + timedelta(seconds=3)
    readonly_sources: dict[str, Any] = {}
    runner_manifest_bindings: list[Any] = []
    for binding in bindings:
        source_id = f"strategy.{binding.strategy_id}.v1"
        store = stores[source_id]
        segment_start, segment_final = store.runner_session_route_bounds(TRADE_DATE)
        drain = route_authority.read_drain_evidence(
            source_id=source_id,
            runner_generation_id=store.source_generation_id,
            strategy_spec_fingerprint=store.spec.spec_fingerprint,
            trade_date=TRADE_DATE,
            segment_start_sequence=segment_start,
            routed_through_sequence=segment_final,
            observed_at=produced_at,
        )
        store.publish_session_close_receipt(
            trade_date=TRADE_DATE,
            session_close_at=session_close,
            source_id=source_id,
            calendar_generation_id=calendar.content_sha256,
            producer_service_id=source_id,
            producer_instance_id=f"{binding.strategy_id}-primary",
            producer_version=PRODUCER_VERSION,
            produced_at=produced_at,
            feature_close_marker=markers[source_id],
            attestation_signer=completion_signer,
            strategy_registration_fingerprint=REGISTRATION_FINGERPRINT,
            executable_fingerprint=EVALUATOR_FINGERPRINT,
            candidate_schema_fingerprint=CANDIDATE_SCHEMA_FINGERPRINT,
            feature_registration_fingerprint=FEATURE_REGISTRATION_FINGERPRINT,
            feature_contract_fingerprint=FEATURE_CONTRACT_FINGERPRINT,
            producer_manifest_fingerprint=PRODUCER_MANIFEST_FINGERPRINT,
            route_evidence=drain,
        )
        readonly_sources[source_id] = ReadonlyStrategyRunnerSignalSource(
            source_id=source_id,
            path=store.path,
            expected_strategy_spec_fingerprint=store.spec.spec_fingerprint,
            expected_evaluator_contract_fingerprint=EVALUATOR_FINGERPRINT,
        )
        runner_manifest_bindings.append(
            LegacyShadowRunnerManifestBinding.create(
                strategy_id=binding.strategy_id,
                strategy_version=1,
                producer_manifest_fingerprint=PRODUCER_MANIFEST_FINGERPRINT,
                producer_commit=PRODUCER_COMMIT,
                producer_service_id=source_id,
                producer_instance_id=f"{binding.strategy_id}-primary",
                producer_version=PRODUCER_VERSION,
                strategy_registration_fingerprint=REGISTRATION_FINGERPRINT,
                strategy_spec_fingerprint=stores[source_id].spec.spec_fingerprint,
                evaluator_contract_fingerprint=EVALUATOR_FINGERPRINT,
                executable_fingerprint=EVALUATOR_FINGERPRINT,
            )
        )

    # -- 4. publish the four exports, in place, inside the generation --------------------
    authority = _FixtureEd25519RecoveryAuthority(
        key_id=RECOVERY_KEY_ID,
        client=recovery_client,
        public_key=recovery_public_key,
    )
    clock = _FixtureClock(session_close)
    authority.bind_clock(
        wall_clock=clock.wall_clock,
        monotonic_ns=clock.monotonic_ns,
        boot_id=lambda: BOOT_ID,
    )
    dependencies = _ProductionLegacyShadowDependencies(
        wall_clock=clock.wall_clock,
        monotonic_ns=clock.monotonic_ns,
        boot_id=lambda: BOOT_ID,
        recovery_signer=authority,
        recovery_verifier=authority,
        filesystem_policy=LegacyShadowFilesystemPolicy(mode="linux-production"),
    )

    shadow_root = generation_root / relative_prefix / "shadow"
    shadow_root.mkdir(mode=0o755, parents=True, exist_ok=True)
    calendar_path = shadow_root / "calendar.json"
    calendar_path.write_text(calendar.model_dump_json(), encoding="utf-8")
    calendar_path.chmod(0o444)

    monitor_rows = (
        {
            "trade_date": TRADE_DATE,
            "ts_code": STRATEGIES[0][2],
            "level": "attack_strong_carry",
            "trigger_time": datetime(2026, 8, 24, 14, 59),
        },
    )
    publish_legacy_monitor_export(
        root=shadow_root / "monitor",
        trade_date=TRADE_DATE,
        rows=monitor_rows,
        producer_commit=PRODUCER_COMMIT,
        producer_version=PRODUCER_VERSION,
        dependencies=dependencies,  # type: ignore[arg-type]
    )

    surge_source = work / "surge.jsonl"
    surge_source.write_text(
        json.dumps(
            {
                "ts_code": STRATEGIES[1][2],
                "confirmed_at": "14:59",
                "status": "confirmed",
            },
            separators=(",", ":"),
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    surge_proof = LegacySurgeCollectionProof.create(
        trade_date=TRADE_DATE,
        started_at=datetime(2026, 8, 24, 1, 25, tzinfo=UTC),
        first_success_at=datetime(2026, 8, 24, 1, 30, tzinfo=UTC),
        last_success_at=session_close,
        successful_snapshots=240,
        nonempty_successful_snapshots=240,
        empty_successful_snapshots=0,
        failed_snapshots=0,
        maximum_active_gap_seconds=60,
        maximum_consecutive_misses=0,
        ending_consecutive_misses=0,
        source_routes=("tushare_rt",),
        market_universe_id="9" * 64,
        market_universe_expected_count=5_000,
        minimum_market_coverage_count=4_900,
        minimum_market_coverage_bps=9_800,
        source_health="healthy",
    )
    publish_legacy_surge_export(
        root=shadow_root / "surge",
        trade_date=TRADE_DATE,
        events_path=surge_source,
        producer_commit=PRODUCER_COMMIT,
        producer_version=PRODUCER_VERSION,
        collection_proof=surge_proof,
        dependencies=dependencies,  # type: ignore[arg-type]
    )

    isolated_root = shadow_root / "isolated-runners"
    for strategy_id, _action, _candidate in STRATEGIES:
        publish_isolated_runner_export(
            root=isolated_root,
            strategy_id=strategy_id,
            trade_date=TRADE_DATE,
            source=readonly_sources[f"strategy.{strategy_id}.v1"],
            expected_commit=PRODUCER_COMMIT,
            dependencies=dependencies,  # type: ignore[arg-type]
        )

    relative_paths = tuple(
        sorted(
            str(path.relative_to(generation_root))
            for path in shadow_root.rglob("*")
            if path.is_file()
        )
    )
    descriptor: dict[str, Any] = {
        "boot_id": BOOT_ID,
        "calendar_content_sha256": calendar.content_sha256,
        "calendar_producer_commit": calendar.producer_commit,
        "calendar_relative_path": str(calendar_path.relative_to(generation_root)),
        "completion_active_key_id": COMPLETION_KEY_ID,
        "completion_active_public_key_pem": completion_public_key.decode("ascii"),
        "isolated_runner_relative_root": str(isolated_root.relative_to(generation_root)),
        "monitor_relative_root": str((shadow_root / "monitor").relative_to(generation_root)),
        "producer_commit": PRODUCER_COMMIT,
        "producer_version": PRODUCER_VERSION,
        "report_active_key_id": RECOVERY_KEY_ID,
        "report_active_public_key_pem": recovery_public_key.decode("ascii"),
        "runner_manifest_bindings": [
            binding.model_dump(mode="json") for binding in runner_manifest_bindings
        ],
        "strategy_bindings": [binding.model_dump(mode="json") for binding in bindings],
        "surge_relative_root": str((shadow_root / "surge").relative_to(generation_root)),
        "trade_date": TRADE_DATE.isoformat(),
    }
    return ShadowFixtureResult(relative_paths=relative_paths, descriptor=descriptor)


class _FixtureClock:
    """A monotone fake clock that stays inside the five-minute publish window."""

    def __init__(self, session_close: datetime) -> None:
        self._session_close = session_close
        self._tick = 0

    def wall_clock(self) -> datetime:
        # Twelve seconds apart keeps every capture/produce/finalize triple ordered and well
        # inside `_PUBLISH_WINDOW` (five minutes) for the four exports this publishes.
        return self._session_close + timedelta(seconds=10 + 2 * self._advance())

    def monotonic_ns(self) -> int:
        return (10 + 2 * self._tick) * 1_000_000_000

    def _advance(self) -> int:
        self._tick += 1
        return self._tick


def main(argv: list[str] | None = None) -> int:
    import argparse

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--generation-root", type=Path, required=True)
    parser.add_argument("--relative-prefix", default="signal-family/fixtures")
    arguments = parser.parse_args(argv)
    sys.path.insert(0, str(REPOSITORY_ROOT / "src"))
    scratch = Path(tempfile.mkdtemp(prefix="rquant-shadow-fixture-", dir=Path.home()))
    scratch.chmod(0o700)
    try:
        result = build_shadow_fixture(
            arguments.generation_root.resolve(),
            relative_prefix=arguments.relative_prefix,
            scratch_root=scratch,
        )
    finally:
        shutil.rmtree(scratch, ignore_errors=True)
    print(json.dumps(result.descriptor, indent=2, sort_keys=True))
    for relative in result.relative_paths:
        print(relative)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
