from __future__ import annotations

import base64
import json
import multiprocessing
import os
import shutil
import sqlite3
import stat
import subprocess
import sys
import tempfile
import threading
import time
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager, suppress
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

import rquant.source_broker_v2_runner as runner_module
from rquant.lab_source_stage import LabSourceStageStore
from rquant.runtime_contracts import canonical_sha256
from rquant.source_broker_v2 import (
    SOURCE_BROKER_V2_AUTHORITY_PURPOSE,
    SourceAuthorityKeyring,
    SourceBrokerV2ClaimOnceRequest,
    SourceBrokerV2ClaimOnceResponse,
    SourceBrokerV2ClaimStatus,
    SourceBrokerV2DispatchEnvelope,
    SourceBrokerV2DispatchOutcome,
    SourceBrokerV2DispatchResponse,
    SourceBrokerV2FinalizeEnvelope,
    SourceBrokerV2FinalizeResponse,
    SourceBrokerV2OutboxPhase,
    SourceBrokerV2ReplayRequest,
    SourceBrokerV2ReplayResponse,
    SourceBrokerV2ReplayStatus,
    source_authority_signature_payload,
)
from rquant.source_broker_v2_job_protocol import (
    SourceBrokerV2AuthorityRef,
    SourceBrokerV2JobIntentEnvelope,
    SourceBrokerV2JobOutcomeStatus,
    SourceBrokerV2NativeEvidence,
    canonical_job_sha256,
    canonical_request_bytes,
)
from rquant.source_broker_v2_queue import (
    SourceBrokerV2SchedulerQueue,
    SourceBrokerV2SchedulerQueueBackpressureError,
    SourceBrokerV2SchedulerQueueIntegrityError,
)
from rquant.source_broker_v2_runner import (
    SourceBrokerV2CredentialPolicy,
    SourceBrokerV2CredentialReader,
    SourceBrokerV2CredentialRoot,
    SourceBrokerV2JobRunner,
    SourceBrokerV2JobRunnerConfig,
    SourceBrokerV2JobRunnerState,
    SourceBrokerV2ProviderBinding,
    SourceBrokerV2ProviderRegistration,
    SourceBrokerV2RunnerBackpressureError,
    SourceBrokerV2RunnerError,
    SourceBrokerV2RunnerFencedError,
    SourceBrokerV2StaticProviderRegistry,
    SourceBrokerV2StrictNativeEvidenceVerifier,
    initialize_source_broker_v2_job_storage,
)
from rquant.strict_json import (
    canonical_json_bytes,
    canonical_model_json_bytes,
    strict_canonical_json_loads,
)

from .source_broker_v2_authorized_intent_fixture import (
    authorities,
    authorized_intent,
    stage_authorized_intent,
)

HASH_1 = "1" * 64
HASH_2 = "2" * 64
HASH_3 = "3" * 64
HASH_4 = "4" * 64
HASH_5 = "5" * 64
HASH_6 = "6" * 64
HASH_7 = "7" * 64
_STAGE_STORES_BY_TRANSPORT: dict[int, LabSourceStageStore] = {}

# Two cases here pit a real monotonic budget against real work - one needs it to
# expire, one needs it not to - and every budget in the file was a wall clock
# literal while the work was whatever the host could manage. On a loaded shard
# the host wins: #168 recorded a ~230ms stall inside a GitHub Actions shard
# blowing the 150ms total deadline and the 250ms lease derived from it together,
# so the post-deadline reconcile write was fenced, `_mark_reconcile_any`
# swallowed the fence, and the job stopped at CLAIMED - the failure names neither
# the deadline nor the lease. The 40ms sibling went the same way on the main push
# for 2b4099b.
#
# The mechanism is the one `test_source_broker_v2_service.py` already carries: a
# budget here is not wall clock, it is a multiple of what one full publish - the
# claim round trip, three native authority receipts, dispatch, the replay that
# confirms it, finalize, and the `synchronous = FULL` ledger writes between them -
# actually costs *in this process*. That cost is measured once at module scope, at
# the point in the shard where these cases run, so the measurement carries the
# shard's own degradation with it. `_runner()` is the only place budgets are
# built, so the deadline, the lease, the takeover grace and the provider sleep all
# take the same scale and the orderings between them survive it.
#
# The scale absorbs a host that is uniformly slower. It cannot absorb a stall
# dropped inside one case after the measurement was taken, and neither can any
# other measurement - so where a case needs one, the ordering it needs is secured
# by margin instead, and stated as such below.
#
# The scale is floored at 1.0: on a host at least as fast as the reference, every
# budget below is exactly the literal it reads as. The reference is what this
# calibration measures on an idle machine - 26-29ms on 3.11 and 29-36ms on 3.12
# over five runs of the max-of-three below - rounded up to the next round number,
# so an idle host scales by 1.0 and anything slower scales up. It is also what
# turns every literal in this file into a count: a budget of N seconds is never
# worth less than N/0.04 of the publishes the host has just demonstrated.
_PUBLISH_REFERENCE_SECONDS = 0.04
# The calibration publish is not under test and must not be the thing that fails
# when the host is slow, so it gets a watchdog rather than a budget.
_PUBLISH_CALIBRATION_DEADLINE_SECONDS = 120.0
# Production requires `lease_seconds >= total_deadline_seconds +
# takeover_grace_seconds`; every runner here derives its lease that way.
_LEASE_SLACK_SECONDS = 0.1
_TAKEOVER_GRACE_SECONDS = 0.05
# Both deadline cases below take this slack instead of the 100ms default. In the
# refusal case the lease is no longer a bound on the work at all, it is a guard
# on the bookkeeping that follows it: the outcome write happens *after* the
# deadline is already gone, and if the lease went with it the write is fenced,
# `_mark_reconcile_any` swallows the fence, and the case reads CLAIMED instead of
# the refusal it asserts - the exact CI failure. In the publish case it keeps a
# fenced terminal write from ever being how the case fails. A guard is secured by
# margin, not by a race: a second is ~25 of the publishes measured below.
_DEADLINE_CASE_LEASE_SLACK_SECONDS = 1.0
# The budget the fast chain has to fit inside. The 150ms literal it replaces was
# not merely uncalibrated, it was too small to calibrate: the scale it would be
# multiplied by is measured once, and what takes this case out is not a host that
# is uniformly slower - that the scale absorbs - but a single scheduler stall
# dropped inside a ~30ms chain. Measured under 8x CPU oversubscription, a publish
# costs 26-45ms nine times out of ten and 150-370ms the tenth, with no warning in
# the sample before it; at 150ms the case loses to that tenth publish and the
# calibration cannot see it coming. So the budget is sized to dominate the stall
# rather than to track the chain, exactly as the service file's blocked-dispatch
# deadline was (`_BLOCKED_PROVIDER_DEADLINE_SECONDS`, same 1.5s, same reason):
# ~37 measured publishes and 4x the worst stall observed under that load, with
# the calibrated scale still applying on top. What the case asserts is unchanged -
# a fast native chain publishes inside one total budget, and every wire call in it
# carries the same absolute deadline - and it still fails if the chain does not
# fit: putting a 2s link in front of the provider, or cutting this budget to 15ms,
# each turn it red on both interpreters.
_FAST_CHAIN_DEADLINE_SECONDS = 1.5
_publish_scale = 1.0


def _host(seconds: float) -> float:
    """Read a budget as wall clock on a quiet host, scaled by a loaded one."""

    return seconds * _publish_scale


def _spawn_initialize_runner_store(
    db_path_value: str,
    round_root_value: str,
    worker_id: int,
    worker_count: int,
) -> None:
    round_root = Path(round_root_value)
    ready = round_root / f"ready-{worker_id}"
    result = round_root / f"result-{worker_id}"
    ready.write_text("ready", encoding="ascii")
    deadline = time.monotonic() + 15
    while len(tuple(round_root.glob("ready-*"))) < worker_count:
        if time.monotonic() >= deadline:
            raise TimeoutError("spawn initialization barrier timed out")
        time.sleep(0.005)
    try:
        runner_module.initialize_source_broker_v2_job_storage(
            Path(db_path_value),
            busy_timeout_ms=10_000,
            max_inbox=8,
        )
        with sqlite3.connect(db_path_value) as connection:
            connection.row_factory = sqlite3.Row
            config = runner_module.load_source_broker_v2_job_store_config(connection)
            integrity = str(connection.execute("PRAGMA integrity_check").fetchone()[0])
            journal = str(connection.execute("PRAGMA journal_mode").fetchone()[0])
        if config.max_inbox != 8 or integrity != "ok" or journal.lower() != "wal":
            raise RuntimeError(
                f"invalid initialized store: max={config.max_inbox}, "
                f"integrity={integrity}, journal={journal}"
            )
    except BaseException as exc:
        result.write_text(f"{type(exc).__name__}: {exc}", encoding="utf-8")
        raise
    result.write_text("ok", encoding="ascii")


class _RunnerSourceAuthoritySecurity:
    def __init__(self) -> None:
        self.authority_id = "source-authority-runner-test"
        self.key_id = "source-authority-runner-key-v2"
        executable = shutil.which("openssl")
        if executable is None:
            pytest.skip("openssl is required for SourceBroker v2 runner tests")
        self._openssl = executable
        self._sign_lock = threading.Lock()
        self._directory = tempfile.TemporaryDirectory(prefix="rquant-runner-authority-test-")
        root = Path(self._directory.name)
        self._private_key = root / "private.pem"
        public_key = root / "public.pem"
        subprocess.run(
            (
                executable,
                "genpkey",
                "-algorithm",
                "ED25519",
                "-out",
                str(self._private_key),
            ),
            check=True,
            capture_output=True,
        )
        subprocess.run(
            (
                executable,
                "pkey",
                "-in",
                str(self._private_key),
                "-pubout",
                "-out",
                str(public_key),
            ),
            check=True,
            capture_output=True,
        )
        self._private_key.chmod(0o600)
        self.keyring = SourceAuthorityKeyring(
            expected_authority_id=self.authority_id,
            allowed_public_keys={self.key_id: public_key.read_bytes()},
            expected_purpose=SOURCE_BROKER_V2_AUTHORITY_PURPOSE,
            expected_schema_version=2,
        )

    def close(self) -> None:
        self._directory.cleanup()

    def sign(self, signing_bytes: bytes) -> str:
        with self._sign_lock:
            # Private to this call, and removed after it. Paths derived from the key
            # are shared by every holder of that key, and this lock reaches only this
            # process's threads: two signers would overwrite each other's scratch files
            # and one could read back a signature over the other's payload (064ffe8).
            scratch = self._private_key.parent
            payload_descriptor, payload_name = tempfile.mkstemp(dir=scratch, suffix=".payload")
            signature_descriptor, signature_name = tempfile.mkstemp(
                dir=scratch, suffix=".signature"
            )
            payload_path = Path(payload_name)
            signature_path = Path(signature_name)
            try:
                os.close(signature_descriptor)
                with os.fdopen(payload_descriptor, "wb") as handle:
                    handle.write(source_authority_signature_payload(signing_bytes))
                completed = subprocess.run(
                    (
                        self._openssl,
                        "pkeyutl",
                        "-sign",
                        "-inkey",
                        str(self._private_key),
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
                    raise RuntimeError(completed.stderr.decode("utf-8", errors="replace"))
                return base64.b64encode(signature_path.read_bytes()).decode("ascii")
            finally:
                for path in (payload_path, signature_path):
                    with suppress(OSError):
                        path.unlink()


@pytest.fixture
def source_security() -> Iterator[_RunnerSourceAuthoritySecurity]:
    security = _RunnerSourceAuthoritySecurity()
    try:
        yield security
    finally:
        security.close()


@contextmanager
def _serving_runner(
    runner: SourceBrokerV2JobRunner,
    *,
    poll_interval_seconds: float,
) -> Iterator[threading.Thread]:
    worker = threading.Thread(
        target=runner.serve_forever,
        kwargs={"poll_interval_seconds": poll_interval_seconds},
    )
    worker.start()
    try:
        yield worker
    finally:
        active_error = sys.exc_info()[1]
        runner.stop()
        worker.join(timeout=5)
        if worker.is_alive():
            cleanup_error = AssertionError("runner worker did not stop within its cleanup deadline")
            if active_error is not None:
                raise BaseExceptionGroup(
                    "runner body and worker cleanup both failed",
                    (active_error, cleanup_error),
                )
            raise cleanup_error


class _RunnerTestTransport:
    def __init__(
        self,
        security: _RunnerSourceAuthoritySecurity,
        *,
        outcome: str = "SUCCESS",
        lose_dispatch_once: bool = False,
        block_dispatch: bool = False,
    ) -> None:
        self._security = security
        self.source_authority_keyring = security.keyring
        self.outcome = outcome
        self.lose_dispatch_once = lose_dispatch_once
        self.dispatch_calls = 0
        self.finalize_calls = 0
        self.replay_calls = 0
        self.claim_once_calls = 0
        self.deadlines: list[float | None] = []
        self.dispatch_entered = threading.Event()
        self.second_dispatch_entered = threading.Event()
        self.release_dispatch = threading.Event()
        if not block_dispatch:
            self.release_dispatch.set()
        self._lock = threading.Lock()
        self._dispatch_results: dict[str, bytes] = {}
        self._finalize_results: dict[str, bytes] = {}
        self._source_claims: dict[str, SourceBrokerV2ClaimOnceRequest] = {}
        self._inflight: set[str] = set()

    def claim_once(self, payload: bytes, *, deadline: float | None = None) -> bytes:
        self.deadlines.append(deadline)
        request = SourceBrokerV2ClaimOnceRequest.model_validate_json(payload)
        with self._lock:
            self.claim_once_calls += 1
            results = (
                self._dispatch_results
                if request.phase is SourceBrokerV2OutboxPhase.DISPATCH
                else self._finalize_results
            )
            result = results.get(request.operation_id)
            existing = self._source_claims.get(request.operation_id)
            if result is not None:
                if request.phase is SourceBrokerV2OutboxPhase.DISPATCH:
                    dispatch = SourceBrokerV2DispatchResponse.model_validate_json(result)
                    status = SourceBrokerV2ClaimStatus(dispatch.outcome.value)
                else:
                    status = SourceBrokerV2ClaimStatus.SUCCESS
            elif request.operation_id in self._inflight:
                status = SourceBrokerV2ClaimStatus.INFLIGHT
            elif (
                existing is None
                or (
                    existing.executor_owner_token_hash == request.executor_owner_token_hash
                    and existing.executor_generation == request.executor_generation
                )
                or datetime.now(UTC) >= existing.not_before_takeover_at
            ):
                self._source_claims[request.operation_id] = request
                status = SourceBrokerV2ClaimStatus.DEFINITIVELY_ABSENT
            else:
                status = SourceBrokerV2ClaimStatus.INFLIGHT
        unsigned = SourceBrokerV2ClaimOnceResponse(
            saga_id=request.saga_id,
            operation_id=request.operation_id,
            phase=request.phase,
            request_hash=request.request_hash,
            operation_request_hash=request.operation_request_hash,
            challenge=request.challenge,
            claim_binding_hash=request.claim_binding_hash,
            claim_generation=request.claim_generation,
            scheduler_fencing_token=request.scheduler_fencing_token,
            executor_owner_token_hash=request.executor_owner_token_hash,
            executor_generation=request.executor_generation,
            max_external_deadline=request.max_external_deadline,
            not_before_takeover_at=request.not_before_takeover_at,
            authority_id=self._security.authority_id,
            key_id=self._security.key_id,
            observed_at=datetime.now(UTC),
            status=status,
            result=result,
            result_hash=(
                None if result is None else canonical_sha256(strict_canonical_json_loads(result))
            ),
            signature=base64.b64encode(b"0" * 64).decode("ascii"),
        )
        response = unsigned.model_copy(
            update={"signature": self._security.sign(unsigned.signing_bytes())}
        )
        return canonical_model_json_bytes(response)

    def _require_current_grant(
        self,
        *,
        operation_id: str,
        request_hash: str,
        receipt: SourceBrokerV2ClaimOnceResponse,
    ) -> None:
        granted = self._source_claims.get(operation_id)
        if (
            granted is None
            or receipt.status is not SourceBrokerV2ClaimStatus.DEFINITIVELY_ABSENT
            or receipt.request_hash != granted.request_hash
            or receipt.operation_request_hash != request_hash
        ):
            raise PermissionError("source operation lacks the current native grant")
        self.source_authority_keyring.require_verified_claim(
            request=granted,
            receipt=receipt,
        )

    def dispatch(self, payload: bytes, *, deadline: float | None = None) -> bytes:
        self.deadlines.append(deadline)
        envelope = SourceBrokerV2DispatchEnvelope.model_validate_json(payload)
        request = envelope.request
        with self._lock:
            self._require_current_grant(
                operation_id=request.operation_id,
                request_hash=request.request_hash,
                receipt=envelope.claim_receipt,
            )
            self.dispatch_calls += 1
            self._inflight.add(request.operation_id)
            if self.dispatch_calls > 1:
                self.second_dispatch_entered.set()
        self.dispatch_entered.set()
        if not self.release_dispatch.wait(timeout=5):
            raise TimeoutError("test did not release source dispatch")
        with self._lock:
            existing = self._dispatch_results.get(request.operation_id)
            if existing is not None:
                return existing
        response = SourceBrokerV2DispatchResponse(
            saga_id=request.saga_id,
            operation_id=request.operation_id,
            call_id=request.call_id,
            request_hash=request.request_hash,
            outcome=SourceBrokerV2DispatchOutcome(self.outcome),
            response=canonical_json_bytes({"rows": [{"code": "000001.SZ"}]}),
            response_hash=canonical_sha256({"rows": [{"code": "000001.SZ"}]}),
            transport_receipt=canonical_json_bytes({"provider": "closed-test", "ok": True}),
        )
        encoded = canonical_model_json_bytes(response)
        with self._lock:
            self._dispatch_results[request.operation_id] = encoded
            self._inflight.discard(request.operation_id)
        if self.lose_dispatch_once:
            self.lose_dispatch_once = False
            raise ConnectionError("transport committed but response was lost")
        return encoded

    def finalize(self, payload: bytes, *, deadline: float | None = None) -> bytes:
        self.deadlines.append(deadline)
        envelope = SourceBrokerV2FinalizeEnvelope.model_validate_json(payload)
        request = envelope.request
        with self._lock:
            self._require_current_grant(
                operation_id=request.operation_id,
                request_hash=request.request_hash,
                receipt=envelope.claim_receipt,
            )
            self.finalize_calls += 1
            self._inflight.add(request.operation_id)
            existing = self._finalize_results.get(request.operation_id)
            if existing is not None:
                return existing
        receipt = {"closed": True, "dispatch": request.dispatch_evidence_hash}
        response = SourceBrokerV2FinalizeResponse(
            saga_id=request.saga_id,
            operation_id=request.operation_id,
            request_hash=request.request_hash,
            final_receipt=canonical_json_bytes(receipt),
            final_receipt_hash=canonical_sha256(receipt),
        )
        encoded = canonical_model_json_bytes(response)
        with self._lock:
            self._finalize_results[request.operation_id] = encoded
            self._inflight.discard(request.operation_id)
        return encoded

    def replay(self, payload: bytes, *, deadline: float | None = None) -> bytes:
        self.deadlines.append(deadline)
        request = SourceBrokerV2ReplayRequest.model_validate_json(payload)
        with self._lock:
            self.replay_calls += 1
            results = (
                self._dispatch_results
                if request.phase is SourceBrokerV2OutboxPhase.DISPATCH
                else self._finalize_results
            )
            result = results.get(request.operation_id)
        unsigned = SourceBrokerV2ReplayResponse(
            saga_id=request.saga_id,
            operation_id=request.operation_id,
            phase=request.phase,
            request_hash=request.request_hash,
            challenge=request.challenge,
            status=(
                SourceBrokerV2ReplayStatus.ABSENT
                if result is None
                else SourceBrokerV2ReplayStatus.FOUND
            ),
            result=result,
            result_hash=(
                None if result is None else canonical_sha256(strict_canonical_json_loads(result))
            ),
            authority_id=self._security.authority_id,
            key_id=self._security.key_id,
            signature=base64.b64encode(b"0" * 64).decode("ascii"),
        )
        response = unsigned.model_copy(
            update={"signature": self._security.sign(unsigned.signing_bytes())}
        )
        return canonical_model_json_bytes(response)


def _authority(
    kind: str,
    *,
    source_transport: _RunnerTestTransport | None = None,
) -> SourceBrokerV2AuthorityRef:
    if source_transport is not None:
        return SourceBrokerV2AuthorityRef(
            authority_id=source_transport._security.authority_id,
            key_id=source_transport._security.key_id,
            purpose=SOURCE_BROKER_V2_AUTHORITY_PURPOSE,
            schema_version=2,
            generation=7,
            fence_hash=HASH_7,
        )
    return SourceBrokerV2AuthorityRef(
        authority_id=f"{kind}-authority",
        key_id=f"{kind}-key-v2",
        purpose=f"rquant-{kind}-receipt",
        schema_version=2,
        generation=7,
        fence_hash=HASH_7,
    )


def _intent(
    transport: _RunnerTestTransport,
    symbol: str = "000001.SZ",
    *,
    deadline: datetime | None = None,
) -> SourceBrokerV2JobIntentEnvelope:
    del deadline
    intent = authorized_intent(
        source_authority=_authority("source", source_transport=transport),
        symbol=symbol,
    )
    stage_authorized_intent(_STAGE_STORES_BY_TRANSPORT[id(transport)], intent)
    return intent


class _AuthorityClient:
    def __init__(self, kind: str, *, forged: bool = False, delay_seconds: float = 0) -> None:
        self.kind = kind
        self.forged = forged
        self.delay_seconds = delay_seconds
        self.observe_calls = 0
        self.verify_calls = 0

    def observe(
        self,
        *,
        intent: SourceBrokerV2JobIntentEnvelope,
        authority: SourceBrokerV2AuthorityRef,
        subject_hash: str,
        deadline: float,
    ) -> SourceBrokerV2NativeEvidence:
        self.observe_calls += 1
        if self.delay_seconds:
            time.sleep(self.delay_seconds)
        challenge = canonical_job_sha256(
            {"kind": self.kind, "observation": self.observe_calls, "operation": intent.operation_id}
        )
        request = canonical_request_bytes(
            {
                "challenge": challenge,
                "kind": self.kind,
                "operation_hash": intent.operation_hash,
                "operation_id": intent.operation_id,
                "subject_hash": subject_hash,
            }
        )
        unsigned = {
            "authority_id": authority.authority_id,
            "challenge": challenge,
            "fence_hash": authority.fence_hash,
            "generation": authority.generation,
            "key_id": authority.key_id,
            "kind": self.kind,
            "operation_hash": intent.operation_hash,
            "operation_id": intent.operation_id,
            "purpose": authority.purpose,
            "request_hash": canonical_job_sha256(request),
            "schema_version": authority.schema_version,
            "subject_hash": subject_hash,
        }
        signature = canonical_job_sha256({"receipt": unsigned, "secret": f"{self.kind}-secret"})
        receipt = canonical_request_bytes(
            {**unsigned, "signature": HASH_3 if self.forged else signature}
        )
        assert time.monotonic() < deadline
        return SourceBrokerV2NativeEvidence.create(
            kind=self.kind,
            request=request,
            receipt=receipt,
        )

    def verify(
        self,
        *,
        intent: SourceBrokerV2JobIntentEnvelope,
        authority: SourceBrokerV2AuthorityRef,
        subject_hash: str,
        evidence: SourceBrokerV2NativeEvidence,
        deadline: float,
    ) -> None:
        self.verify_calls += 1
        request = evidence.request_json
        receipt = evidence.receipt_json
        unsigned = {key: value for key, value in receipt.items() if key != "signature"}
        signature = canonical_job_sha256({"receipt": unsigned, "secret": f"{self.kind}-secret"})
        expected = {
            "authority_id": authority.authority_id,
            "challenge": request["challenge"],
            "fence_hash": authority.fence_hash,
            "generation": authority.generation,
            "key_id": authority.key_id,
            "kind": self.kind,
            "operation_hash": intent.operation_hash,
            "operation_id": intent.operation_id,
            "purpose": authority.purpose,
            "request_hash": canonical_job_sha256(evidence.request),
            "schema_version": authority.schema_version,
            "subject_hash": subject_hash,
        }
        if unsigned != expected or receipt.get("signature") != signature:
            raise ValueError(f"{self.kind} native receipt signature or binding is invalid")
        assert time.monotonic() < deadline


class _DelayTransport:
    def __init__(self, transport: _RunnerTestTransport, delay_seconds: float) -> None:
        self._transport = transport
        self.delay_seconds = delay_seconds

    def _call(self, name: str, payload: bytes, *, deadline: float | None) -> bytes:
        time.sleep(self.delay_seconds)
        return getattr(self._transport, name)(payload, deadline=deadline)

    def claim_once(self, payload: bytes, *, deadline: float | None = None) -> bytes:
        return self._call("claim_once", payload, deadline=deadline)

    def replay(self, payload: bytes, *, deadline: float | None = None) -> bytes:
        return self._call("replay", payload, deadline=deadline)

    def dispatch(self, payload: bytes, *, deadline: float | None = None) -> bytes:
        return self._call("dispatch", payload, deadline=deadline)

    def finalize(self, payload: bytes, *, deadline: float | None = None) -> bytes:
        return self._call("finalize", payload, deadline=deadline)


class _UnknownReplayTransport(_DelayTransport):
    def replay(self, payload: bytes, *, deadline: float | None = None) -> bytes:
        del deadline
        request = SourceBrokerV2ReplayRequest.model_validate_json(payload)
        unsigned = SourceBrokerV2ReplayResponse(
            saga_id=request.saga_id,
            operation_id=request.operation_id,
            phase=request.phase,
            request_hash=request.request_hash,
            challenge=request.challenge,
            status=SourceBrokerV2ReplayStatus.UNKNOWN,
            authority_id=self._transport._security.authority_id,
            key_id=self._transport._security.key_id,
            signature="MA==",
        )
        response = unsigned.model_copy(
            update={"signature": self._transport._security.sign(unsigned.signing_bytes())}
        )
        self._transport.replay_calls += 1
        return canonical_model_json_bytes(response)


class _ForgedReplayTransport(_DelayTransport):
    def replay(self, payload: bytes, *, deadline: float | None = None) -> bytes:
        response = SourceBrokerV2ReplayResponse.model_validate_json(
            self._transport.replay(payload, deadline=deadline)
        )
        return canonical_model_json_bytes(response.model_copy(update={"signature": "MA=="}))


def _registration(
    transport: _RunnerTestTransport,
    *,
    delay_seconds: float = 0,
    unknown_replay: bool = False,
    forged_kind: str | None = None,
    seen_credentials: list[str] | None = None,
    attempted_env: list[str] | None = None,
) -> SourceBrokerV2ProviderRegistration:
    claim = _AuthorityClient("claim", forged=forged_kind == "claim")
    quota = _AuthorityClient("quota", forged=forged_kind == "quota")
    lineage = _AuthorityClient("lineage", forged=forged_kind == "lineage")
    source_transport: Any
    if forged_kind == "source":
        source_transport = _ForgedReplayTransport(transport, delay_seconds)
    elif unknown_replay:
        source_transport = _UnknownReplayTransport(transport, delay_seconds)
    elif delay_seconds:
        source_transport = _DelayTransport(transport, delay_seconds)
    else:
        source_transport = transport

    def factory(credentials: SourceBrokerV2CredentialReader) -> SourceBrokerV2ProviderBinding:
        if seen_credentials is not None:
            seen_credentials.append(credentials.env("RQUANT_SOURCE_TOKEN"))
        if attempted_env is not None:
            with pytest.raises(PermissionError, match="allowlist"):
                credentials.env("HOME")
            attempted_env.append("HOME")
        verifier = SourceBrokerV2StrictNativeEvidenceVerifier.for_nonproduction_test(
            source_keyring=transport.source_authority_keyring,
            claim_client=claim,
            quota_client=quota,
            lineage_client=lineage,
        )
        return SourceBrokerV2ProviderBinding(
            transport=source_transport,
            verifier=verifier,
        )

    return SourceBrokerV2ProviderRegistration.for_nonproduction_test(
        factory=factory,
        credential_policy=SourceBrokerV2CredentialPolicy(
            allowed_env=("RQUANT_SOURCE_TOKEN",),
            allowed_files=(),
        ),
    )


def _runner(
    tmp_path: Path,
    transport: _RunnerTestTransport,
    *,
    owner_id: str = "owner-a",
    total_deadline_seconds: float = 3.0,
    lease_slack_seconds: float = _LEASE_SLACK_SECONDS,
    delay_seconds: float = 0,
    unknown_replay: bool = False,
    forged_kind: str | None = None,
    seen_credentials: list[str] | None = None,
    attempted_env: list[str] | None = None,
    max_inbox: int = 100,
    max_batch: int = 10,
    stage_store: LabSourceStageStore | None = None,
) -> SourceBrokerV2JobRunner:
    db_path = tmp_path / "runner.sqlite3"
    initialize_source_broker_v2_job_storage(
        db_path,
        busy_timeout_ms=2_000,
        max_inbox=max_inbox,
    )
    if stage_store is None:
        stage_store = LabSourceStageStore(
            tmp_path / "source-stage.sqlite3",
            queue_store_path=db_path,
            manifest_keyring=authorities().authorization_keyring,
            authorization_keyring=authorities().authorization_keyring,
        )
    _STAGE_STORES_BY_TRANSPORT[id(transport)] = stage_store
    registry = SourceBrokerV2StaticProviderRegistry.for_nonproduction_test(
        {
            "daily-bars": _registration(
                transport,
                delay_seconds=_host(delay_seconds),
                unknown_replay=unknown_replay,
                forged_kind=forged_kind,
                seen_credentials=seen_credentials,
                attempted_env=attempted_env,
            )
        }
    )
    return SourceBrokerV2JobRunner(
        db_path=db_path,
        registry=registry,
        # Every wall clock quantity a case hands to this helper - the budget, the
        # lease that has to cover it, the grace inside the claim it issues and
        # the provider sleep it races - is written at the call site as quiet-host
        # seconds and scaled here, once. Scaling them together is the whole
        # point: a deadline that grows while the sleep it must fire inside stays
        # put is not calibration, it is a different case.
        config=SourceBrokerV2JobRunnerConfig(
            owner_id=owner_id,
            lease_seconds=_host(total_deadline_seconds + lease_slack_seconds),
            total_deadline_seconds=_host(total_deadline_seconds),
            takeover_grace_seconds=_host(_TAKEOVER_GRACE_SECONDS),
            busy_timeout_ms=2_000,
            max_batch=max_batch,
            max_inbox=max_inbox,
        ),
        manifest_keyring=authorities().authorization_keyring,
        authorization_keyring=authorities().authorization_keyring,
        stage_store=stage_store,
    )


def _use_fast_source_signatures(
    monkeypatch: pytest.MonkeyPatch,
    transport: _RunnerTestTransport,
) -> None:
    monkeypatch.setattr(transport._security, "sign", lambda _payload: "MA==")
    monkeypatch.setattr(
        transport.source_authority_keyring,
        "require_verified_replay",
        lambda **_kwargs: None,
    )
    monkeypatch.setattr(
        transport.source_authority_keyring,
        "require_verified_claim",
        lambda **_kwargs: None,
    )


@pytest.fixture(scope="module", autouse=True)
def _calibrate_publish_cost(tmp_path_factory: pytest.TempPathFactory) -> None:
    """Time one full publish, three times, in this process.

    A publish is the whole quantity the budgeted cases race: the claim round
    trip, the three native authority receipts, the dispatch, the replay that
    confirms it, the finalize, and the `synchronous = FULL` ledger writes
    between them. Measuring it - rather than any one stage - is what lets a
    budget be stated as a multiple of the work it bounds instead of as a wall
    clock literal that a 2 vCPU runner can outrun.

    It has to be measured here, at module scope, and not inside the cases: the
    cases are holding the budget under test by the time they would ask, and a
    measurement taken before the shard loaded the host would carry none of the
    degradation the budgets exist to absorb. The slowest of three wins, which
    keeps the scale on the conservative side of a host that is still degrading.
    """

    global _publish_scale

    security = _RunnerSourceAuthoritySecurity()
    samples: list[float] = []
    try:
        with pytest.MonkeyPatch.context() as patch:
            patch.setenv("RQUANT_SOURCE_TOKEN", "source-secret")
            transport = _RunnerTestTransport(security)
            _use_fast_source_signatures(patch, transport)
            runner = _runner(
                tmp_path_factory.mktemp("publish-cost-calibration"),
                transport,
                total_deadline_seconds=_PUBLISH_CALIBRATION_DEADLINE_SECONDS,
            )
            for index in range(3):
                intent = _intent(transport, f"{index + 900:06d}.SZ")
                runner.enqueue_intent(intent)
                started = time.monotonic()
                claimed = runner.run_once()
                samples.append(time.monotonic() - started)
                assert claimed == 1, "calibration publish did not run exactly one job"
                state = runner.get_state(intent.operation_id)
                assert state is SourceBrokerV2JobRunnerState.PUBLISHED, (
                    f"calibration publish ended in {state}"
                )
    finally:
        security.close()
    _publish_scale = max(1.0, max(samples) / _PUBLISH_REFERENCE_SECONDS)


def test_runner_rejects_direct_legacy_envelope_before_provider_effect(
    tmp_path: Path,
    source_security: _RunnerSourceAuthoritySecurity,
) -> None:
    transport = _RunnerTestTransport(source_security)
    runner = _runner(tmp_path, transport)
    authorized = _intent(transport)
    legacy = SourceBrokerV2JobIntentEnvelope.model_validate(
        {
            **authorized.model_dump(mode="python"),
            "authorization": None,
            "authorization_payload": None,
            "authorization_payload_commitment": None,
            "authorization_template_commitment": None,
        },
        strict=True,
    )

    with pytest.raises(SourceBrokerV2RunnerError, match="authorization"):
        runner.enqueue_intent(legacy)
    assert transport.dispatch_calls == 0
    with sqlite3.connect(tmp_path / "runner.sqlite3") as connection:
        assert connection.execute("SELECT COUNT(*) FROM source_broker_v2_jobs").fetchone() == (0,)


def test_runner_uses_allowlisted_credentials_and_publishes_four_evidence_chain(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    source_security: _RunnerSourceAuthoritySecurity,
) -> None:
    monkeypatch.setenv("RQUANT_SOURCE_TOKEN", "source-secret")
    transport = _RunnerTestTransport(source_security)
    seen: list[str] = []
    runner = _runner(tmp_path, transport, seen_credentials=seen)
    intent_created_at = datetime.now(UTC)
    intent = _intent(transport)
    assert intent.deadline >= intent_created_at + timedelta(seconds=29)

    runner.enqueue_intent(intent)
    assert runner.run_once() == 1
    outcome = runner.get_outcome(intent.operation_id)

    assert runner.get_state(intent.operation_id) is SourceBrokerV2JobRunnerState.PUBLISHED
    assert outcome.status is SourceBrokerV2JobOutcomeStatus.SUCCESS
    assert outcome.quota_evidence.receipt
    assert seen == ["source-secret"]
    assert b"source-secret" not in (tmp_path / "runner.sqlite3").read_bytes()


def test_credential_reader_denies_environment_outside_per_source_allowlist(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    source_security: _RunnerSourceAuthoritySecurity,
) -> None:
    monkeypatch.setenv("RQUANT_SOURCE_TOKEN", "source-secret")
    monkeypatch.setenv("HOME", "/should/not/be/read")
    transport = _RunnerTestTransport(source_security)
    attempted: list[str] = []
    runner = _runner(tmp_path, transport, attempted_env=attempted)
    runner.enqueue_intent(_intent(transport))

    assert runner.run_once() == 1
    assert attempted == ["HOME"]


def _secure_credential_reader(
    root: Path,
    allowed_file: Path,
) -> SourceBrokerV2CredentialReader:
    root.mkdir(mode=0o700, parents=True, exist_ok=True)
    root.chmod(0o700)
    root_identity = root.stat()
    return SourceBrokerV2CredentialReader(
        SourceBrokerV2CredentialPolicy(
            allowed_env=(),
            allowed_files=(allowed_file,),
            trusted_file_roots=(
                SourceBrokerV2CredentialRoot(
                    path=root,
                    owner_uid=root_identity.st_uid,
                    owner_gid=root_identity.st_gid,
                ),
            ),
        )
    )


def test_credential_file_reader_rejects_symlink_ancestor(tmp_path: Path) -> None:
    root = tmp_path / "credentials"
    actual = tmp_path / "actual"
    actual.mkdir(mode=0o700)
    actual.chmod(0o700)
    secret = actual / "token"
    secret.write_text("stolen", encoding="utf-8")
    secret.chmod(0o600)
    root.mkdir(mode=0o700)
    root.chmod(0o700)
    (root / "linked").symlink_to(actual, target_is_directory=True)
    requested = root / "linked" / "token"
    reader = _secure_credential_reader(root, requested)

    with pytest.raises(SourceBrokerV2RunnerError, match="symlink|directory|trusted"):
        reader.file(requested)


def test_credential_file_reader_uses_open_descriptor_across_leaf_exchange(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "credentials"
    secret = root / "token"
    reader = _secure_credential_reader(root, secret)
    secret.write_text("original", encoding="utf-8")
    secret.chmod(0o600)
    original_open = runner_module.os.open
    exchanged = False

    def exchange_after_open(
        path: str | bytes,
        flags: int,
        mode: int = 0o777,
        *,
        dir_fd: int | None = None,
    ) -> int:
        nonlocal exchanged
        descriptor = original_open(path, flags, mode, dir_fd=dir_fd)
        if path == "token" and not flags & os.O_DIRECTORY and not exchanged:
            exchanged = True
            secret.rename(root / "opened-token")
            secret.write_text("attacker", encoding="utf-8")
            secret.chmod(0o600)
        return descriptor

    monkeypatch.setattr(runner_module.os, "open", exchange_after_open)

    assert reader.file(secret) == "original"
    assert exchanged


def test_credential_file_reader_rejects_hardlink_and_special_file(tmp_path: Path) -> None:
    root = tmp_path / "credentials"
    original = root / "original"
    hardlink = root / "hardlink"
    reader = _secure_credential_reader(root, hardlink)
    original.write_text("secret", encoding="utf-8")
    original.chmod(0o600)
    os.link(original, hardlink)

    with pytest.raises(SourceBrokerV2RunnerError, match="hardlink"):
        reader.file(hardlink)

    fifo = root / "fifo"
    os.mkfifo(fifo, mode=0o600)
    fifo_reader = _secure_credential_reader(root, fifo)
    with pytest.raises(SourceBrokerV2RunnerError, match="regular"):
        fifo_reader.file(fifo)


def test_credential_file_reader_rejects_wrong_owner_or_mode(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "credentials"
    secret = root / "token"
    reader = _secure_credential_reader(root, secret)
    secret.write_text("secret", encoding="utf-8")
    secret.chmod(0o644)
    with pytest.raises(SourceBrokerV2RunnerError, match="mode"):
        reader.file(secret)

    secret.chmod(0o600)
    original_fstat = runner_module.os.fstat

    def wrong_owner(descriptor: int) -> os.stat_result:
        observed = original_fstat(descriptor)
        if stat.S_ISREG(observed.st_mode):
            fields = list(observed)
            fields[4] = observed.st_uid + 1
            return os.stat_result(fields)
        return observed

    monkeypatch.setattr(runner_module.os, "fstat", wrong_owner)
    with pytest.raises(SourceBrokerV2RunnerError, match="owner"):
        reader.file(secret)


def test_production_registry_rejects_nonproduction_authority_wrapper(
    tmp_path: Path,
    source_security: _RunnerSourceAuthoritySecurity,
) -> None:
    transport = _RunnerTestTransport(source_security)
    registration = _registration(transport)

    with pytest.raises(TypeError, match="production"):
        SourceBrokerV2StaticProviderRegistry.for_production({"daily-bars": registration})
    assert transport.dispatch_calls == 0


def test_production_verifier_rejects_arbitrary_local_authority_wrappers() -> None:
    with pytest.raises(TypeError, match="authority runtime"):
        SourceBrokerV2StrictNativeEvidenceVerifier.for_production(
            runtime=object(),  # type: ignore[arg-type]
            scheduler_clients=object(),  # type: ignore[arg-type]
        )


def test_production_object_new_registry_is_rejected_before_transport_effect(
    tmp_path: Path,
) -> None:
    class WrongTransport:
        def __init__(self) -> None:
            self.effects = 0

        def _effect(self, _payload: bytes, *, deadline: float | None = None) -> bytes:
            del deadline
            self.effects += 1
            return b"{}"

        claim_once = _effect
        replay = _effect
        dispatch = _effect
        finalize = _effect

    wrong_transport = WrongTransport()
    forged_verifier = object.__new__(SourceBrokerV2StrictNativeEvidenceVerifier)
    forged_verifier._profile = runner_module.SourceBrokerV2RegistryProfile.PRODUCTION
    forged_binding = SourceBrokerV2ProviderBinding(
        transport=wrong_transport,
        verifier=forged_verifier,
    )
    forged_registration = object.__new__(SourceBrokerV2ProviderRegistration)
    forged_registration.profile = runner_module.SourceBrokerV2RegistryProfile.PRODUCTION
    forged_registration.credential_policy = SourceBrokerV2CredentialPolicy()
    forged_registration._factory = None
    forged_registration._binding = forged_binding

    registry = object.__new__(SourceBrokerV2StaticProviderRegistry)
    registry._profile = runner_module.SourceBrokerV2RegistryProfile.PRODUCTION
    registry._registrations = {"daily-bars": forged_registration}
    db_path = tmp_path / "forged-registry.sqlite3"
    initialize_source_broker_v2_job_storage(db_path, busy_timeout_ms=2_000)
    stage_store = LabSourceStageStore(
        tmp_path / "forged-registry-stage.sqlite3",
        queue_store_path=db_path,
        manifest_keyring=authorities().authorization_keyring,
        authorization_keyring=authorities().authorization_keyring,
    )

    with pytest.raises(SourceBrokerV2RunnerError, match="production registry"):
        SourceBrokerV2JobRunner(
            db_path=db_path,
            registry=registry,
            config=SourceBrokerV2JobRunnerConfig(
                owner_id="forged-registry",
                lease_seconds=3.1,
                total_deadline_seconds=3.0,
                takeover_grace_seconds=0.05,
            ),
            manifest_keyring=authorities().authorization_keyring,
            authorization_keyring=authorities().authorization_keyring,
            stage_store=stage_store,
        )

    assert wrong_transport.effects == 0


def test_response_loss_replays_before_any_second_dispatch(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    source_security: _RunnerSourceAuthoritySecurity,
) -> None:
    monkeypatch.setenv("RQUANT_SOURCE_TOKEN", "source-secret")
    transport = _RunnerTestTransport(source_security, lose_dispatch_once=True)
    runner = _runner(tmp_path, transport)
    intent = _intent(transport)
    runner.enqueue_intent(intent)

    assert runner.run_once() == 1
    assert runner.get_state(intent.operation_id) is SourceBrokerV2JobRunnerState.PUBLISHED
    assert transport.dispatch_calls == 1
    assert transport.replay_calls >= 2


def test_copied_runner_row_without_exact_stage_record_fails_closed_with_zero_effects(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    source_security: _RunnerSourceAuthoritySecurity,
) -> None:
    monkeypatch.setenv("RQUANT_SOURCE_TOKEN", "source-secret")
    transport = _RunnerTestTransport(source_security)
    original = _runner(tmp_path / "original", transport, owner_id="original-owner")
    intent = _intent(transport)
    original.enqueue_intent(intent)
    copied_path = tmp_path / "copied" / "runner.sqlite3"
    copied_path.parent.mkdir(parents=True)
    shutil.copyfile(tmp_path / "original" / "runner.sqlite3", copied_path)

    assert original.run_once() == 1
    dispatch_effects = transport.dispatch_calls
    finalize_effects = transport.finalize_calls

    copied = _runner(tmp_path / "copied", transport, owner_id="copy-owner")
    assert copied.run_once() == 0
    assert copied.get_state(intent.operation_id) is SourceBrokerV2JobRunnerState.RECONCILE_REQUIRED
    assert transport.dispatch_calls == dispatch_effects
    assert transport.finalize_calls == finalize_effects


def test_runner_rejects_persisted_row_after_stage_root_replacement_without_effects(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    source_security: _RunnerSourceAuthoritySecurity,
) -> None:
    monkeypatch.setenv("RQUANT_SOURCE_TOKEN", "source-secret")
    transport = _RunnerTestTransport(source_security)
    runner = _runner(tmp_path, transport)
    intent = _intent(transport)
    runner.enqueue_intent(intent)
    stage_store = _STAGE_STORES_BY_TRANSPORT[id(transport)]
    for candidate in (
        stage_store.path,
        Path(f"{stage_store.path}-wal"),
        Path(f"{stage_store.path}-shm"),
    ):
        candidate.unlink(missing_ok=True)
    LabSourceStageStore(
        stage_store.path,
        queue_store_path=tmp_path / "runner.sqlite3",
        manifest_keyring=authorities().authorization_keyring,
        authorization_keyring=authorities().authorization_keyring,
    )

    assert runner.run_once() == 0
    assert runner.get_state(intent.operation_id) is SourceBrokerV2JobRunnerState.RECONCILE_REQUIRED
    assert transport.claim_once_calls == 0
    assert transport.replay_calls == 0
    assert transport.dispatch_calls == 0
    assert transport.finalize_calls == 0


def test_external_unknown_fails_closed_before_dispatch(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    source_security: _RunnerSourceAuthoritySecurity,
) -> None:
    monkeypatch.setenv("RQUANT_SOURCE_TOKEN", "source-secret")
    transport = _RunnerTestTransport(source_security)
    runner = _runner(tmp_path, transport, unknown_replay=True)
    intent = _intent(transport)
    runner.enqueue_intent(intent)

    assert runner.run_once() == 1
    assert runner.get_state(intent.operation_id) is SourceBrokerV2JobRunnerState.RECONCILE_REQUIRED
    assert transport.dispatch_calls == 0
    assert not runner._leases  # noqa: SLF001
    with sqlite3.connect(tmp_path / "runner.sqlite3") as connection:
        reason = str(
            connection.execute(
                "SELECT terminal_reason FROM source_broker_v2_jobs WHERE operation_id = ?",
                (intent.operation_id,),
            ).fetchone()[0]
        )
    assert json.loads(reason) == {
        "code": "external_reconcile_required",
        "exception_class": "SourceBrokerV2RunnerError",
        "operation_hash": intent.operation_hash,
        "phase": "process_new",
    }
    assert "UNKNOWN" not in reason
    assert "http" not in reason
    assert "request" not in reason


def test_forged_prerequisite_signature_fails_before_source_effect(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    source_security: _RunnerSourceAuthoritySecurity,
) -> None:
    monkeypatch.setenv("RQUANT_SOURCE_TOKEN", "source-secret")
    transport = _RunnerTestTransport(source_security)
    runner = _runner(tmp_path, transport, forged_kind="quota")
    intent = _intent(transport)
    runner.enqueue_intent(intent)

    assert runner.run_once() == 1
    assert runner.get_state(intent.operation_id) is SourceBrokerV2JobRunnerState.RECONCILE_REQUIRED
    assert transport.dispatch_calls == 0


def test_forged_source_native_signature_fails_before_source_effect(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    source_security: _RunnerSourceAuthoritySecurity,
) -> None:
    monkeypatch.setenv("RQUANT_SOURCE_TOKEN", "source-secret")
    transport = _RunnerTestTransport(source_security)
    runner = _runner(tmp_path, transport, forged_kind="source")
    intent = _intent(transport)
    runner.enqueue_intent(intent)

    assert runner.run_once() == 1
    assert runner.get_state(intent.operation_id) is SourceBrokerV2JobRunnerState.RECONCILE_REQUIRED
    assert transport.dispatch_calls == 0


def test_total_monotonic_deadline_40ms_rejects_late_acceptance(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    source_security: _RunnerSourceAuthoritySecurity,
) -> None:
    monkeypatch.setenv("RQUANT_SOURCE_TOKEN", "source-secret")
    transport = _RunnerTestTransport(source_security)
    _use_fast_source_signatures(monkeypatch, transport)
    # The acceptance is late by construction, not by arithmetic: the provider
    # takes half again the whole budget to answer the claim, so `budget.call`
    # finds the deadline gone the moment it returns - at any host speed, and
    # whatever the runner's own work costs. The previous form put a 30ms sleep
    # and ~50ms of runner work against a 40ms budget and let the sum decide,
    # which is a race the host can win from either side once the budget scales.
    runner = _runner(
        tmp_path,
        transport,
        total_deadline_seconds=0.04,
        lease_slack_seconds=_DEADLINE_CASE_LEASE_SLACK_SECONDS,
        delay_seconds=0.06,
    )
    intent = _intent(transport)
    runner.enqueue_intent(intent)

    assert runner.run_once() == 1
    assert runner.get_state(intent.operation_id) is SourceBrokerV2JobRunnerState.RECONCILE_REQUIRED
    assert transport.dispatch_calls == 0


def test_total_monotonic_deadline_allows_fast_native_chain(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    source_security: _RunnerSourceAuthoritySecurity,
) -> None:
    monkeypatch.setenv("RQUANT_SOURCE_TOKEN", "source-secret")
    transport = _RunnerTestTransport(source_security)
    _use_fast_source_signatures(monkeypatch, transport)
    # A budget that dominates the host's worst stall rather than tracking the
    # chain's cost, and a lease that cannot fence the terminal write ahead of the
    # budget: if this case fails now it is because the chain did not fit, not
    # because the shard stalled somewhere inside it.
    runner = _runner(
        tmp_path,
        transport,
        total_deadline_seconds=_FAST_CHAIN_DEADLINE_SECONDS,
        lease_slack_seconds=_DEADLINE_CASE_LEASE_SLACK_SECONDS,
        delay_seconds=0.002,
    )
    intent = _intent(transport)
    runner.enqueue_intent(intent)

    assert runner.run_once() == 1
    assert runner.get_state(intent.operation_id) is SourceBrokerV2JobRunnerState.PUBLISHED
    observed_deadlines = [deadline for deadline in transport.deadlines if deadline is not None]
    assert observed_deadlines
    assert len(set(observed_deadlines)) == 1
    assert not runner._leases  # noqa: SLF001


def test_concurrent_owners_produce_one_physical_dispatch(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    source_security: _RunnerSourceAuthoritySecurity,
) -> None:
    monkeypatch.setenv("RQUANT_SOURCE_TOKEN", "source-secret")
    transport = _RunnerTestTransport(source_security)
    first = _runner(tmp_path, transport, owner_id="owner-a")
    second = _runner(tmp_path, transport, owner_id="owner-b")
    intent = _intent(transport)
    first.enqueue_intent(intent)

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = tuple(pool.map(lambda runner: runner.run_once(), (first, second)))

    assert sum(results) == 1
    assert transport.dispatch_calls == 1
    assert not first._leases  # noqa: SLF001
    assert not second._leases  # noqa: SLF001

    _use_fast_source_signatures(monkeypatch, transport)
    for index in range(40):
        next_intent = _intent(transport, f"{index + 2:06d}.SZ")
        first.enqueue_intent(next_intent)
        assert first.run_once() == 1
        assert first.get_state(next_intent.operation_id) is SourceBrokerV2JobRunnerState.PUBLISHED
        assert not first._leases  # noqa: SLF001


def test_lease_constraint_and_stale_owner_fencing(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    source_security: _RunnerSourceAuthoritySecurity,
) -> None:
    monkeypatch.setenv("RQUANT_SOURCE_TOKEN", "source-secret")
    with pytest.raises(ValueError, match="lease"):
        SourceBrokerV2JobRunnerConfig(
            owner_id="owner-a",
            lease_seconds=0.1,
            total_deadline_seconds=0.08,
            takeover_grace_seconds=0.05,
        )

    transport = _RunnerTestTransport(source_security)
    runner = _runner(tmp_path, transport)
    intent = _intent(transport)
    runner.enqueue_intent(intent)
    assert runner.claim_pending() == (intent.operation_id,)
    with sqlite3.connect(tmp_path / "runner.sqlite3") as connection:
        connection.execute(
            "UPDATE source_broker_v2_jobs SET lease_generation = lease_generation + 1 "
            "WHERE operation_id = ?",
            (intent.operation_id,),
        )
    with pytest.raises(SourceBrokerV2RunnerFencedError):
        runner.mark_dispatching_for_recovery_test(intent.operation_id)
    assert intent.operation_id not in runner._leases  # noqa: SLF001


def test_backpressure_wal_full_busy_timeout_and_clean_stop(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    source_security: _RunnerSourceAuthoritySecurity,
) -> None:
    monkeypatch.setenv("RQUANT_SOURCE_TOKEN", "source-secret")
    transport = _RunnerTestTransport(source_security)
    runner = _runner(tmp_path, transport, max_inbox=1)
    original_checkpoint = runner._checkpoint  # noqa: SLF001
    checkpoint_calls = 0

    def checkpoint_spy() -> None:
        nonlocal checkpoint_calls
        checkpoint_calls += 1
        original_checkpoint()

    monkeypatch.setattr(runner, "_checkpoint", checkpoint_spy)
    intent = _intent(transport)
    runner.enqueue_intent(intent)
    assert checkpoint_calls == 0
    runner.checkpoint()
    assert checkpoint_calls == 1
    with pytest.raises(SourceBrokerV2RunnerBackpressureError):
        runner.enqueue_intent(_intent(transport, "000002.SZ"))
    assert runner.sqlite_pragmas() == {
        "busy_timeout": 2_000,
        "journal_mode": "wal",
        "synchronous": 2,
    }

    with _serving_runner(runner, poll_interval_seconds=0.01) as worker:
        deadline = time.monotonic() + 5
        while runner.get_state(intent.operation_id) is not SourceBrokerV2JobRunnerState.PUBLISHED:
            assert time.monotonic() < deadline
            runner.wake()
            time.sleep(0.005)
    assert not worker.is_alive()
    assert runner.closed
    assert not runner._leases  # noqa: SLF001

    bulk_transport = _RunnerTestTransport(source_security)
    bulk = _runner(
        tmp_path / "bulk",
        bulk_transport,
        max_inbox=100,
        max_batch=7,
    )
    bulk_intents = tuple(_intent(bulk_transport, f"{index + 100:06d}.SZ") for index in range(30))
    for bulk_intent in bulk_intents:
        bulk.enqueue_intent(bulk_intent)
    expired = "2000-01-01T00:00:00.000000+00:00"
    with sqlite3.connect(tmp_path / "bulk" / "runner.sqlite3") as connection:
        for index, bulk_intent in enumerate(bulk_intents):
            connection.execute(
                """
                UPDATE source_broker_v2_jobs
                SET state = ?, owner_id = 'dead-owner', lease_generation = 1,
                    lease_expires_at = ?, heartbeat_at = ?
                WHERE operation_id = ?
                """,
                (
                    "CLAIMED" if index < 15 else "DISPATCHING",
                    expired,
                    expired,
                    bulk_intent.operation_id,
                ),
            )
    batches: list[int] = []
    while recovered := bulk.recover_expired_once():
        batches.append(recovered)
    assert batches == [7, 7, 7, 7, 2]
    with sqlite3.connect(tmp_path / "bulk" / "runner.sqlite3") as connection:
        query_plan = tuple(
            str(row[3])
            for row in connection.execute(
                """
                EXPLAIN QUERY PLAN
                SELECT operation_id, operation_hash, state, lease_generation
                FROM source_broker_v2_jobs
                    INDEXED BY source_broker_v2_jobs_expiry_ordered
                WHERE state IN ('CLAIMED', 'DISPATCHING') AND lease_expires_at <= ?
                ORDER BY lease_expires_at, state, operation_id
                LIMIT ?
                """,
                (expired, 7),
            )
        )
        states = dict(
            connection.execute(
                "SELECT state, COUNT(*) FROM source_broker_v2_jobs GROUP BY state"
            ).fetchall()
        )
        indexes = {
            str(row[1]) for row in connection.execute("PRAGMA index_list(source_broker_v2_jobs)")
        }
        expired_reasons = tuple(
            str(row[0])
            for row in connection.execute(
                "SELECT terminal_reason FROM source_broker_v2_jobs "
                "WHERE state = 'RECONCILE_REQUIRED'"
            )
        )
    assert states == {"NEW": 15, "RECONCILE_REQUIRED": 15}
    assert "source_broker_v2_jobs_expiry_ordered" in indexes
    assert any("source_broker_v2_jobs_expiry_ordered" in detail for detail in query_plan)
    assert all("TEMP B-TREE" not in detail for detail in query_plan)
    assert all(json.loads(reason)["code"] == "lease_expired" for reason in expired_reasons)

    cleanup_runner = _runner(
        tmp_path / "cleanup",
        _RunnerTestTransport(source_security),
    )
    with (
        pytest.raises(RuntimeError, match="cleanup probe"),
        _serving_runner(cleanup_runner, poll_interval_seconds=0.01) as cleanup_worker,
    ):
        raise RuntimeError("cleanup probe")
    assert not cleanup_worker.is_alive()
    assert cleanup_runner.closed


def test_runner_store_owns_capacity_across_queue_clients_and_restarts(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    source_security: _RunnerSourceAuthoritySecurity,
) -> None:
    monkeypatch.setenv("RQUANT_SOURCE_TOKEN", "source-secret")
    transport = _RunnerTestTransport(source_security)
    _runner(tmp_path, transport, owner_id="owner-a", max_inbox=1)
    queue = SourceBrokerV2SchedulerQueue(
        tmp_path / "runner.sqlite3",
        manifest_keyring=authorities().authorization_keyring,
        authorization_keyring=authorities().authorization_keyring,
        stage_store=_STAGE_STORES_BY_TRANSPORT[id(transport)],
    )
    queue.enqueue_intent(_intent(transport, "000001.SZ"))

    with pytest.raises(SourceBrokerV2SchedulerQueueBackpressureError):
        queue.enqueue_intent(_intent(transport, "000002.SZ"))

    _runner(tmp_path, transport, owner_id="owner-b", max_inbox=1)
    with pytest.raises(SourceBrokerV2RunnerError, match="max_inbox"):
        _runner(tmp_path, transport, owner_id="owner-c", max_inbox=2)


def test_runner_store_first_initialization_is_spawn_safe_for_50_rounds(
    tmp_path: Path,
) -> None:
    context = multiprocessing.get_context("spawn")
    worker_count = 8

    for round_index in range(50):
        round_root = tmp_path / f"round-{round_index:02d}"
        round_root.mkdir()
        db_path = round_root / "runner.sqlite3"
        processes = tuple(
            context.Process(
                target=_spawn_initialize_runner_store,
                args=(str(db_path), str(round_root), worker_id, worker_count),
            )
            for worker_id in range(worker_count)
        )
        try:
            for process in processes:
                process.start()
            deadline = time.monotonic() + 30
            for process in processes:
                process.join(timeout=max(0.0, deadline - time.monotonic()))
            assert all(not process.is_alive() for process in processes), round_index
            assert [process.exitcode for process in processes] == [0] * worker_count
            assert [
                (round_root / f"result-{worker_id}").read_text(encoding="utf-8")
                for worker_id in range(worker_count)
            ] == ["ok"] * worker_count
            with sqlite3.connect(db_path) as connection:
                connection.row_factory = sqlite3.Row
                config = runner_module.load_source_broker_v2_job_store_config(connection)
                assert connection.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
                assert connection.execute("PRAGMA journal_mode").fetchone()[0] == "wal"
            assert config.max_inbox == 8
        finally:
            for process in processes:
                if process.is_alive():
                    process.terminate()
                    process.join(timeout=5)
                process.close()


def test_queue_verifies_runner_atomic_published_commit_before_returning_outcome(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    source_security: _RunnerSourceAuthoritySecurity,
) -> None:
    monkeypatch.setenv("RQUANT_SOURCE_TOKEN", "source-secret")
    transport = _RunnerTestTransport(source_security)
    runner = _runner(tmp_path, transport)
    intent = _intent(transport)
    runner.enqueue_intent(intent)
    assert runner.run_once() == 1
    expected = runner.get_outcome(intent.operation_id)
    queue = SourceBrokerV2SchedulerQueue(
        tmp_path / "runner.sqlite3",
        manifest_keyring=authorities().authorization_keyring,
        authorization_keyring=authorities().authorization_keyring,
        stage_store=_STAGE_STORES_BY_TRANSPORT[id(transport)],
    )
    assert queue.get_verified_published_outcome(intent.operation_id) == expected

    with sqlite3.connect(tmp_path / "runner.sqlite3") as connection:
        trigger = connection.execute(
            "SELECT sql FROM sqlite_master WHERE type = 'trigger' "
            "AND name = 'source_broker_v2_published_immutable'"
        ).fetchone()
        if trigger is not None:
            connection.execute("DROP TRIGGER source_broker_v2_published_immutable")
        connection.execute(
            "UPDATE source_broker_v2_jobs SET dispatch_receipt = ? WHERE operation_id = ?",
            (b"{}", intent.operation_id),
        )
        if trigger is not None:
            connection.execute(str(trigger[0]))

    with pytest.raises(SourceBrokerV2SchedulerQueueIntegrityError):
        queue.get_verified_published_outcome(intent.operation_id)
