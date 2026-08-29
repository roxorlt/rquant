from __future__ import annotations

import base64
import os
import shutil
import socket
import sqlite3
import subprocess
import tempfile
import threading
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Literal

import pytest

from rquant.runtime_contracts import canonical_sha256
from rquant.source_broker_protocol import (
    MAX_SOURCE_BROKER_FRAME_BYTES,
    PeerCredentialsPolicy,
    ServerCredentialsPolicy,
    SocketEndpointIdentity,
    SocketEndpointPolicy,
    SourceBrokerTransportError,
)
from rquant.source_broker_v2 import (
    SOURCE_BROKER_V2_AUTHORITY_PURPOSE,
    SourceAuthorityKeyring,
    SourceBrokerV2ClaimOnceRequest,
    SourceBrokerV2ClaimOnceResponse,
    SourceBrokerV2ClaimStatus,
    SourceBrokerV2DispatchEnvelope,
    SourceBrokerV2DispatchOutcome,
    SourceBrokerV2DispatchRequest,
    SourceBrokerV2DispatchResponse,
    SourceBrokerV2FinalizeEnvelope,
    SourceBrokerV2FinalizeRequest,
    SourceBrokerV2FinalizeResponse,
    SourceBrokerV2OutboxPhase,
    SourceBrokerV2ReplayRequest,
    SourceBrokerV2ReplayResponse,
    SourceBrokerV2ReplayStatus,
    SourceBrokerV2SagaIntegrityError,
    SourceBrokerV2TransportDeadlineError,
    SourceBrokerV2WireRequest,
    source_authority_signature_payload,
    source_claim_attempt_id,
    source_effect_operation_id,
)
from rquant.strict_json import (
    canonical_json_bytes,
    canonical_model_json_bytes,
    strict_canonical_json_loads,
)

HASHES = {
    "attempt": "1" * 64,
    "claim_plan": "2" * 64,
    "binding": "3" * 64,
    "manifest": "4" * 64,
    "owner": "5" * 64,
    "challenge": "6" * 64,
    "final_challenge": "7" * 64,
}
NOW = datetime(2026, 8, 9, 4, tzinfo=UTC)


# A watchdog for "the provider thread got in at all", not a subject: the
# bounded rejections these cases assert afterwards are what is under test. One
# or two seconds is a developer machine's budget and the x64 runner missed it.
#
# Only the two cases that hand the whole dispatch to a helper thread with no
# deadline may use it, because only there is provider entry *guaranteed*: with
# no deadline there is no checkpoint that can refuse ahead of the provider, so
# the wait either observes the entry or the service hung. A case whose dispatch
# carries a deadline must not wait on entry - if a prologue checkpoint refuses
# first the provider thread is never spawned, the event is never set, and the
# wait can only burn its full timeout and then report `assert False`, which
# names neither the stage that refused nor the reason. Those cases assert
# `entered.is_set()` instead; see `_BLOCKED_PROVIDER_DEADLINE_SECONDS`.
_PROVIDER_ENTRY_WATCHDOG_SECONDS = 30

# Every deadline in this file that has to expire *while the provider is
# running* must first survive the dispatch prologue: envelope parsing, the
# Ed25519 verification of the claim receipt, and the four or five
# `synchronous = FULL` ledger transactions that precede
# `_require_deadline(stage="before provider dispatch")`.
#
# Sized against a measurement rather than a literal, because the literals were
# wrong in a way isolation could not show. A CI diagnostic job (017d808)
# timed that prologue at 11ms on an idle ubuntu-24.04 x64 runner - the same as
# this laptop - and running these three cases alone on that runner is green.
# Inside a full shard, after ~2500 other cases have run, the same prologue
# overruns the 50ms deadline outright: three cases failed with
# `dispatch_calls == 0`, which is "the provider was never reached", not
# "the wait was too short". A larger literal would only move that cliff, so the
# prologue is measured once per module - in the same process, at the point in
# the shard where these cases actually run, so the measurement carries the
# degradation with it - and the budgets are read as multiples of it.
#
# The scale is floored at 1.0, so on an unloaded host every budget below is
# exactly the literal it has always been. Deadlines, the provider sleeps they
# must fire inside, and the elapsed guards all take the same scale: the
# orderings between them (prologue < deadline < prologue + provider sleep) hold
# at every scale precisely because none of the three is scaled alone.
_PROLOGUE_REFERENCE_SECONDS = 0.0125
# Two cases starve the authority signing step of deadline and require the
# service to refuse *at that step*. Three quantities decide whether they do,
# and they only work as a set:
#
#   prologue  <  deadline  <  signing cost
#
# The left inequality keeps the three `_require_deadline` checkpoints ahead of
# signing from firing first - if one of them does, the refusal is real but its
# message is not "authority signing" and the case does not recognise it. The
# right one is what starves the signer. Scaling the deadline alone broke the
# right inequality (the signing finished inside the enlarged deadline: DID NOT
# RAISE); pinning the deadline to a literal while the prologue grows with the
# host breaks the left one instead, and a full shard's prologue sits right on
# top of 30ms. Scaling all three by the same factor keeps both inequalities
# true at every host speed, which is the only form that is not a race.
_SIGNING_STARVED_DEADLINE_SECONDS = 0.03
_SIGNING_COST_SECONDS = 0.1
# No case here needs a window any more, and paying for one is what broke every
# one of them in turn. The shape that kept failing was
# `prologue < deadline < prologue + provider sleep`: a deadline squeezed from
# both sides by two wall clock quantities, racing a third - the prologue - that
# appears in neither. Each of the five cases that had it now gets its upper
# bound from the test instead: the provider blocks until the case releases it,
# so the deadline expires while the provider is still inside no matter how
# large it is. That leaves a single ordering to secure, `prologue < deadline`,
# and a one-sided ordering is secured by margin rather than by a measurement
# taken at module import and then outrun by the shard's own load.
#
# The window form is gone from this file: the 40ms constant the last two cases
# used was deleted with them, because a budget whose comment describes an
# admissible window is a trap for whoever adds the sixth case.
#
# The old form was a 50ms deadline that had to beat the prologue, and inside a
# full shard on a 2 vCPU runner it lost: `dispatch_calls == 0`, the provider
# never reached, so the exactly-once invariant the case exists for was not
# exercised at all - the case failed on the one thing it was not testing. Here
# the prologue is tens of milliseconds and the ledger writes behind it are
# bounded by the 500ms busy timeout, so a budget in seconds is not a race with
# anything. The calibrated scale still applies on top of it.
#
# The two `stop()` / global-gate cases were left on the 50ms literal when the
# first one was converted, and the same cliff took the stop() case out on a
# 3.12 shard (30.197s against a 0.108s norm). Their exposure was measured: the
# 50ms literal leaves the prologue a margin of 4x its own calibrated cost, and
# a sweep that shrinks the scale - the exact equivalent of the host degrading
# *after* the module-scope calibration was taken - puts the cliff at 2.5x to
# 3.3x on both 3.11 and 3.12, non-monotonically, which is a wall clock race and
# not a bound. A 2.5x swing in a ~10ms prologue of SQLite commits, Ed25519
# verification and openssl work, on a 2 vCPU runner 2500 cases into a shard, is
# ordinary. All three now share this budget, and each derives its provider's
# block cap from it rather than from a literal, so enlarging the deadline can
# never collide with the block.
_BLOCKED_PROVIDER_DEADLINE_SECONDS = 1.5
# `_BlockingProvider` blocks until its case releases it, so this is not a budget
# for anything a case measures - it is the guard that turns a hung case into a
# named failure instead of a hung shard. What it has to outlast is the work the
# case does *while holding* the provider: a signed claim plus a signed replay
# round trip in one case, a whole second dispatch in the other. Every bit of
# that scales with the host, and the cap was the one quantity in the pair that
# did not - a bare `timeout=5`, the same shape of defect the blocked-dispatch
# deadlines above were just converted out of. Expressed as a count of measured
# prologues it is 5.0s on a quiet host, exactly what it has always been, and
# 400x whatever prologue this process actually measured everywhere else, so the
# guard and the work it guards move together.
_BLOCKING_PROVIDER_RELEASE_PROLOGUES = 400
# The calibration dispatch is not under test and must not be the thing that
# fails when the host is slow, so it gets a watchdog rather than a budget.
_PROLOGUE_CALIBRATION_DEADLINE_SECONDS = 120.0
_prologue_scale = 1.0


def _host(seconds: float) -> float:
    """Read a budget as wall clock on a quiet host, scaled by a loaded one."""

    return seconds * _prologue_scale


def _openssl() -> str:
    executable = shutil.which("openssl")
    if executable is None:
        pytest.skip("openssl is required for SourceBroker v2 service signing tests")
    return executable


def _keypair(root: Path, key_id: str) -> tuple[Path, bytes]:
    root.mkdir(mode=0o700, parents=True, exist_ok=True)
    private_key = root / f"{key_id}.private.pem"
    public_key = root / f"{key_id}.public.pem"
    if private_key.exists() and public_key.exists():
        return private_key, public_key.read_bytes()
    created = subprocess.run(
        (_openssl(), "genpkey", "-algorithm", "ED25519", "-out", str(private_key)),
        check=False,
        capture_output=True,
    )
    assert created.returncode == 0, created.stderr.decode("utf-8", errors="replace")
    private_key.chmod(0o600)
    exported = subprocess.run(
        (_openssl(), "pkey", "-in", str(private_key), "-pubout", "-out", str(public_key)),
        check=False,
        capture_output=True,
    )
    assert exported.returncode == 0, exported.stderr.decode("utf-8", errors="replace")
    return private_key, public_key.read_bytes()


def _source_payload(symbol: str = "000001.SZ") -> bytes:
    return canonical_json_bytes({"symbol": symbol, "trade_date": "2026-08-07"})


def _hash_payload(payload: bytes) -> str:
    import rquant.strict_json as strict_json

    return canonical_sha256(strict_json.strict_canonical_json_loads(payload))


def _dispatch_request(
    operation_id: str | None = None,
    *,
    saga_id: str = "saga-source-v2",
) -> SourceBrokerV2DispatchRequest:
    payload = _source_payload()
    return SourceBrokerV2DispatchRequest(
        saga_id=saga_id,
        operation_id=operation_id
        or source_effect_operation_id(
            saga_id=saga_id,
            phase=SourceBrokerV2OutboxPhase.DISPATCH,
        ),
        call_id="daily-bars",
        attempt_identity_hash=HASHES["attempt"],
        claim_plan_hash=HASHES["claim_plan"],
        claim_binding_hash=HASHES["binding"],
        manifest_hash=HASHES["manifest"],
        payload=payload,
        claim_payload_hash=_hash_payload(payload),
        dispatch_payload_hash=_hash_payload(payload),
    )


def _claim_once_request(
    dispatch_request: SourceBrokerV2DispatchRequest,
    *,
    challenge: str = HASHES["challenge"],
    phase: SourceBrokerV2OutboxPhase = SourceBrokerV2OutboxPhase.DISPATCH,
    operation_request_hash: str | None = None,
    executor_owner_token_hash: str = HASHES["owner"],
    executor_generation: int = 3,
    max_external_deadline: datetime | None = None,
    not_before_takeover_at: datetime | None = None,
) -> SourceBrokerV2ClaimOnceRequest:
    return SourceBrokerV2ClaimOnceRequest(
        saga_id=dispatch_request.saga_id,
        operation_id=dispatch_request.operation_id,
        phase=phase,
        operation_request_hash=operation_request_hash or dispatch_request.request_hash,
        challenge=challenge,
        claim_binding_hash=dispatch_request.claim_binding_hash,
        claim_generation=11,
        scheduler_fencing_token=29,
        executor_owner_token_hash=executor_owner_token_hash,
        executor_generation=executor_generation,
        max_external_deadline=max_external_deadline or NOW + timedelta(seconds=10),
        not_before_takeover_at=not_before_takeover_at or NOW + timedelta(seconds=20),
    )


def _finalize_request(
    dispatch: SourceBrokerV2DispatchResponse,
    *,
    operation_id: str | None = None,
) -> SourceBrokerV2FinalizeRequest:
    return SourceBrokerV2FinalizeRequest(
        saga_id=dispatch.saga_id,
        operation_id=operation_id
        or source_effect_operation_id(
            saga_id=dispatch.saga_id,
            phase=SourceBrokerV2OutboxPhase.SOURCE_FINALIZE,
        ),
        dispatch_evidence_hash=dispatch.evidence_hash,
        claim_binding_hash=HASHES["binding"],
    )


class _CountingProvider:
    def __init__(
        self,
        *,
        fail_dispatch_once: bool = False,
        fail_finalize_once: bool = False,
        sleep_dispatch_seconds: float = 0.0,
        sleep_finalize_seconds: float = 0.0,
    ) -> None:
        self.dispatch_calls = 0
        self.finalize_calls = 0
        self.fail_dispatch_once = fail_dispatch_once
        self.fail_finalize_once = fail_finalize_once
        self.sleep_dispatch_seconds = sleep_dispatch_seconds
        self.sleep_finalize_seconds = sleep_finalize_seconds

    def dispatch(self, request: SourceBrokerV2DispatchRequest):
        from rquant.source_broker_v2_service import SourceBrokerV2ProviderDispatchResult

        self.dispatch_calls += 1
        if self.sleep_dispatch_seconds:
            time.sleep(self.sleep_dispatch_seconds)
        if self.fail_dispatch_once:
            self.fail_dispatch_once = False
            raise RuntimeError("provider lost terminal response")
        response = canonical_json_bytes(
            {"call": self.dispatch_calls, "operation_id": request.operation_id}
        )
        receipt = canonical_json_bytes({"provider": "unit", "request_hash": request.request_hash})
        return SourceBrokerV2ProviderDispatchResult(
            outcome=SourceBrokerV2DispatchOutcome.SUCCESS,
            response=response,
            transport_receipt=receipt,
        )

    def finalize(self, request: SourceBrokerV2FinalizeRequest):
        from rquant.source_broker_v2_service import SourceBrokerV2ProviderFinalizeResult

        self.finalize_calls += 1
        if self.sleep_finalize_seconds:
            time.sleep(self.sleep_finalize_seconds)
        if self.fail_finalize_once:
            self.fail_finalize_once = False
            raise RuntimeError("provider lost final receipt")
        return SourceBrokerV2ProviderFinalizeResult(
            final_receipt=canonical_json_bytes(
                {"finalize": self.finalize_calls, "request_hash": request.request_hash}
            )
        )


class _ExternalAuthoritySignatureError(SourceBrokerTransportError):
    pass


class _ExternalAuthorityConflictError(SourceBrokerTransportError):
    pass


class _FakeExternalDispatchAuthority:
    """Non-production, process-external authority model with a strict bytes boundary."""

    def __init__(self) -> None:
        self.available = True
        self.force_old_absent = False
        self.drop_complete_response_once = False
        self.reserve_calls = 0
        self.lookup_calls = 0
        self.complete_calls = 0
        self.failures: dict[str, BaseException] = {}
        self._generation = 0
        self._records: dict[str, dict[str, object]] = {}
        self._lock = threading.Lock()

    def reserve(self, payload: bytes, *, deadline: float | None = None) -> bytes:
        assert type(payload) is bytes
        if deadline is not None:
            assert deadline > time.monotonic()
        with self._lock:
            self.reserve_calls += 1
            self._raise_configured_failure("reserve")
            if not self.available:
                raise SourceBrokerTransportError("external dispatch authority unavailable")
            request = strict_canonical_json_loads(payload)
            assert isinstance(request, dict)
            operation_id = str(request["operation_id"])
            binding = canonical_sha256(request)
            record = self._records.get(operation_id)
            if record is None:
                self._generation += 1
                record = {
                    "binding": binding,
                    "authority_generation": self._generation,
                    "authority_fence": canonical_sha256(
                        {"generation": self._generation, "operation_id": operation_id}
                    ),
                    "result_json": None,
                    "result_hash": None,
                }
                self._records[operation_id] = record
                status = "absent"
            else:
                assert record["binding"] == binding
                if self.force_old_absent:
                    status = "absent"
                elif record["result_json"] is None:
                    status = "unknown"
                else:
                    status = "found"
            result_json = record["result_json"] if status == "found" else None
            result_hash = record["result_hash"] if status == "found" else None
            return canonical_json_bytes(
                {
                    "operation": "reserve",
                    "authority_fence": record["authority_fence"],
                    "authority_generation": record["authority_generation"],
                    "operation_id": operation_id,
                    "request_binding_hash": binding,
                    "result_hash": result_hash,
                    "result_json": result_json,
                    "schema_version": 1,
                    "status": status,
                }
            )

    def lookup(self, payload: bytes, *, deadline: float | None = None) -> bytes:
        assert type(payload) is bytes
        if deadline is not None:
            assert deadline > time.monotonic()
        with self._lock:
            self.lookup_calls += 1
            self._raise_configured_failure("lookup")
            if not self.available:
                raise SourceBrokerTransportError("external dispatch authority unavailable")
            request = strict_canonical_json_loads(payload)
            assert isinstance(request, dict)
            operation_id = str(request["operation_id"])
            binding = canonical_sha256(request)
            record = self._records.get(operation_id)
            if record is None:
                generation = self._generation + 1
                fence = canonical_sha256(
                    {"generation": generation, "lookup": True, "operation_id": operation_id}
                )
                status = "absent"
                result_json = None
                result_hash = None
            else:
                assert record["binding"] == binding
                generation = int(record["authority_generation"])
                fence = str(record["authority_fence"])
                if self.force_old_absent:
                    status = "absent"
                else:
                    status = "found" if record["result_json"] is not None else "unknown"
                result_json = record["result_json"] if status == "found" else None
                result_hash = record["result_hash"] if status == "found" else None
            return canonical_json_bytes(
                {
                    "operation": "lookup",
                    "authority_fence": fence,
                    "authority_generation": generation,
                    "operation_id": operation_id,
                    "request_binding_hash": binding,
                    "result_hash": result_hash,
                    "result_json": result_json,
                    "schema_version": 1,
                    "status": status,
                }
            )

    def complete(self, payload: bytes, *, deadline: float | None = None) -> bytes:
        assert type(payload) is bytes
        if deadline is not None:
            assert deadline > time.monotonic()
        with self._lock:
            self.complete_calls += 1
            self._raise_configured_failure("complete")
            if not self.available:
                raise SourceBrokerTransportError("external dispatch authority unavailable")
            request = strict_canonical_json_loads(payload)
            assert isinstance(request, dict)
            operation_id = str(request["operation_id"])
            record = self._records[operation_id]
            assert record["authority_generation"] == request["authority_generation"]
            assert record["authority_fence"] == request["authority_fence"]
            assert record["binding"] == request["request_binding_hash"]
            result_json = request["result_json"]
            result_hash = request["result_hash"]
            if record["result_json"] is not None:
                assert record["result_json"] == result_json
                assert record["result_hash"] == result_hash
            record["result_json"] = result_json
            record["result_hash"] = result_hash
            response = canonical_json_bytes(
                {
                    "operation": "complete",
                    "authority_fence": record["authority_fence"],
                    "authority_generation": record["authority_generation"],
                    "operation_id": operation_id,
                    "request_binding_hash": record["binding"],
                    "result_hash": record["result_hash"],
                    "result_json": record["result_json"],
                    "schema_version": 1,
                    "status": "found",
                }
            )
            if self.drop_complete_response_once:
                self.drop_complete_response_once = False
                raise SourceBrokerTransportError("external complete response was lost")
            return response

    def _raise_configured_failure(self, operation: str) -> None:
        error = self.failures.pop(operation, None)
        if error is not None:
            raise error

    def forget(self, operation_id: str) -> None:
        with self._lock:
            self._records.pop(operation_id, None)


class _ScriptedUnixSocket:
    def __init__(self, response_factory) -> None:
        self.response_factory = response_factory
        self.sent = bytearray()
        self.response = bytearray()
        self.timeouts: list[float | None] = []
        self.connected_path: str | None = None

    def __enter__(self):
        return self

    def __exit__(self, _exc_type, _exc, _traceback) -> None:
        self.close()

    def settimeout(self, value: float | None) -> None:
        self.timeouts.append(value)

    def connect(self, path: str) -> None:
        self.connected_path = path

    def send(self, payload) -> int:
        raw = bytes(payload)
        self.sent.extend(raw)
        return len(raw)

    def recv(self, size: int) -> bytes:
        if not self.response:
            request_size = int.from_bytes(self.sent[:4], "big")
            request = bytes(self.sent[4 : 4 + request_size])
            raw_response = self.response_factory(request)
            self.response.extend(len(raw_response).to_bytes(4, "big") + raw_response)
        chunk = bytes(self.response[:size])
        del self.response[:size]
        return chunk

    def close(self) -> None:
        return None


def _external_reserve_request(operation_id: str = "c" * 64) -> bytes:
    from rquant.source_broker_v2_service import ExternalDispatchReserveRequest

    return canonical_model_json_bytes(
        ExternalDispatchReserveRequest(
            operation_id=operation_id,
            saga_id="saga-external-authority",
            phase=SourceBrokerV2OutboxPhase.DISPATCH,
            operation_request_hash="d" * 64,
            claim_binding_hash="e" * 64,
            claim_generation=7,
            scheduler_fencing_token=11,
            executor_owner_token_hash="f" * 64,
            executor_generation=13,
            max_external_deadline=NOW + timedelta(seconds=10),
            not_before_takeover_at=NOW + timedelta(seconds=20),
        )
    )


def _sign_external_response(private_key: Path, signing_bytes: bytes) -> str:
    with tempfile.TemporaryDirectory(prefix="rquant-external-sign-test-") as directory_name:
        root = Path(directory_name)
        payload_path = root / "payload.bin"
        signature_path = root / "signature.bin"
        payload_path.write_bytes(source_authority_signature_payload(signing_bytes))
        completed = subprocess.run(
            (
                _openssl(),
                "pkeyutl",
                "-sign",
                "-inkey",
                str(private_key),
                "-rawin",
                "-in",
                str(payload_path),
                "-out",
                str(signature_path),
            ),
            check=False,
            capture_output=True,
        )
        assert completed.returncode == 0, completed.stderr.decode("utf-8", errors="replace")
        return base64.b64encode(signature_path.read_bytes()).decode("ascii")


def _external_signed_response(
    raw_wire_request: bytes,
    *,
    private_key: Path,
    key_id: str,
    challenge: str | None = None,
) -> bytes:
    from rquant.source_broker_v2_service import (
        ExternalDispatchAuthorityResponse,
        ExternalDispatchAuthoritySignedResponse,
        ExternalDispatchAuthorityWireRequest,
    )

    wire = ExternalDispatchAuthorityWireRequest.model_validate_json(raw_wire_request)
    request = strict_canonical_json_loads(wire.payload)
    assert isinstance(request, dict)
    result = canonical_model_json_bytes(
        ExternalDispatchAuthorityResponse(
            operation=wire.operation,
            status="absent",
            operation_id=str(request["operation_id"]),
            request_binding_hash=canonical_sha256(request),
            authority_generation=17,
            authority_fence="9" * 64,
        )
    )
    response = ExternalDispatchAuthoritySignedResponse(
        operation=wire.operation,
        challenge=challenge or wire.challenge,
        request_hash=wire.request_hash,
        authority_id="external-authority",
        key_id=key_id,
        result=result,
        result_hash=_hash_payload(result),
        signature="unsigned",
    )
    signature = _sign_external_response(private_key, response.signing_bytes())
    return canonical_model_json_bytes(response.model_copy(update={"signature": signature}))


class _BlockingProvider(_CountingProvider):
    def __init__(self) -> None:
        super().__init__()
        self.entered = threading.Event()
        self.release = threading.Event()
        self.block_seconds = _host(
            _PROLOGUE_REFERENCE_SECONDS * _BLOCKING_PROVIDER_RELEASE_PROLOGUES
        )

    def dispatch(self, request: SourceBrokerV2DispatchRequest):
        from rquant.source_broker_v2_service import SourceBrokerV2ProviderDispatchResult

        self.dispatch_calls += 1
        self.entered.set()
        if not self.release.wait(timeout=self.block_seconds):
            raise TimeoutError("provider dispatch test release was not signaled")
        response = canonical_json_bytes(
            {"call": self.dispatch_calls, "operation_id": request.operation_id}
        )
        receipt = canonical_json_bytes({"provider": "unit", "request_hash": request.request_hash})
        return SourceBrokerV2ProviderDispatchResult(
            outcome=SourceBrokerV2DispatchOutcome.SUCCESS,
            response=response,
            transport_receipt=receipt,
        )


class _ShortBlockingProvider(_CountingProvider):
    def __init__(self, *, block_seconds: float) -> None:
        super().__init__()
        self.block_seconds = block_seconds
        self.entered = threading.Event()
        self.release = threading.Event()

    def dispatch(self, request: SourceBrokerV2DispatchRequest):
        from rquant.source_broker_v2_service import SourceBrokerV2ProviderDispatchResult

        self.dispatch_calls += 1
        self.entered.set()
        if not self.release.wait(timeout=self.block_seconds):
            raise TimeoutError("provider dispatch remained blocked")
        response = canonical_json_bytes(
            {"call": self.dispatch_calls, "operation_id": request.operation_id}
        )
        receipt = canonical_json_bytes({"provider": "unit", "request_hash": request.request_hash})
        return SourceBrokerV2ProviderDispatchResult(
            outcome=SourceBrokerV2DispatchOutcome.SUCCESS,
            response=response,
            transport_receipt=receipt,
        )


class _ShortBlockingFinalizeProvider(_CountingProvider):
    """The finalize twin of `_ShortBlockingProvider`.

    Only finalize blocks: the cases that need it have to complete a real
    dispatch first in order to have something to finalize at all.
    """

    def __init__(self, *, block_seconds: float) -> None:
        super().__init__()
        self.block_seconds = block_seconds
        self.entered = threading.Event()
        self.release = threading.Event()

    def finalize(self, request: SourceBrokerV2FinalizeRequest):
        from rquant.source_broker_v2_service import SourceBrokerV2ProviderFinalizeResult

        self.finalize_calls += 1
        self.entered.set()
        if not self.release.wait(timeout=self.block_seconds):
            raise TimeoutError("provider finalize remained blocked")
        return SourceBrokerV2ProviderFinalizeResult(
            final_receipt=canonical_json_bytes(
                {"finalize": self.finalize_calls, "request_hash": request.request_hash}
            )
        )


class _SensitiveProviderError(RuntimeError):
    pass


class _SensitiveFailProvider(_CountingProvider):
    def __init__(self, secret: str) -> None:
        super().__init__()
        self.secret = secret

    def dispatch(self, request: SourceBrokerV2DispatchRequest):
        self.dispatch_calls += 1
        raise _SensitiveProviderError(
            f"payload={self.secret} token={self.secret} signature={self.secret}"
        )


class _LedgerLockingFailProvider(_CountingProvider):
    def __init__(self, *, ledger_path: Path) -> None:
        super().__init__()
        self.ledger_path = ledger_path
        self.locked = threading.Event()
        self.connection: sqlite3.Connection | None = None

    def dispatch(self, request: SourceBrokerV2DispatchRequest):
        self.dispatch_calls += 1
        self.connection = sqlite3.connect(
            self.ledger_path,
            timeout=0.001,
            isolation_level=None,
            check_same_thread=False,
        )
        self.connection.execute("BEGIN IMMEDIATE")
        self.locked.set()
        raise RuntimeError("provider failed while ledger lock is held")

    def close(self) -> None:
        if self.connection is not None:
            self.connection.close()
            self.connection = None


class _DeadlineAwareTestSigner:
    authority_id = "source-authority"
    key_id = "current"

    def __init__(self, *, required_seconds: float) -> None:
        self.required_seconds = required_seconds
        self.seen_deadlines: list[float | None] = []

    def sign(self, signing_bytes: bytes, *, deadline: float | None = None) -> str:
        assert signing_bytes
        self.seen_deadlines.append(deadline)
        if deadline is None:
            time.sleep(self.required_seconds)
            return base64.b64encode(b"unsigned-test-signature").decode("ascii")
        remaining = deadline - time.monotonic()
        if remaining < self.required_seconds:
            raise SourceBrokerV2TransportDeadlineError(
                "V2 source broker server deadline expired before authority signing"
            )
        time.sleep(self.required_seconds)
        return base64.b64encode(b"unsigned-test-signature").decode("ascii")


def _service(
    tmp_path: Path,
    *,
    provider: _CountingProvider | None = None,
    key_id: str = "current",
    extra_key_id: str | None = None,
    signer: object | None = None,
    busy_timeout_ms: int = 5_000,
    external_authority: object | None = None,
    max_inflight: int = 1,
    event_sink: object | None = None,
    clock: object | None = None,
):
    from rquant.source_broker_v2_service import (
        OpenSslSourceBrokerV2AuthoritySigner,
        SourceBrokerV2ProviderService,
    )

    private_key, public_key = _keypair(tmp_path / f"keys-{key_id}", key_id)
    keys = {key_id: public_key}
    if extra_key_id is not None:
        _extra_private, extra_public = _keypair(tmp_path / f"keys-{extra_key_id}", extra_key_id)
        keys[extra_key_id] = extra_public
    keyring = SourceAuthorityKeyring(
        expected_authority_id="source-authority",
        allowed_public_keys=keys,
        expected_purpose=SOURCE_BROKER_V2_AUTHORITY_PURPOSE,
        expected_schema_version=2,
    )
    authority_signer = signer or OpenSslSourceBrokerV2AuthoritySigner(
        authority_id="source-authority",
        key_id=key_id,
        private_key_path=private_key,
    )
    return (
        SourceBrokerV2ProviderService.create_for_test(
            ledger_path=tmp_path / "source-provider-ledger.sqlite3",
            provider=provider or _CountingProvider(),
            authority_signer=authority_signer,
            authority_keyring=keyring,
            external_dispatch_authority=(
                external_authority
                if external_authority is not None
                else _FakeExternalDispatchAuthority()
            ),
            busy_timeout_ms=busy_timeout_ms,
            clock=clock or (lambda: NOW),
            max_inflight=max_inflight,
            event_sink=event_sink,
            profile="nonproduction",
        ),
        keyring,
    )


def _decode_claim(raw: bytes) -> SourceBrokerV2ClaimOnceResponse:
    return SourceBrokerV2ClaimOnceResponse.model_validate_json(raw)


def _claim(service: object, request: SourceBrokerV2ClaimOnceRequest):
    response = _decode_claim(service.claim_once(canonical_model_json_bytes(request)))
    return response


class _EntryStampProvider(_CountingProvider):
    """Records when the service actually handed the call to the provider."""

    def __init__(self) -> None:
        super().__init__()
        self.dispatch_entered_at: float | None = None
        self.finalize_entered_at: float | None = None

    def dispatch(self, request: SourceBrokerV2DispatchRequest):
        self.dispatch_entered_at = time.monotonic()
        return super().dispatch(request)

    def finalize(self, request: SourceBrokerV2FinalizeRequest):
        self.finalize_entered_at = time.monotonic()
        return super().finalize(request)


@pytest.fixture(scope="module", autouse=True)
def _calibrate_provider_prologue(tmp_path_factory: pytest.TempPathFactory) -> None:
    """Time what a call spends before the provider, once, in this process.

    Both phases are measured and the larger wins: finalize verifies a second
    claim receipt and writes more ledger rows than dispatch, and it is the
    finalize case that went red. The measurement has to happen at module scope
    rather than inside each case - it is a full signed round trip, and the
    cases that need it are holding blocking providers by the time they would
    ask. Taking the slowest of three keeps the scale on the conservative side
    of a host that is still degrading.
    """
    global _prologue_scale

    root = tmp_path_factory.mktemp("provider-prologue-calibration")
    samples: list[float] = []
    for index in range(3):
        provider = _EntryStampProvider()
        service, _keyring = _service(root / f"calibration-{index}", provider=provider)
        request = _dispatch_request()
        claim = _claim(service, _claim_once_request(request))
        envelope = canonical_model_json_bytes(
            SourceBrokerV2DispatchEnvelope(request=request, claim_receipt=claim)
        )
        started = time.monotonic()
        raw = service.dispatch(
            envelope,
            deadline=started + _PROLOGUE_CALIBRATION_DEADLINE_SECONDS,
        )
        entered = provider.dispatch_entered_at
        assert entered is not None, "calibration dispatch never reached the provider"
        samples.append(entered - started)

        dispatch = SourceBrokerV2DispatchResponse.model_validate_json(raw)
        finalize_request = _finalize_request(dispatch)
        finalize_claim = _claim(
            service,
            _claim_once_request(
                _dispatch_request(operation_id=finalize_request.operation_id),
                challenge=HASHES["final_challenge"],
                phase=SourceBrokerV2OutboxPhase.SOURCE_FINALIZE,
                operation_request_hash=finalize_request.request_hash,
            ),
        )
        finalize_envelope = canonical_model_json_bytes(
            SourceBrokerV2FinalizeEnvelope(
                request=finalize_request,
                claim_receipt=finalize_claim,
            )
        )
        started = time.monotonic()
        service.finalize(
            finalize_envelope,
            deadline=started + _PROLOGUE_CALIBRATION_DEADLINE_SECONDS,
        )
        entered = provider.finalize_entered_at
        assert entered is not None, "calibration finalize never reached the provider"
        samples.append(entered - started)
    _prologue_scale = max(1.0, max(samples) / _PROLOGUE_REFERENCE_SECONDS)


def _ledger_row(tmp_path: Path, operation_id: str) -> sqlite3.Row:
    connection = sqlite3.connect(tmp_path / "source-provider-ledger.sqlite3")
    connection.row_factory = sqlite3.Row
    try:
        row = connection.execute(
            "SELECT * FROM source_broker_v2_provider_operation WHERE operation_id = ?",
            (operation_id,),
        ).fetchone()
    finally:
        connection.close()
    assert row is not None
    return row


def _sqlite_backup(source: Path, destination: Path) -> None:
    with (
        sqlite3.connect(source) as source_connection,
        sqlite3.connect(destination) as destination_connection,
    ):
        source_connection.backup(destination_connection)


def test_production_service_fails_closed_without_external_dispatch_authority(
    tmp_path: Path,
) -> None:
    from rquant.source_broker_v2_service import (
        OpenSslSourceBrokerV2AuthoritySigner,
        SourceBrokerV2ProviderService,
    )

    private_key, public_key = _keypair(tmp_path / "keys-required-authority", "current")
    keyring = SourceAuthorityKeyring(
        expected_authority_id="source-authority",
        allowed_public_keys={"current": public_key},
        expected_purpose=SOURCE_BROKER_V2_AUTHORITY_PURPOSE,
        expected_schema_version=2,
    )
    signer = OpenSslSourceBrokerV2AuthoritySigner(
        authority_id="source-authority",
        key_id="current",
        private_key_path=private_key,
    )

    with pytest.raises(ValueError, match="external dispatch authority"):
        SourceBrokerV2ProviderService(
            ledger_path=tmp_path / "closed-ledger.sqlite3",
            provider=_CountingProvider(),
            authority_signer=signer,
            authority_keyring=keyring,
            clock=lambda: NOW,
        )


def test_production_service_rejects_rollbackable_protocol_fake(tmp_path: Path) -> None:
    from rquant.source_broker_v2_service import (
        OpenSslSourceBrokerV2AuthoritySigner,
        SourceBrokerV2ProviderService,
    )

    private_key, public_key = _keypair(tmp_path / "keys-production-exact", "current")
    keyring = SourceAuthorityKeyring(
        expected_authority_id="source-authority",
        allowed_public_keys={"current": public_key},
        expected_purpose=SOURCE_BROKER_V2_AUTHORITY_PURPOSE,
        expected_schema_version=2,
    )
    signer = OpenSslSourceBrokerV2AuthoritySigner(
        authority_id="source-authority",
        key_id="current",
        private_key_path=private_key,
    )
    rollbackable_fake = _FakeExternalDispatchAuthority()

    with pytest.raises(TypeError, match="exact Unix.*authority client"):
        SourceBrokerV2ProviderService(
            ledger_path=tmp_path / "production-ledger.sqlite3",
            provider=_CountingProvider(),
            authority_signer=signer,
            authority_keyring=keyring,
            external_dispatch_authority=rollbackable_fake,
            clock=lambda: NOW,
        )

    with pytest.raises(ValueError, match="nonproduction"):
        SourceBrokerV2ProviderService.create_for_test(
            ledger_path=tmp_path / "test-ledger.sqlite3",
            provider=_CountingProvider(),
            authority_signer=signer,
            authority_keyring=keyring,
            external_dispatch_authority=rollbackable_fake,
            clock=lambda: NOW,
            profile="production",  # type: ignore[arg-type]
        )


def test_simultaneous_local_and_fake_rollback_has_no_production_path(
    tmp_path: Path,
) -> None:
    from rquant.source_broker_v2_service import (
        OpenSslSourceBrokerV2AuthoritySigner,
        SourceBrokerV2ProviderService,
    )

    provider = _CountingProvider()
    rollbackable_fake = _FakeExternalDispatchAuthority()
    service, keyring = _service(
        tmp_path,
        provider=provider,
        external_authority=rollbackable_fake,
    )
    request = _dispatch_request()
    claim = _claim(service, _claim_once_request(request))
    snapshot = tmp_path / "before-provider.sqlite3"
    _sqlite_backup(service.ledger_path, snapshot)
    service.dispatch(
        canonical_model_json_bytes(
            SourceBrokerV2DispatchEnvelope(request=request, claim_receipt=claim)
        )
    )
    _sqlite_backup(snapshot, service.ledger_path)
    rollbackable_fake.forget(request.operation_id)

    with pytest.raises(TypeError, match="exact Unix.*authority client"):
        SourceBrokerV2ProviderService(
            ledger_path=service.ledger_path,
            provider=provider,
            authority_signer=OpenSslSourceBrokerV2AuthoritySigner(
                authority_id="source-authority",
                key_id="current",
                private_key_path=tmp_path / "keys-current" / "current.private.pem",
            ),
            authority_keyring=keyring,
            external_dispatch_authority=rollbackable_fake,
            clock=lambda: NOW,
        )

    assert provider.dispatch_calls == 1


def test_exact_external_authority_client_accepts_rotated_signed_lookup(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import rquant.source_broker_v2_service as service_module
    from rquant.source_broker_v2_service import (
        ExternalDispatchAuthorityResponse,
        UnixSocketExternalDispatchAuthorityClient,
    )

    _current_private, current_public = _keypair(tmp_path / "external-current", "current")
    next_private, next_public = _keypair(tmp_path / "external-next", "next")
    endpoint = SocketEndpointPolicy(
        path=tmp_path / "external-authority.sock",
        owner_uid=os.getuid(),
        group_gid=os.getgid(),
        mode=0o600,
    )
    identity = SocketEndpointIdentity(
        device=1,
        inode=2,
        owner_uid=os.getuid(),
        group_gid=os.getgid(),
        mode=0o600,
    )
    scripted = _ScriptedUnixSocket(
        lambda request: _external_signed_response(
            request,
            private_key=next_private,
            key_id="next",
        )
    )
    monkeypatch.setattr(service_module, "require_linux_source_broker_transport", lambda: None)
    monkeypatch.setattr(
        service_module,
        "validate_socket_endpoint",
        lambda _endpoint, expected_identity=None: expected_identity or identity,
    )
    monkeypatch.setattr(service_module, "verify_connected_server_authority", lambda **_kw: None)
    monkeypatch.setattr(
        service_module,
        "_external_kernel_peer_credentials",
        lambda _connection: (123, os.getuid(), os.getgid()),
    )
    monkeypatch.setattr(service_module.socket, "socket", lambda *_args, **_kw: scripted)
    client = UnixSocketExternalDispatchAuthorityClient(
        endpoint=endpoint,
        server_policy=ServerCredentialsPolicy(
            expected_uid=os.getuid(),
            expected_gid=os.getgid(),
            expected_pid=123,
        ),
        expected_authority_id="external-authority",
        allowed_public_keys={"current": current_public, "next": next_public},
        total_request_deadline_seconds=1,
    )

    raw = client.lookup(_external_reserve_request())
    response = ExternalDispatchAuthorityResponse.model_validate_json(raw)

    assert response.operation == "lookup"
    assert response.status == "absent"
    assert client.allowed_key_ids == ("current", "next")
    assert scripted.sent
    with pytest.raises(AttributeError):
        client.lookup = lambda _payload: b"{}"  # type: ignore[method-assign]


@pytest.mark.parametrize("attack", ["old_challenge", "forged_signature"])
def test_exact_external_authority_client_rejects_forged_lookup(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    attack: str,
) -> None:
    import rquant.source_broker_v2_service as service_module
    from rquant.source_broker_v2_service import UnixSocketExternalDispatchAuthorityClient

    current_private, current_public = _keypair(tmp_path / "external-trusted", "current")
    rogue_private, _rogue_public = _keypair(tmp_path / "external-rogue", "rogue")
    signing_key = rogue_private if attack == "forged_signature" else current_private
    endpoint = SocketEndpointPolicy(
        path=tmp_path / f"external-{attack}.sock",
        owner_uid=os.getuid(),
        group_gid=os.getgid(),
        mode=0o600,
    )
    identity = SocketEndpointIdentity(1, 2, os.getuid(), os.getgid(), 0o600)
    scripted = _ScriptedUnixSocket(
        lambda request: _external_signed_response(
            request,
            private_key=signing_key,
            key_id="current",
            challenge="0" * 64 if attack == "old_challenge" else None,
        )
    )
    monkeypatch.setattr(service_module, "require_linux_source_broker_transport", lambda: None)
    monkeypatch.setattr(
        service_module,
        "validate_socket_endpoint",
        lambda _endpoint, expected_identity=None: expected_identity or identity,
    )
    monkeypatch.setattr(service_module, "verify_connected_server_authority", lambda **_kw: None)
    monkeypatch.setattr(
        service_module,
        "_external_kernel_peer_credentials",
        lambda _connection: (123, os.getuid(), os.getgid()),
    )
    monkeypatch.setattr(service_module.socket, "socket", lambda *_args, **_kw: scripted)
    client = UnixSocketExternalDispatchAuthorityClient(
        endpoint=endpoint,
        server_policy=ServerCredentialsPolicy(
            expected_uid=os.getuid(),
            expected_gid=os.getgid(),
            expected_pid=123,
        ),
        expected_authority_id="external-authority",
        allowed_public_keys={"current": current_public},
        total_request_deadline_seconds=1,
    )

    with pytest.raises(SourceBrokerV2SagaIntegrityError, match="binding|signature"):
        client.lookup(_external_reserve_request())


@pytest.mark.parametrize("attack", ["wrong_peer", "endpoint_replaced"])
def test_exact_external_authority_client_rejects_transport_identity_before_write(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    attack: str,
) -> None:
    import rquant.source_broker_v2_service as service_module
    from rquant.source_broker_v2_service import UnixSocketExternalDispatchAuthorityClient

    _private, public = _keypair(tmp_path / "external-transport", "current")
    endpoint = SocketEndpointPolicy(
        path=tmp_path / f"external-{attack}.sock",
        owner_uid=os.getuid(),
        group_gid=os.getgid(),
        mode=0o600,
    )
    identity = SocketEndpointIdentity(1, 2, os.getuid(), os.getgid(), 0o600)
    scripted = _ScriptedUnixSocket(
        lambda _request: pytest.fail("identity failure must happen before response read")
    )
    validations = 0

    def validate(_endpoint, expected_identity=None):
        nonlocal validations
        validations += 1
        if attack == "endpoint_replaced" and expected_identity is not None:
            raise SourceBrokerTransportError("external authority endpoint was replaced")
        return expected_identity or identity

    monkeypatch.setattr(service_module, "require_linux_source_broker_transport", lambda: None)
    monkeypatch.setattr(service_module, "validate_socket_endpoint", validate)
    monkeypatch.setattr(service_module, "verify_connected_server_authority", lambda **_kw: None)
    peer_uid = os.getuid() + 1 if attack == "wrong_peer" else os.getuid()
    monkeypatch.setattr(
        service_module,
        "_external_kernel_peer_credentials",
        lambda _connection: (123, peer_uid, os.getgid()),
    )
    monkeypatch.setattr(service_module.socket, "socket", lambda *_args, **_kw: scripted)
    client = UnixSocketExternalDispatchAuthorityClient(
        endpoint=endpoint,
        server_policy=ServerCredentialsPolicy(
            expected_uid=os.getuid(),
            expected_gid=os.getgid(),
            expected_pid=123,
        ),
        expected_authority_id="external-authority",
        allowed_public_keys={"current": public},
        total_request_deadline_seconds=1,
    )

    with pytest.raises(SourceBrokerTransportError, match="credentials|replaced"):
        client.lookup(_external_reserve_request())

    assert not scripted.sent
    assert validations >= 1


def test_exact_external_authority_client_enforces_one_total_deadline(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import rquant.source_broker_v2_service as service_module
    from rquant.source_broker_v2_service import UnixSocketExternalDispatchAuthorityClient

    _private, public = _keypair(tmp_path / "external-deadline", "current")
    endpoint = SocketEndpointPolicy(
        path=tmp_path / "external-deadline.sock",
        owner_uid=os.getuid(),
        group_gid=os.getgid(),
        mode=0o600,
    )
    identity = SocketEndpointIdentity(1, 2, os.getuid(), os.getgid(), 0o600)

    def delayed_response(_request: bytes) -> bytes:
        time.sleep(0.03)
        return b"{}"

    scripted = _ScriptedUnixSocket(delayed_response)
    monkeypatch.setattr(service_module, "require_linux_source_broker_transport", lambda: None)
    monkeypatch.setattr(
        service_module,
        "validate_socket_endpoint",
        lambda _endpoint, expected_identity=None: expected_identity or identity,
    )
    monkeypatch.setattr(service_module, "verify_connected_server_authority", lambda **_kw: None)
    monkeypatch.setattr(
        service_module,
        "_external_kernel_peer_credentials",
        lambda _connection: (123, os.getuid(), os.getgid()),
    )
    monkeypatch.setattr(service_module.socket, "socket", lambda *_args, **_kw: scripted)
    client = UnixSocketExternalDispatchAuthorityClient(
        endpoint=endpoint,
        server_policy=ServerCredentialsPolicy(
            expected_uid=os.getuid(),
            expected_gid=os.getgid(),
            expected_pid=123,
        ),
        expected_authority_id="external-authority",
        allowed_public_keys={"current": public},
        total_request_deadline_seconds=0.01,
    )

    started = time.monotonic()
    with pytest.raises(SourceBrokerV2TransportDeadlineError, match="deadline"):
        client.lookup(_external_reserve_request())

    assert time.monotonic() - started < _host(0.2)


def test_complete_committed_response_loss_recovers_through_signed_lookup(
    tmp_path: Path,
) -> None:
    provider = _CountingProvider()
    authority = _FakeExternalDispatchAuthority()
    authority.drop_complete_response_once = True
    events: list[object] = []
    service, keyring = _service(
        tmp_path,
        provider=provider,
        external_authority=authority,
        event_sink=events.append,
    )
    request = _dispatch_request()
    claim = _claim(service, _claim_once_request(request))
    envelope = canonical_model_json_bytes(
        SourceBrokerV2DispatchEnvelope(request=request, claim_receipt=claim)
    )

    with pytest.raises(SourceBrokerTransportError, match="unknown|reconcile"):
        service.dispatch(envelope)
    assert provider.dispatch_calls == 1
    assert authority.complete_calls == 1

    replay_request = SourceBrokerV2ReplayRequest(
        saga_id=request.saga_id,
        operation_id=request.operation_id,
        phase=SourceBrokerV2OutboxPhase.DISPATCH,
        operation_request_hash=request.request_hash,
        challenge="e" * 64,
    )
    replay = SourceBrokerV2ReplayResponse.model_validate_json(
        service.replay(canonical_model_json_bytes(replay_request))
    )
    keyring.require_verified_replay(request=replay_request, receipt=replay)

    assert replay.status is SourceBrokerV2ReplayStatus.FOUND
    assert replay.result is not None
    assert authority.lookup_calls == 1
    assert provider.dispatch_calls == 1
    assert _ledger_row(tmp_path, request.operation_id)["status"] == "success"
    complete_events = [
        event
        for event in events
        if event.category == "authority_error" and event.authority_operation == "complete"
    ]
    assert len(complete_events) == 1
    assert complete_events[0].reconcile is True
    assert complete_events[0].outcome == "failure"
    lookup_events = [event for event in events if event.category == "authority_lookup"]
    assert len(lookup_events) == 1
    assert lookup_events[0].authority_operation == "lookup"
    assert lookup_events[0].outcome == "found"
    assert lookup_events[0].operation_hash != request.operation_id


@pytest.mark.parametrize(
    ("authority_operation", "error_type"),
    [
        ("reserve", SourceBrokerV2TransportDeadlineError),
        ("lookup", _ExternalAuthoritySignatureError),
        ("complete", _ExternalAuthorityConflictError),
    ],
)
def test_external_authority_operation_errors_emit_dedicated_redacted_events(
    tmp_path: Path,
    authority_operation: str,
    error_type: type[BaseException],
) -> None:
    secret = f"SENSITIVE-{authority_operation}-payload-token-raw-result-signature"
    events: list[object] = []
    provider = _CountingProvider()
    authority = _FakeExternalDispatchAuthority()
    service, _keyring = _service(
        tmp_path,
        provider=provider,
        external_authority=authority,
        event_sink=events.append,
    )
    request = _dispatch_request()
    claim = _claim(service, _claim_once_request(request))
    envelope = canonical_model_json_bytes(
        SourceBrokerV2DispatchEnvelope(request=request, claim_receipt=claim)
    )

    if authority_operation == "lookup":
        authority.drop_complete_response_once = True
        with pytest.raises(SourceBrokerTransportError, match="unknown|reconcile"):
            service.dispatch(envelope)
        events.clear()
        authority.failures["lookup"] = error_type(secret)
        replay_request = SourceBrokerV2ReplayRequest(
            saga_id=request.saga_id,
            operation_id=request.operation_id,
            phase=SourceBrokerV2OutboxPhase.DISPATCH,
            operation_request_hash=request.request_hash,
            challenge="0" * 64,
        )
        replay = SourceBrokerV2ReplayResponse.model_validate_json(
            service.replay(canonical_model_json_bytes(replay_request))
        )
        assert replay.status is SourceBrokerV2ReplayStatus.UNKNOWN
    else:
        authority.failures[authority_operation] = error_type(secret)
        with pytest.raises(
            (SourceBrokerTransportError, SourceBrokerV2TransportDeadlineError),
            match="unknown|reconcile|deadline",
        ):
            service.dispatch(envelope)

    authority_events = [
        event
        for event in events
        if event.category == "authority_error" and event.authority_operation == authority_operation
    ]
    assert len(authority_events) == 1
    event = authority_events[0]
    assert event.phase == "dispatch"
    assert event.operation_hash != request.operation_id
    assert event.exception_class == error_type.__name__
    assert event.reconcile is True
    assert event.outcome == "failure"
    rendered = "\n".join(item.model_dump_json() for item in events)
    assert secret not in rendered
    assert request.operation_id not in rendered
    assert request.payload.decode("utf-8") not in rendered


def test_lookup_absent_after_provider_started_never_redispatches(tmp_path: Path) -> None:
    provider = _CountingProvider()
    authority = _FakeExternalDispatchAuthority()
    authority.drop_complete_response_once = True
    service, keyring = _service(
        tmp_path,
        provider=provider,
        external_authority=authority,
    )
    request = _dispatch_request()
    claim = _claim(service, _claim_once_request(request))
    envelope = canonical_model_json_bytes(
        SourceBrokerV2DispatchEnvelope(request=request, claim_receipt=claim)
    )
    with pytest.raises(SourceBrokerTransportError, match="unknown|reconcile"):
        service.dispatch(envelope)
    authority.forget(request.operation_id)

    replay_request = SourceBrokerV2ReplayRequest(
        saga_id=request.saga_id,
        operation_id=request.operation_id,
        phase=SourceBrokerV2OutboxPhase.DISPATCH,
        operation_request_hash=request.request_hash,
        challenge="d" * 64,
    )
    replay = SourceBrokerV2ReplayResponse.model_validate_json(
        service.replay(canonical_model_json_bytes(replay_request))
    )
    keyring.require_verified_replay(request=replay_request, receipt=replay)

    assert replay.status is SourceBrokerV2ReplayStatus.UNKNOWN
    assert authority.lookup_calls == 1
    assert provider.dispatch_calls == 1


def test_terminal_result_loss_is_repaired_by_external_lookup(tmp_path: Path) -> None:
    provider = _CountingProvider()
    authority = _FakeExternalDispatchAuthority()
    service, keyring = _service(
        tmp_path,
        provider=provider,
        external_authority=authority,
    )
    request = _dispatch_request()
    claim = _claim(service, _claim_once_request(request))
    expected = service.dispatch(
        canonical_model_json_bytes(
            SourceBrokerV2DispatchEnvelope(request=request, claim_receipt=claim)
        )
    )
    with sqlite3.connect(service.ledger_path) as connection:
        connection.execute(
            "UPDATE source_broker_v2_provider_operation SET result_json = NULL, "
            "result_hash = NULL WHERE operation_id = ?",
            (request.operation_id,),
        )

    replay_request = SourceBrokerV2ReplayRequest(
        saga_id=request.saga_id,
        operation_id=request.operation_id,
        phase=SourceBrokerV2OutboxPhase.DISPATCH,
        operation_request_hash=request.request_hash,
        challenge="c" * 64,
    )
    replay = SourceBrokerV2ReplayResponse.model_validate_json(
        service.replay(canonical_model_json_bytes(replay_request))
    )
    keyring.require_verified_replay(request=replay_request, receipt=replay)

    assert replay.status is SourceBrokerV2ReplayStatus.FOUND
    assert replay.result == expected
    assert authority.lookup_calls == 1
    assert provider.dispatch_calls == 1


def test_external_authority_recovers_after_local_ledger_snapshot_rollback(
    tmp_path: Path,
) -> None:
    provider = _CountingProvider()
    authority = _FakeExternalDispatchAuthority()
    service, keyring = _service(
        tmp_path,
        provider=provider,
        external_authority=authority,
    )
    request = _dispatch_request()
    claim_request = _claim_once_request(request)
    claim = _claim(service, claim_request)
    keyring.require_verified_claim(request=claim_request, receipt=claim)
    snapshot_path = tmp_path / "pre-dispatch-snapshot.sqlite3"
    _sqlite_backup(service.ledger_path, snapshot_path)
    envelope = canonical_model_json_bytes(
        SourceBrokerV2DispatchEnvelope(request=request, claim_receipt=claim)
    )

    first = service.dispatch(envelope)
    assert provider.dispatch_calls == 1
    assert authority.complete_calls == 1

    _sqlite_backup(snapshot_path, service.ledger_path)
    restarted, restarted_keyring = _service(
        tmp_path,
        provider=provider,
        external_authority=authority,
    )
    replayed_claim = _claim(restarted, claim_request)
    restarted_keyring.require_verified_claim(request=claim_request, receipt=replayed_claim)
    recovered = restarted.dispatch(
        canonical_model_json_bytes(
            SourceBrokerV2DispatchEnvelope(
                request=request,
                claim_receipt=replayed_claim,
            )
        )
    )

    assert recovered == first
    assert provider.dispatch_calls == 1
    assert authority.reserve_calls == 2


def test_external_authority_loss_marks_reconcile_before_provider_call(tmp_path: Path) -> None:
    provider = _CountingProvider()
    authority = _FakeExternalDispatchAuthority()
    events: list[object] = []
    service, keyring = _service(
        tmp_path,
        provider=provider,
        external_authority=authority,
        event_sink=events.append,
    )
    request = _dispatch_request()
    claim = _claim(service, _claim_once_request(request))
    secret = "SENSITIVE-reserve-transport-payload-token-signature"
    authority.failures["reserve"] = SourceBrokerTransportError(secret)

    with pytest.raises(SourceBrokerTransportError, match="unknown|reconcile"):
        service.dispatch(
            canonical_model_json_bytes(
                SourceBrokerV2DispatchEnvelope(request=request, claim_receipt=claim)
            )
        )

    replay_request = SourceBrokerV2ReplayRequest(
        saga_id=request.saga_id,
        operation_id=request.operation_id,
        phase=SourceBrokerV2OutboxPhase.DISPATCH,
        operation_request_hash=request.request_hash,
        challenge="f" * 64,
    )
    replay = SourceBrokerV2ReplayResponse.model_validate_json(
        service.replay(canonical_model_json_bytes(replay_request))
    )
    keyring.require_verified_replay(request=replay_request, receipt=replay)
    assert replay.status is SourceBrokerV2ReplayStatus.ABSENT
    assert provider.dispatch_calls == 0
    authority_events = [
        event
        for event in events
        if event.category == "authority_error" and event.authority_operation == "reserve"
    ]
    assert len(authority_events) == 1
    assert authority_events[0].exception_class == "SourceBrokerTransportError"
    assert secret not in "\n".join(event.model_dump_json() for event in events)


def test_old_external_authority_fence_is_not_reused_from_local_copy(tmp_path: Path) -> None:
    provider = _CountingProvider()
    authority = _FakeExternalDispatchAuthority()
    service, _keyring = _service(
        tmp_path,
        provider=provider,
        external_authority=authority,
    )
    request = _dispatch_request()
    claim = _claim(service, _claim_once_request(request))
    envelope = canonical_model_json_bytes(
        SourceBrokerV2DispatchEnvelope(request=request, claim_receipt=claim)
    )
    first = service.dispatch(envelope)
    assert first
    assert provider.dispatch_calls == 1
    row = _ledger_row(tmp_path, request.operation_id)
    old_generation = row["external_authority_generation"]
    old_fence = row["external_authority_fence"]

    with sqlite3.connect(service.ledger_path) as connection:
        connection.execute(
            "UPDATE source_broker_v2_provider_operation SET "
            "status = ?, result_json = NULL, result_hash = NULL, "
            "invocation_owner_token = NULL, invocation_owner_pid = NULL, "
            "terminal_at = NULL, unknown_reason = NULL WHERE operation_id = ?",
            ("definitively_absent", request.operation_id),
        )
    authority.force_old_absent = True
    with pytest.raises(SourceBrokerTransportError, match="unknown|reconcile|fence"):
        service.dispatch(envelope)

    current = _ledger_row(tmp_path, request.operation_id)
    assert current["external_authority_generation"] == old_generation
    assert current["external_authority_fence"] == old_fence
    assert current["status"] == "reconcile_required"
    assert provider.dispatch_calls == 1


def test_claim_dispatch_finalize_and_replay_survive_restart_with_one_provider_call(
    tmp_path: Path,
) -> None:
    provider = _CountingProvider()
    service, keyring = _service(tmp_path, provider=provider)
    request = _dispatch_request()
    claim_request = _claim_once_request(request)

    claim = _claim(service, claim_request)
    keyring.require_verified_claim(request=claim_request, receipt=claim)
    assert claim.status is SourceBrokerV2ClaimStatus.DEFINITIVELY_ABSENT

    dispatch_raw = service.dispatch(
        canonical_model_json_bytes(
            SourceBrokerV2DispatchEnvelope(request=request, claim_receipt=claim)
        )
    )
    dispatch = SourceBrokerV2DispatchResponse.model_validate_json(dispatch_raw)
    assert dispatch.request_hash == request.request_hash
    assert provider.dispatch_calls == 1

    duplicated = service.dispatch(
        canonical_model_json_bytes(
            SourceBrokerV2DispatchEnvelope(request=request, claim_receipt=claim)
        )
    )
    assert duplicated == dispatch_raw
    assert provider.dispatch_calls == 1

    restarted, restarted_keyring = _service(tmp_path, provider=provider)
    replay_request = SourceBrokerV2ReplayRequest(
        saga_id=request.saga_id,
        operation_id=request.operation_id,
        phase=SourceBrokerV2OutboxPhase.DISPATCH,
        operation_request_hash=request.request_hash,
        challenge="8" * 64,
    )
    replay = SourceBrokerV2ReplayResponse.model_validate_json(
        restarted.replay(canonical_model_json_bytes(replay_request))
    )
    restarted_keyring.require_verified_replay(request=replay_request, receipt=replay)
    assert replay.status is SourceBrokerV2ReplayStatus.FOUND
    assert replay.result == dispatch_raw
    assert provider.dispatch_calls == 1

    terminal_claim = _claim(restarted, claim_request)
    restarted_keyring.require_verified_claim(request=claim_request, receipt=terminal_claim)
    assert terminal_claim.status is SourceBrokerV2ClaimStatus.SUCCESS
    assert terminal_claim.result == dispatch_raw

    finalize_request = _finalize_request(dispatch)
    finalize_claim_request = _claim_once_request(
        _dispatch_request(operation_id=finalize_request.operation_id),
        challenge=HASHES["final_challenge"],
        phase=SourceBrokerV2OutboxPhase.SOURCE_FINALIZE,
        operation_request_hash=finalize_request.request_hash,
    )
    finalize_claim = _claim(restarted, finalize_claim_request)
    finalize = SourceBrokerV2FinalizeResponse.model_validate_json(
        restarted.finalize(
            canonical_model_json_bytes(
                SourceBrokerV2FinalizeEnvelope(
                    request=finalize_request,
                    claim_receipt=finalize_claim,
                )
            )
        )
    )
    assert finalize.request_hash == finalize_request.request_hash
    assert provider.finalize_calls == 1


def test_provider_started_then_unknown_never_dispatches_automatically_again(
    tmp_path: Path,
) -> None:
    provider = _CountingProvider(fail_dispatch_once=True)
    service, _keyring = _service(tmp_path, provider=provider)
    request = _dispatch_request()
    claim = _claim(service, _claim_once_request(request))
    envelope = canonical_model_json_bytes(
        SourceBrokerV2DispatchEnvelope(request=request, claim_receipt=claim)
    )

    with pytest.raises(SourceBrokerTransportError, match="reconcile|required|unknown"):
        service.dispatch(envelope)

    restarted, keyring = _service(tmp_path, provider=provider)
    replay_request = SourceBrokerV2ReplayRequest(
        saga_id=request.saga_id,
        operation_id=request.operation_id,
        phase=SourceBrokerV2OutboxPhase.DISPATCH,
        operation_request_hash=request.request_hash,
        challenge="9" * 64,
    )
    replay = SourceBrokerV2ReplayResponse.model_validate_json(
        restarted.replay(canonical_model_json_bytes(replay_request))
    )
    keyring.require_verified_replay(request=replay_request, receipt=replay)
    assert replay.status is SourceBrokerV2ReplayStatus.UNKNOWN
    with pytest.raises(SourceBrokerTransportError, match="reconcile|required|unknown"):
        restarted.dispatch(envelope)
    assert provider.dispatch_calls == 1


def test_service_construction_waits_for_listener_reconciliation_of_foreign_invocation(
    tmp_path: Path,
) -> None:
    service, _keyring = _service(tmp_path)
    request = _dispatch_request()
    claim_request = _claim_once_request(request)
    _claim(service, claim_request)
    ledger_path = tmp_path / "source-provider-ledger.sqlite3"
    with sqlite3.connect(ledger_path) as connection:
        connection.execute(
            "UPDATE source_broker_v2_provider_operation SET "
            "status = 'invoking', provider_started_at = ?, invocation_owner_token = ?, "
            "invocation_owner_pid = ?, updated_at = ? WHERE operation_id = ?",
            (
                NOW.isoformat(),
                "foreign-daemon-token",
                999_999,
                NOW.isoformat(),
                request.operation_id,
            ),
        )

    restarted, keyring = _service(tmp_path)
    assert _ledger_row(tmp_path, request.operation_id)["status"] == "invoking"
    restarted.reconcile_abandoned_invocations_after_listener_acquired()
    assert _ledger_row(tmp_path, request.operation_id)["status"] == "reconcile_required"

    replay_request = SourceBrokerV2ReplayRequest(
        saga_id=request.saga_id,
        operation_id=request.operation_id,
        phase=SourceBrokerV2OutboxPhase.DISPATCH,
        operation_request_hash=request.request_hash,
        challenge="e" * 64,
    )
    replay = SourceBrokerV2ReplayResponse.model_validate_json(
        restarted.replay(canonical_model_json_bytes(replay_request))
    )

    keyring.require_verified_replay(request=replay_request, receipt=replay)
    assert replay.status is SourceBrokerV2ReplayStatus.UNKNOWN


def test_replay_refuses_to_sign_terminal_result_for_alternate_saga(
    tmp_path: Path,
) -> None:
    provider = _CountingProvider()
    service, _keyring = _service(tmp_path, provider=provider)
    request = _dispatch_request()
    claim = _claim(service, _claim_once_request(request))
    service.dispatch(
        canonical_model_json_bytes(
            SourceBrokerV2DispatchEnvelope(request=request, claim_receipt=claim)
        )
    )
    alternate = SourceBrokerV2ReplayRequest(
        saga_id="alternate-saga",
        operation_id=request.operation_id,
        phase=SourceBrokerV2OutboxPhase.DISPATCH,
        operation_request_hash=request.request_hash,
        challenge="f" * 64,
    )

    with pytest.raises(SourceBrokerTransportError, match="binding conflicts"):
        service.replay(canonical_model_json_bytes(alternate))

    assert provider.dispatch_calls == 1


def test_claim_conflict_cannot_rebind_takeover_window_or_overwrite_fence(
    tmp_path: Path,
) -> None:
    import sqlite3

    service, _keyring = _service(tmp_path)
    request = _dispatch_request()
    original = _claim_once_request(request)
    claim = _claim(service, original)
    assert claim.status is SourceBrokerV2ClaimStatus.DEFINITIVELY_ABSENT

    malicious = _claim_once_request(
        request,
        challenge="a" * 64,
        executor_owner_token_hash="e" * 64,
        executor_generation=4,
        max_external_deadline=NOW,
        not_before_takeover_at=NOW,
    )
    refused = _claim(service, malicious)
    assert refused.status is SourceBrokerV2ClaimStatus.INFLIGHT

    with sqlite3.connect(tmp_path / "source-provider-ledger.sqlite3") as connection:
        row = connection.execute(
            "SELECT executor_owner_token_hash, executor_generation, "
            "max_external_deadline, not_before_takeover_at "
            "FROM source_broker_v2_provider_operation WHERE operation_id = ?",
            (request.operation_id,),
        ).fetchone()
        attempts = connection.execute(
            "SELECT attempt_id FROM source_broker_v2_provider_claim_attempt "
            "WHERE effect_operation_id = ?",
            (request.operation_id,),
        ).fetchall()

    assert row == (
        original.executor_owner_token_hash,
        original.executor_generation,
        original.max_external_deadline.isoformat(),
        original.not_before_takeover_at.isoformat(),
    )
    assert attempts == [
        (
            source_claim_attempt_id(
                effect_operation_id=request.operation_id,
                executor_owner_token_hash=original.executor_owner_token_hash,
                executor_generation=original.executor_generation,
                max_external_deadline=original.max_external_deadline,
                not_before_takeover_at=original.not_before_takeover_at,
            ),
        )
    ]


def test_takeover_replaces_current_fence_and_rejects_stale_grant_dispatch(
    tmp_path: Path,
) -> None:
    provider = _CountingProvider()
    service, keyring = _service(tmp_path, provider=provider)
    request = _dispatch_request()
    original_request = _claim_once_request(
        request,
        max_external_deadline=NOW,
        not_before_takeover_at=NOW,
    )
    original_claim = _claim(service, original_request)
    keyring.require_verified_claim(request=original_request, receipt=original_claim)

    takeover_request = _claim_once_request(
        request,
        challenge="b" * 64,
        executor_owner_token_hash="e" * 64,
        executor_generation=original_request.executor_generation,
        max_external_deadline=NOW,
        not_before_takeover_at=NOW,
    )
    takeover_claim = _claim(service, takeover_request)
    keyring.require_verified_claim(request=takeover_request, receipt=takeover_claim)
    assert takeover_claim.status is SourceBrokerV2ClaimStatus.DEFINITIVELY_ABSENT

    with pytest.raises(SourceBrokerTransportError, match="claim binding conflicts"):
        service.dispatch(
            canonical_model_json_bytes(
                SourceBrokerV2DispatchEnvelope(request=request, claim_receipt=original_claim)
            )
        )

    dispatch = SourceBrokerV2DispatchResponse.model_validate_json(
        service.dispatch(
            canonical_model_json_bytes(
                SourceBrokerV2DispatchEnvelope(request=request, claim_receipt=takeover_claim)
            )
        )
    )
    assert dispatch.operation_id == request.operation_id
    assert provider.dispatch_calls == 1
    with sqlite3.connect(tmp_path / "source-provider-ledger.sqlite3") as connection:
        attempts = connection.execute(
            "SELECT attempt_id, executor_owner_token_hash, executor_generation "
            "FROM source_broker_v2_provider_claim_attempt "
            "WHERE effect_operation_id = ? ORDER BY created_at",
            (request.operation_id,),
        ).fetchall()
    assert len(attempts) == 2
    assert attempts[0][0] != attempts[1][0]
    assert attempts[0][1:] == (
        original_request.executor_owner_token_hash,
        original_request.executor_generation,
    )
    assert attempts[1][1:] == (
        takeover_request.executor_owner_token_hash,
        takeover_request.executor_generation,
    )


def test_live_claim_and_replay_do_not_poison_inflight_provider_completion(
    tmp_path: Path,
) -> None:
    provider = _BlockingProvider()
    service, keyring = _service(tmp_path, provider=provider)
    request = _dispatch_request()
    claim_request = _claim_once_request(request)
    claim = _claim(service, claim_request)
    envelope = canonical_model_json_bytes(
        SourceBrokerV2DispatchEnvelope(request=request, claim_receipt=claim)
    )
    results: list[bytes] = []
    errors: list[BaseException] = []

    def invoke() -> None:
        try:
            results.append(service.dispatch(envelope))
        except BaseException as exc:  # pragma: no cover - asserted below
            errors.append(exc)

    thread = threading.Thread(target=invoke)
    thread.start()
    assert provider.entered.wait(timeout=_PROVIDER_ENTRY_WATCHDOG_SECONDS)

    live_claim_request = _claim_once_request(request, challenge="b" * 64)
    live_claim = _claim(service, live_claim_request)
    keyring.require_verified_claim(request=live_claim_request, receipt=live_claim)
    assert live_claim.status is SourceBrokerV2ClaimStatus.UNKNOWN

    live_replay_request = SourceBrokerV2ReplayRequest(
        saga_id=request.saga_id,
        operation_id=request.operation_id,
        phase=SourceBrokerV2OutboxPhase.DISPATCH,
        operation_request_hash=request.request_hash,
        challenge="c" * 64,
    )
    live_replay = SourceBrokerV2ReplayResponse.model_validate_json(
        service.replay(canonical_model_json_bytes(live_replay_request))
    )
    keyring.require_verified_replay(request=live_replay_request, receipt=live_replay)
    assert live_replay.status is SourceBrokerV2ReplayStatus.UNKNOWN

    provider.release.set()
    thread.join(timeout=2)
    assert not thread.is_alive()
    assert not errors
    assert len(results) == 1

    dispatch = SourceBrokerV2DispatchResponse.model_validate_json(results[0])
    assert dispatch.operation_id == request.operation_id
    found_replay_request = SourceBrokerV2ReplayRequest(
        saga_id=request.saga_id,
        operation_id=request.operation_id,
        phase=SourceBrokerV2OutboxPhase.DISPATCH,
        operation_request_hash=request.request_hash,
        challenge="d" * 64,
    )
    found_replay = SourceBrokerV2ReplayResponse.model_validate_json(
        service.replay(canonical_model_json_bytes(found_replay_request))
    )
    assert found_replay.status is SourceBrokerV2ReplayStatus.FOUND
    assert found_replay.result == results[0]
    assert provider.dispatch_calls == 1


def test_duplicate_dispatch_wait_uses_single_request_deadline(tmp_path: Path) -> None:
    provider = _BlockingProvider()
    service, _keyring = _service(tmp_path, provider=provider, busy_timeout_ms=500)
    request = _dispatch_request()
    claim = _claim(service, _claim_once_request(request))
    envelope = canonical_model_json_bytes(
        SourceBrokerV2DispatchEnvelope(request=request, claim_receipt=claim)
    )
    results: list[bytes] = []
    errors: list[BaseException] = []

    def invoke() -> None:
        try:
            results.append(service.dispatch(envelope))
        except BaseException as exc:  # pragma: no cover - asserted below
            errors.append(exc)

    thread = threading.Thread(target=invoke)
    thread.start()
    assert provider.entered.wait(timeout=_PROVIDER_ENTRY_WATCHDOG_SECONDS)

    started = time.monotonic()
    # Matched on the stage, not on the word "deadline". Every checkpoint in the
    # dispatch prologue raises a message containing "deadline" too, so the loose
    # match accepted a refusal that never reached the duplicate wait at all -
    # and accepted it *green*, which is worse than a red: the case kept passing
    # while silently testing nothing it names. Measured, not feared: at a
    # prologue 5x its calibrated cost this case still passes with the refusal
    # coming from `after dispatch claim verification`. `_wait_for_terminal` has
    # exactly two deadline exits and both say "duplicate provider", so the
    # family literal pins the stage without pinning which of the two won a race
    # the case does not care about.
    with pytest.raises(SourceBrokerV2TransportDeadlineError, match="duplicate provider"):
        service.dispatch(envelope, deadline=time.monotonic() + _host(0.05))
    assert time.monotonic() - started < _host(0.25)

    provider.release.set()
    thread.join(timeout=2)
    assert not thread.is_alive()
    assert not errors
    assert len(results) == 1


def test_blocked_provider_dispatch_returns_at_deadline_without_auto_repeat(
    tmp_path: Path,
) -> None:
    deadline_seconds = _host(_BLOCKED_PROVIDER_DEADLINE_SECONDS)
    # The provider has to still be inside its call when the deadline expires, so
    # its block outlives the whole budget. It never waits that long in practice:
    # the case releases it as soon as the refusal has been observed.
    provider = _ShortBlockingProvider(block_seconds=deadline_seconds * 4)
    service, _keyring = _service(tmp_path, provider=provider, busy_timeout_ms=500)
    request = _dispatch_request()
    claim = _claim(service, _claim_once_request(request))
    envelope = canonical_model_json_bytes(
        SourceBrokerV2DispatchEnvelope(request=request, claim_receipt=claim)
    )

    deadline = time.monotonic() + deadline_seconds
    try:
        with pytest.raises(SourceBrokerV2TransportDeadlineError, match="deadline"):
            service.dispatch(envelope, deadline=deadline)
    finally:
        provider.release.set()
    returned_at = time.monotonic()
    # Where the refusal came from, stated rather than inferred: a prologue
    # checkpoint firing first would leave the provider unentered, and that used
    # to surface three assertions later as `dispatch_calls == 0`.
    assert provider.entered.is_set()
    # Returned at the deadline, not when the provider's block would have ended.
    assert returned_at - deadline < _host(0.2)

    replay_request = SourceBrokerV2ReplayRequest(
        saga_id=request.saga_id,
        operation_id=request.operation_id,
        phase=SourceBrokerV2OutboxPhase.DISPATCH,
        operation_request_hash=request.request_hash,
        challenge="1" * 63 + "2",
    )
    replay = SourceBrokerV2ReplayResponse.model_validate_json(
        service.replay(canonical_model_json_bytes(replay_request))
    )
    assert replay.status is SourceBrokerV2ReplayStatus.UNKNOWN

    # Generous here too, and deliberately: the provider is released by now, so a
    # service that auto-repeated the call would reach it and succeed. A short
    # deadline could hide that behind a second expiry.
    with pytest.raises((SourceBrokerTransportError, SourceBrokerV2TransportDeadlineError)):
        service.dispatch(envelope, deadline=time.monotonic() + deadline_seconds)
    assert provider.dispatch_calls == 1


def test_global_provider_gate_bounds_blocked_sources_before_authority_reserve(
    tmp_path: Path,
) -> None:
    blocked_deadline_seconds = _host(_BLOCKED_PROVIDER_DEADLINE_SECONDS)
    provider = _ShortBlockingProvider(block_seconds=blocked_deadline_seconds * 4)
    authority = _FakeExternalDispatchAuthority()
    events: list[object] = []
    service, _keyring = _service(
        tmp_path,
        provider=provider,
        external_authority=authority,
        max_inflight=1,
        event_sink=events.append,
    )
    first_request = _dispatch_request(saga_id="saga-source-v2-daily").model_copy(
        update={"call_id": "daily-bars"}
    )
    second_request = _dispatch_request(saga_id="saga-source-v2-intraday").model_copy(
        update={"call_id": "intraday-bars"}
    )

    def envelope_for(request: SourceBrokerV2DispatchRequest) -> bytes:
        claim = _claim(service, _claim_once_request(request))
        return canonical_model_json_bytes(
            SourceBrokerV2DispatchEnvelope(request=request, claim_receipt=claim)
        )

    first_envelope = envelope_for(first_request)
    second_envelope = envelope_for(second_request)
    try:
        with pytest.raises(SourceBrokerV2TransportDeadlineError, match="deadline"):
            service.dispatch(first_envelope, deadline=time.monotonic() + blocked_deadline_seconds)
        # See the sibling stop() case: entry is a fact by the time the deadline
        # has been spent inside the provider, so it is asserted, not waited for.
        assert provider.entered.is_set()

        started = time.monotonic()
        with pytest.raises(SourceBrokerTransportError, match="reconcile|required|unknown"):
            service.dispatch(second_envelope, deadline=time.monotonic() + _host(0.5))
        assert time.monotonic() - started < _host(0.2)
        assert provider.dispatch_calls == 1
        assert authority.reserve_calls == 1
        assert (
            sum(
                thread.name == "rquant-source-broker-v2-provider-call"
                for thread in threading.enumerate()
            )
            == 1
        )
        assert _ledger_row(tmp_path, second_request.operation_id)["status"] == (
            "reconcile_required"
        )
    finally:
        provider.release.set()

    deadline = time.monotonic() + 2
    while time.monotonic() < deadline and any(
        thread.name == "rquant-source-broker-v2-provider-call" for thread in threading.enumerate()
    ):
        time.sleep(0.01)
    assert not any(
        thread.name == "rquant-source-broker-v2-provider-call" for thread in threading.enumerate()
    )

    recovered = service.dispatch(second_envelope, deadline=time.monotonic() + 1)
    assert SourceBrokerV2DispatchResponse.model_validate_json(recovered).call_id == (
        "intraday-bars"
    )
    assert provider.dispatch_calls == 2
    assert any(event.category == "provider_capacity" for event in events)
    assert any(
        event.category == "provider_late_outcome" and event.outcome == "success" for event in events
    )


def test_provider_service_stop_prevents_new_threads_and_reservations(tmp_path: Path) -> None:
    blocked_deadline_seconds = _host(_BLOCKED_PROVIDER_DEADLINE_SECONDS)
    provider = _ShortBlockingProvider(block_seconds=blocked_deadline_seconds * 4)
    authority = _FakeExternalDispatchAuthority()
    service, _keyring = _service(
        tmp_path,
        provider=provider,
        external_authority=authority,
    )
    first_request = _dispatch_request(saga_id="saga-source-v2-stop-active")
    second_request = _dispatch_request(saga_id="saga-source-v2-stop-refused")
    first_claim = _claim(service, _claim_once_request(first_request))
    second_claim = _claim(service, _claim_once_request(second_request))
    first_envelope = canonical_model_json_bytes(
        SourceBrokerV2DispatchEnvelope(request=first_request, claim_receipt=first_claim)
    )
    second_envelope = canonical_model_json_bytes(
        SourceBrokerV2DispatchEnvelope(request=second_request, claim_receipt=second_claim)
    )

    try:
        with pytest.raises(SourceBrokerV2TransportDeadlineError, match="deadline"):
            service.dispatch(first_envelope, deadline=time.monotonic() + blocked_deadline_seconds)
        # Stated rather than waited for: the deadline above is spent *inside*
        # the provider call, so entry is already a fact here. When it was not -
        # when a prologue checkpoint refused first - the provider thread was
        # never spawned and no wait could ever have observed the entry.
        assert provider.entered.is_set()

        service.stop()
        service.stop()
        with pytest.raises(
            SourceBrokerTransportError,
            match="reconcile|required|unknown|stopped",
        ):
            service.dispatch(second_envelope, deadline=time.monotonic() + _host(0.5))

        assert provider.dispatch_calls == 1
        assert authority.reserve_calls == 1
        assert (
            sum(
                thread.name == "rquant-source-broker-v2-provider-call"
                for thread in threading.enumerate()
            )
            == 1
        )
    finally:
        provider.release.set()

    deadline = time.monotonic() + 2
    while time.monotonic() < deadline and any(
        thread.name == "rquant-source-broker-v2-provider-call" for thread in threading.enumerate()
    ):
        time.sleep(0.01)
    assert not any(
        thread.name == "rquant-source-broker-v2-provider-call" for thread in threading.enumerate()
    )


def test_security_events_are_structured_and_never_include_provider_secrets(
    tmp_path: Path,
) -> None:
    secret = "TOP-SECRET-PAYLOAD-TOKEN-SIGNATURE"
    events: list[object] = []
    provider = _SensitiveFailProvider(secret)
    service, _keyring = _service(
        tmp_path,
        provider=provider,
        event_sink=events.append,
    )
    request = _dispatch_request(saga_id="saga-source-v2-sensitive-failure")
    claim = _claim(service, _claim_once_request(request))
    envelope = canonical_model_json_bytes(
        SourceBrokerV2DispatchEnvelope(request=request, claim_receipt=claim)
    )

    with pytest.raises(SourceBrokerTransportError, match="reconcile|required|unknown"):
        service.dispatch(envelope)

    assert events
    rendered = "\n".join(event.model_dump_json() for event in events)
    assert secret not in rendered
    assert request.operation_id not in rendered
    assert request.payload.decode("utf-8") not in rendered
    assert all(
        {"phase", "operation_hash", "exception_class", "category", "reconcile"}
        <= set(event.model_dump())
        for event in events
    )
    provider_events = [event for event in events if event.category == "provider_exception"]
    assert len(provider_events) == 1
    assert provider_events[0].phase == "dispatch"
    assert provider_events[0].exception_class == "_SensitiveProviderError"
    assert provider_events[0].reconcile is True
    assert len(provider_events[0].operation_hash) == 64


def test_blocked_provider_finalize_returns_at_deadline_without_auto_repeat(
    tmp_path: Path,
) -> None:
    # The finalize twin of test_blocked_provider_dispatch_returns_at_deadline_
    # without_auto_repeat, and it gets that case's construction rather than the
    # 40ms window it was left on. What this case is about is in its name: the
    # finalize returns *at its deadline* while the provider is still inside the
    # call, and the service does not repeat the provider call afterwards. That
    # needs one ordering - provider entered, then deadline - and an 80ms sleep
    # racing a 40ms deadline secures it only while the prologue stays under
    # 40ms. It did not: the dispatch sibling of this pair went red on a 3.12
    # shard with `dispatch_calls == 0`, the provider never reached.
    #
    # A provider that blocks until this case releases it turns that ordering
    # into a fact. The deadline cannot outrun the provider's return because the
    # provider does not return until released, and the release happens after
    # the refusal has been observed. The budget is then one-sided, so it takes
    # the one-sided constant, and the block cap is derived from it rather than
    # from a literal that a larger deadline could collide with.
    deadline_seconds = _host(_BLOCKED_PROVIDER_DEADLINE_SECONDS)
    provider = _ShortBlockingFinalizeProvider(block_seconds=deadline_seconds * 4)
    service, _keyring = _service(tmp_path, provider=provider, busy_timeout_ms=500)
    request = _dispatch_request()
    claim = _claim(service, _claim_once_request(request))
    dispatch = SourceBrokerV2DispatchResponse.model_validate_json(
        service.dispatch(
            canonical_model_json_bytes(
                SourceBrokerV2DispatchEnvelope(request=request, claim_receipt=claim)
            )
        )
    )
    finalize_request = _finalize_request(dispatch)
    finalize_claim_request = _claim_once_request(
        _dispatch_request(operation_id=finalize_request.operation_id),
        challenge=HASHES["final_challenge"],
        phase=SourceBrokerV2OutboxPhase.SOURCE_FINALIZE,
        operation_request_hash=finalize_request.request_hash,
    )
    finalize_claim = _claim(service, finalize_claim_request)
    envelope = canonical_model_json_bytes(
        SourceBrokerV2FinalizeEnvelope(
            request=finalize_request,
            claim_receipt=finalize_claim,
        )
    )

    deadline = time.monotonic() + deadline_seconds
    try:
        # Pinned to the stage, not to the word "deadline": every checkpoint in
        # the finalize prologue also says "deadline", so the loose match used to
        # accept a refusal that never reached the provider - which is exactly
        # how this shape fails - and left it to be noticed three assertions
        # later, if at all.
        with pytest.raises(SourceBrokerV2TransportDeadlineError, match="during provider finalize"):
            service.finalize(envelope, deadline=deadline)
        returned_at = time.monotonic()
    finally:
        provider.release.set()
    # Where the refusal came from, stated rather than inferred.
    assert provider.entered.is_set()
    # Anchored to the deadline rather than to the call's start, which is the
    # only anchor that still says something once the budget is one-sided:
    # returned at the deadline, not when the provider's block would have ended.
    assert returned_at - deadline < _host(0.2)

    replay_request = SourceBrokerV2ReplayRequest(
        saga_id=finalize_request.saga_id,
        operation_id=finalize_request.operation_id,
        phase=SourceBrokerV2OutboxPhase.SOURCE_FINALIZE,
        operation_request_hash=finalize_request.request_hash,
        challenge="8" * 64,
    )
    replay = SourceBrokerV2ReplayResponse.model_validate_json(
        service.replay(canonical_model_json_bytes(replay_request))
    )
    assert replay.status is SourceBrokerV2ReplayStatus.UNKNOWN

    with pytest.raises((SourceBrokerTransportError, SourceBrokerV2TransportDeadlineError)):
        service.finalize(envelope, deadline=time.monotonic() + _host(1.0))
    assert provider.finalize_calls == 1


def test_provider_finalize_error_requires_reconcile_without_auto_repeat(
    tmp_path: Path,
) -> None:
    provider = _CountingProvider(fail_finalize_once=True)
    service, keyring = _service(tmp_path, provider=provider)
    request = _dispatch_request()
    claim = _claim(service, _claim_once_request(request))
    dispatch = SourceBrokerV2DispatchResponse.model_validate_json(
        service.dispatch(
            canonical_model_json_bytes(
                SourceBrokerV2DispatchEnvelope(request=request, claim_receipt=claim)
            )
        )
    )
    finalize_request = _finalize_request(dispatch)
    finalize_claim_request = _claim_once_request(
        _dispatch_request(operation_id=finalize_request.operation_id),
        challenge=HASHES["final_challenge"],
        phase=SourceBrokerV2OutboxPhase.SOURCE_FINALIZE,
        operation_request_hash=finalize_request.request_hash,
    )
    finalize_claim = _claim(service, finalize_claim_request)
    keyring.require_verified_claim(request=finalize_claim_request, receipt=finalize_claim)
    envelope = canonical_model_json_bytes(
        SourceBrokerV2FinalizeEnvelope(
            request=finalize_request,
            claim_receipt=finalize_claim,
        )
    )

    with pytest.raises(SourceBrokerTransportError, match="reconcile|required|unknown"):
        service.finalize(envelope)

    replay_request = SourceBrokerV2ReplayRequest(
        saga_id=finalize_request.saga_id,
        operation_id=finalize_request.operation_id,
        phase=SourceBrokerV2OutboxPhase.SOURCE_FINALIZE,
        operation_request_hash=finalize_request.request_hash,
        challenge="9" * 64,
    )
    replay = SourceBrokerV2ReplayResponse.model_validate_json(
        service.replay(canonical_model_json_bytes(replay_request))
    )
    keyring.require_verified_replay(request=replay_request, receipt=replay)
    assert replay.status is SourceBrokerV2ReplayStatus.UNKNOWN

    with pytest.raises(SourceBrokerTransportError, match="reconcile|required|unknown"):
        service.finalize(envelope)
    assert provider.finalize_calls == 1


def test_locked_unknown_fence_blocks_duplicates_and_reconciles_after_release(
    tmp_path: Path,
) -> None:
    # Three quantities decide this case, and - as the starved-signing pair above
    # already had to learn - they only work as a set:
    #
    #   prologue  <  fence refusal  <  busy timeout
    #
    # The middle term is the subject: the service must refuse the moment it
    # finds the fence unwritable, *not* sit on the lock until the busy timeout
    # gives up. The guard below is what tells those two apart, and it can only
    # do that while it sits strictly between them. The busy timeout was the one
    # term of the three pinned to a bare literal while the other two scale with
    # the host, so on a host slow enough for `_host()` to matter the guard drifts
    # up past it and silently stops discriminating - at scale 2.5 the guard is
    # already 0.5s and a service that waited out the whole 500ms would pass.
    # All three scale together now, so both orderings hold at every host speed.
    busy_timeout_seconds = _host(0.5)
    refusal_guard_seconds = _host(0.2)
    ledger_path = tmp_path / "source-provider-ledger.sqlite3"
    provider = _LedgerLockingFailProvider(ledger_path=ledger_path)
    service, keyring = _service(
        tmp_path,
        provider=provider,
        busy_timeout_ms=max(1, round(busy_timeout_seconds * 1000)),
    )
    request = _dispatch_request()
    claim = _claim(service, _claim_once_request(request))
    envelope = canonical_model_json_bytes(
        SourceBrokerV2DispatchEnvelope(request=request, claim_receipt=claim)
    )
    replay_request = SourceBrokerV2ReplayRequest(
        saga_id=request.saga_id,
        operation_id=request.operation_id,
        phase=SourceBrokerV2OutboxPhase.DISPATCH,
        operation_request_hash=request.request_hash,
        challenge="4" * 64,
    )

    started = time.monotonic()
    try:
        # The refusal has to name itself. `SourceBrokerV2TransportDeadlineError`
        # subclasses `SourceBrokerTransportError`, so the old two-element tuple
        # was just `SourceBrokerTransportError` with extra words and it accepted
        # a deadline expiry as if it were the fence refusal the case is named
        # for - whether that expiry came from the prologue outrunning the budget
        # or from the service waiting on the lock. Neither of those messages
        # contains reconcile, required or unknown, so the stage is pinned here
        # rather than inferred from the stopwatch two lines down.
        #
        # The budget is one-sided and therefore gets the one-sided constant.
        # Measured, not assumed: what ends this call is the unwritable fence,
        # not the deadline, so enlarging the budget from 50ms to 5s leaves the
        # call at the same 15-25ms and leaves every assertion below untouched.
        # All it has to do is survive the prologue, which the 50ms literal
        # stopped doing at 2.9x its calibrated cost.
        with pytest.raises(SourceBrokerTransportError, match="reconcile|required|unknown"):
            service.dispatch(
                envelope,
                deadline=time.monotonic() + _host(_BLOCKED_PROVIDER_DEADLINE_SECONDS),
            )
        returned_at = time.monotonic()
        # Stated rather than inferred: the fence is unwritable *because the
        # provider is holding the ledger lock*, and that precondition was until
        # now taken on trust. `_LedgerLockingFailProvider` sets this only after
        # `BEGIN IMMEDIATE` has succeeded, so it witnesses the lock, not just
        # the entry that `dispatch_calls` already covers.
        assert provider.locked.is_set()
        assert returned_at - started < refusal_guard_seconds
        assert provider.dispatch_calls == 1

        replay = SourceBrokerV2ReplayResponse.model_validate_json(
            service.replay(canonical_model_json_bytes(replay_request))
        )
        keyring.require_verified_replay(request=replay_request, receipt=replay)
        assert replay.status is SourceBrokerV2ReplayStatus.UNKNOWN

        duplicate_started = time.monotonic()
        # Scaled along with the busy timeout above: these two only have to
        # outlast a refusal, but leaving them as literals while the busy timeout
        # grows would invert `busy timeout < deadline` on a slow host and change
        # which of the two ends the call.
        with pytest.raises(SourceBrokerTransportError, match="reconcile|required|unknown"):
            service.dispatch(envelope, deadline=time.monotonic() + _host(1.0))
        assert time.monotonic() - duplicate_started < refusal_guard_seconds
        assert provider.dispatch_calls == 1
    finally:
        provider.close()

    with pytest.raises(SourceBrokerTransportError, match="reconcile|required|unknown"):
        service.dispatch(envelope, deadline=time.monotonic() + _host(1.0))
    assert _ledger_row(tmp_path, request.operation_id)["status"] == "reconcile_required"
    assert provider.dispatch_calls == 1


def test_provider_result_after_deadline_requires_reconcile_not_terminal_replay(
    tmp_path: Path,
) -> None:
    # "After the deadline" used to be arithmetic - an 80ms provider sleep against
    # a 40ms deadline - and arithmetic on two wall clock quantities is a race on
    # a third, the prologue, that is in neither of them. On a 3.12 shard the
    # prologue ate the 40ms and the case failed with `dispatch_calls == 0`: the
    # provider was never reached, so nothing arrived after the deadline and the
    # property was not exercised at all.
    #
    # Here the ordering is made, not hoped for. The provider blocks until this
    # case releases it, so the deadline necessarily expires while the provider
    # is still inside; the release happens only after that refusal has been
    # observed, so the successful result it then produces is *by construction*
    # late. The deadline's size stops mattering, which is what lets it be sized
    # to survive the prologue instead of to lose a race with it.
    late_result = threading.Event()
    events: list[object] = []

    def record(event: object) -> None:
        events.append(event)
        if getattr(event, "category", None) == "provider_late_outcome":
            late_result.set()

    provider = _BlockingProvider()
    service, _keyring = _service(
        tmp_path, provider=provider, busy_timeout_ms=500, event_sink=record
    )
    request = _dispatch_request()
    claim = _claim(service, _claim_once_request(request))
    envelope = canonical_model_json_bytes(
        SourceBrokerV2DispatchEnvelope(request=request, claim_receipt=claim)
    )

    try:
        with pytest.raises(SourceBrokerV2TransportDeadlineError, match="during provider dispatch"):
            service.dispatch(
                envelope,
                deadline=time.monotonic() + _host(_BLOCKED_PROVIDER_DEADLINE_SECONDS),
            )
        assert provider.entered.is_set()
    finally:
        provider.release.set()
    # The subject is a result that *arrived* late, so wait for it to actually
    # arrive before asking what the service did with it. The old form asserted
    # over a provider that was very likely still sleeping, which is how a case
    # can pass without its subject ever existing. The budget is the provider's
    # own derived cap: after the release the result is one function return away,
    # so this is a hang guard, not a race.
    assert late_result.wait(timeout=provider.block_seconds)
    assert any(
        getattr(event, "category", None) == "provider_late_outcome"
        and getattr(event, "outcome", None) == "success"
        for event in events
    )

    replay_request = SourceBrokerV2ReplayRequest(
        saga_id=request.saga_id,
        operation_id=request.operation_id,
        phase=SourceBrokerV2OutboxPhase.DISPATCH,
        operation_request_hash=request.request_hash,
        challenge="9" * 64,
    )
    replay = SourceBrokerV2ReplayResponse.model_validate_json(
        service.replay(canonical_model_json_bytes(replay_request))
    )
    assert replay.status is SourceBrokerV2ReplayStatus.UNKNOWN
    with pytest.raises(SourceBrokerTransportError, match="reconcile|required|unknown"):
        service.dispatch(envelope, deadline=time.monotonic() + _host(1.0))
    assert provider.dispatch_calls == 1


def test_claim_signing_uses_remaining_single_request_deadline(tmp_path: Path) -> None:
    signer = _DeadlineAwareTestSigner(required_seconds=_host(_SIGNING_COST_SECONDS))
    service, _keyring = _service(tmp_path, signer=signer)
    request = _claim_once_request(_dispatch_request())

    started = time.monotonic()
    with pytest.raises(SourceBrokerV2TransportDeadlineError, match="authority signing"):
        service.claim_once(
            canonical_model_json_bytes(request),
            deadline=time.monotonic() + _host(_SIGNING_STARVED_DEADLINE_SECONDS),
        )

    assert time.monotonic() - started < _host(0.08)
    assert len(signer.seen_deadlines) == 1
    assert signer.seen_deadlines[0] is not None


def test_replay_signing_uses_remaining_single_request_deadline(tmp_path: Path) -> None:
    signer = _DeadlineAwareTestSigner(required_seconds=_host(_SIGNING_COST_SECONDS))
    provider = _CountingProvider()
    service, _keyring = _service(tmp_path, provider=provider, signer=signer)
    request = SourceBrokerV2ReplayRequest(
        saga_id="saga-source-v2",
        operation_id=source_effect_operation_id(
            saga_id="saga-source-v2",
            phase=SourceBrokerV2OutboxPhase.DISPATCH,
        ),
        phase=SourceBrokerV2OutboxPhase.DISPATCH,
        operation_request_hash="d" * 64,
        challenge="e" * 64,
    )

    started = time.monotonic()
    with pytest.raises(SourceBrokerV2TransportDeadlineError, match="authority signing"):
        service.replay(
            canonical_model_json_bytes(request),
            deadline=time.monotonic() + _host(_SIGNING_STARVED_DEADLINE_SECONDS),
        )

    assert time.monotonic() - started < _host(0.08)
    assert len(signer.seen_deadlines) == 1
    assert signer.seen_deadlines[0] is not None
    assert provider.dispatch_calls == 0
    assert provider.finalize_calls == 0


def test_concurrent_duplicate_dispatches_share_one_durable_result(tmp_path: Path) -> None:
    provider = _CountingProvider(sleep_dispatch_seconds=_host(0.05))
    service, _keyring = _service(tmp_path, provider=provider)
    request = _dispatch_request()
    claim = _claim(service, _claim_once_request(request))
    envelope = canonical_model_json_bytes(
        SourceBrokerV2DispatchEnvelope(request=request, claim_receipt=claim)
    )
    results: list[bytes] = []
    errors: list[BaseException] = []

    def invoke() -> None:
        try:
            results.append(service.dispatch(envelope))
        except BaseException as exc:  # pragma: no cover - failure path asserted below
            errors.append(exc)

    threads = [threading.Thread(target=invoke) for _ in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=5)

    assert all(not thread.is_alive() for thread in threads)
    assert not errors
    assert len(results) == 8
    assert len(set(results)) == 1
    assert results == [results[0]] * 8
    assert provider.dispatch_calls == 1


def test_claim_and_replay_can_rotate_from_current_to_next_key(tmp_path: Path) -> None:
    provider = _CountingProvider()
    external_authority = _FakeExternalDispatchAuthority()
    service, old_keyring = _service(
        tmp_path,
        provider=provider,
        key_id="current",
        extra_key_id="next",
        external_authority=external_authority,
    )
    request = _dispatch_request()
    claim_request = _claim_once_request(request)
    old_claim = _claim(service, claim_request)
    old_keyring.require_verified_claim(request=claim_request, receipt=old_claim)
    assert old_claim.key_id == "current"

    dispatch = service.dispatch(
        canonical_model_json_bytes(
            SourceBrokerV2DispatchEnvelope(request=request, claim_receipt=old_claim)
        )
    )
    next_private, next_public = _keypair(tmp_path / "keys-next-active", "next")
    current_public = old_keyring._public_keys["current"]  # noqa: SLF001
    rotated_keyring = SourceAuthorityKeyring(
        expected_authority_id="source-authority",
        allowed_public_keys={"current": current_public, "next": next_public},
        expected_purpose=SOURCE_BROKER_V2_AUTHORITY_PURPOSE,
        expected_schema_version=2,
    )
    from rquant.source_broker_v2_service import (
        OpenSslSourceBrokerV2AuthoritySigner,
        SourceBrokerV2ProviderService,
    )

    rotated = SourceBrokerV2ProviderService.create_for_test(
        ledger_path=tmp_path / "source-provider-ledger.sqlite3",
        provider=provider,
        authority_signer=OpenSslSourceBrokerV2AuthoritySigner(
            authority_id="source-authority",
            key_id="next",
            private_key_path=next_private,
        ),
        authority_keyring=rotated_keyring,
        external_dispatch_authority=external_authority,
        clock=lambda: NOW,
        profile="nonproduction",
    )
    replay_request = SourceBrokerV2ReplayRequest(
        saga_id=request.saga_id,
        operation_id=request.operation_id,
        phase=SourceBrokerV2OutboxPhase.DISPATCH,
        operation_request_hash=request.request_hash,
        challenge="a" * 64,
    )
    replay = SourceBrokerV2ReplayResponse.model_validate_json(
        rotated.replay(canonical_model_json_bytes(replay_request))
    )
    rotated_keyring.require_verified_replay(request=replay_request, receipt=replay)
    assert replay.key_id == "next"
    assert replay.result == dispatch
    assert _claim(rotated, claim_request).key_id == "next"


@pytest.mark.parametrize(
    ("mutation", "message"),
    [
        ("duplicate", "duplicate|canonical|malformed"),
        ("extra", "validation|extra|canonical|malformed"),
        ("nonfinite", "non-finite|canonical|malformed|invalid"),
        ("wrong-payload-hash", "payload hash|validation|canonical|malformed"),
        ("oversize", "size|too large|frame"),
        ("forbidden-provider-object", "provider|extra|validation|canonical|malformed"),
    ],
)
def test_wire_decoder_rejects_ambiguous_or_forbidden_requests(
    tmp_path: Path,
    mutation: Literal[
        "duplicate",
        "extra",
        "nonfinite",
        "wrong-payload-hash",
        "oversize",
        "forbidden-provider-object",
    ],
    message: str,
) -> None:
    from rquant.source_broker_v2_service import (
        SOURCE_BROKER_V2_MAX_WIRE_BYTES,
        decode_v2_wire_request,
    )

    payload = canonical_model_json_bytes(_claim_once_request(_dispatch_request()))
    request = SourceBrokerV2WireRequest(
        operation="claim_once",
        challenge=HASHES["challenge"],
        payload=payload,
        payload_hash=_hash_payload(payload),
    )
    raw = canonical_model_json_bytes(request)
    if mutation == "duplicate":
        raw = (
            b'{"challenge":"'
            + HASHES["challenge"].encode()
            + b'","challenge":"'
            + HASHES["challenge"].encode()
            + b'","contract":"rquant-source-broker-unix-request/v2","operation":"claim_once",'
            + b'"payload":"AA==","payload_hash":"'
            + ("0" * 64).encode()
            + b'","schema_version":2}'
        )
    elif mutation == "extra":
        raw = raw[:-1] + b',"callback":"python:evil"}'
    elif mutation == "nonfinite":
        raw = (
            b'{"challenge":"'
            + HASHES["challenge"].encode()
            + b'","contract":"rquant-source-broker-unix-request/v2",'
            + b'"operation":"claim_once","payload":"AA==","payload_hash":"'
            + ("0" * 64).encode()
            + b'","schema_version":NaN}'
        )
    elif mutation == "wrong-payload-hash":
        raw = raw.replace(request.payload_hash.encode(), ("f" * 64).encode())
    elif mutation == "oversize":
        raw = b"{" + (b'"a":1,' * (SOURCE_BROKER_V2_MAX_WIRE_BYTES // 3)) + b'"z":1}'
    elif mutation == "forbidden-provider-object":
        forbidden = canonical_json_bytes(
            {
                **_claim_once_request(_dispatch_request()).model_dump(mode="json"),
                "provider": {"pickle": "gASV"},
            }
        )
        raw = canonical_model_json_bytes(
            SourceBrokerV2WireRequest(
                operation="claim_once",
                challenge=HASHES["challenge"],
                payload=forbidden,
                payload_hash=_hash_payload(forbidden),
            )
        )

    with pytest.raises(SourceBrokerTransportError, match=message):
        decoded = decode_v2_wire_request(raw)
        decoded.parse_payload()


class _FakeSqliteCursor:
    def __init__(
        self,
        *,
        one: tuple[object, ...] | None = None,
        rows: list[dict[str, object]] | None = None,
    ) -> None:
        self._one = one
        self._rows = rows or []
        self.rowcount = 0

    def fetchone(self) -> tuple[object, ...] | None:
        return self._one

    def fetchall(self) -> list[dict[str, object]]:
        return self._rows


class _FakePragmaConnection:
    def __init__(self, *, journal_mode: str, synchronous: int) -> None:
        self.journal_mode = journal_mode
        self.synchronous = synchronous
        self.closed = False
        self.row_factory: object | None = None

    def execute(self, sql: str, _params: object = ()) -> _FakeSqliteCursor:
        normalized = " ".join(sql.strip().lower().split())
        if normalized == "pragma journal_mode=wal":
            return _FakeSqliteCursor(one=(self.journal_mode,))
        if normalized == "pragma synchronous=full":
            return _FakeSqliteCursor()
        if normalized == "pragma synchronous":
            return _FakeSqliteCursor(one=(self.synchronous,))
        if normalized.startswith("pragma table_info("):
            return _FakeSqliteCursor(
                rows=[
                    {"name": "invocation_owner_token"},
                    {"name": "invocation_owner_pid"},
                ]
            )
        return _FakeSqliteCursor()

    def close(self) -> None:
        self.closed = True

    def __enter__(self) -> _FakePragmaConnection:
        return self

    def __exit__(self, *_args: object) -> None:
        self.close()


class _RecordingSqliteConnection:
    def __init__(self, connection: sqlite3.Connection) -> None:
        self._connection = connection
        self.statements: list[str] = []
        self.connect_timeout: float | None = None

    @property
    def row_factory(self) -> object:
        return self._connection.row_factory

    @row_factory.setter
    def row_factory(self, value: object) -> None:
        self._connection.row_factory = value

    def execute(self, sql: str, params: object = ()) -> sqlite3.Cursor:
        self.statements.append(" ".join(sql.strip().lower().split()))
        return self._connection.execute(sql, params)

    def commit(self) -> None:
        self._connection.commit()

    def rollback(self) -> None:
        self._connection.rollback()

    def close(self) -> None:
        self._connection.close()

    def __enter__(self) -> _RecordingSqliteConnection:
        return self

    def __exit__(self, *_args: object) -> None:
        self.close()


@pytest.mark.parametrize(
    ("journal_mode", "synchronous"),
    [
        ("delete", 2),
        ("wal", 1),
    ],
)
def test_sqlite_ledger_fails_closed_when_required_pragmas_are_not_active(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    journal_mode: str,
    synchronous: int,
) -> None:
    import rquant.source_broker_v2_service as service_module

    fake = _FakePragmaConnection(journal_mode=journal_mode, synchronous=synchronous)
    monkeypatch.setattr(service_module.sqlite3, "connect", lambda *_args, **_kwargs: fake)

    with pytest.raises(SourceBrokerTransportError, match="WAL|FULL"):
        _service(tmp_path, signer=_DeadlineAwareTestSigner(required_seconds=0.0))

    assert fake.closed


def test_sqlite_ledger_uses_wal_full_sync_busy_timeout_and_unique_operation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import rquant.source_broker_v2_service as service_module

    real_connect = sqlite3.connect
    connections: list[_RecordingSqliteConnection] = []

    def recording_connect(*args: object, **kwargs: object) -> _RecordingSqliteConnection:
        connection = _RecordingSqliteConnection(real_connect(*args, **kwargs))
        timeout = kwargs.get("timeout")
        if isinstance(timeout, int | float):
            connection.connect_timeout = float(timeout)
        connections.append(connection)
        return connection

    monkeypatch.setattr(service_module.sqlite3, "connect", recording_connect)

    service, _keyring = _service(tmp_path)
    request = _claim_once_request(_dispatch_request())
    _claim(service, request)

    ledger_path = tmp_path / "source-provider-ledger.sqlite3"
    with real_connect(ledger_path) as legacy:
        legacy.execute("PRAGMA foreign_keys=OFF")
        legacy.execute("DROP TABLE source_broker_v2_provider_claim_attempt")
        legacy.execute(
            "ALTER TABLE source_broker_v2_provider_operation DROP COLUMN active_claim_attempt_id"
        )
    _migrated, _migrated_keyring = _service(tmp_path)

    statements = [statement for connection in connections for statement in connection.statements]
    with real_connect(ledger_path) as connection:
        unique_indexes = connection.execute(
            "SELECT sql FROM sqlite_master WHERE type='index' AND sql LIKE '%operation_id%'"
        ).fetchall()
        migrated = connection.execute(
            "SELECT active_claim_attempt_id FROM source_broker_v2_provider_operation "
            "WHERE operation_id = ?",
            (request.operation_id,),
        ).fetchone()
        attempts = connection.execute(
            "SELECT attempt_id FROM source_broker_v2_provider_claim_attempt "
            "WHERE effect_operation_id = ?",
            (request.operation_id,),
        ).fetchall()

    assert any(statement == "pragma journal_mode=wal" for statement in statements)
    assert any(statement == "pragma synchronous=full" for statement in statements)
    assert any(statement == "pragma synchronous" for statement in statements)
    assert any(statement == "pragma busy_timeout=5000" for statement in statements)
    assert any(connection.connect_timeout == 5.0 for connection in connections)
    assert unique_indexes
    assert migrated is not None
    assert attempts == [migrated]


def test_darwin_or_missing_so_peercred_fails_before_socket_creation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import rquant.source_broker_v2_server as server_module
    from rquant.source_broker_v2_server import SourceBrokerV2UnixService

    provider_service, _keyring = _service(tmp_path)
    endpoint = SocketEndpointPolicy(
        path=tmp_path / "source-v2.sock",
        owner_uid=os.getuid(),
        group_gid=os.getgid(),
        mode=0o600,
    )
    monkeypatch.setattr(server_module, "source_broker_peer_credentials_supported", lambda: False)

    def fail_socket(*_args: object, **_kwargs: object) -> socket.socket:
        raise AssertionError("socket must not be created when peer credentials are unavailable")

    monkeypatch.setattr(server_module.socket, "socket", fail_socket)
    service = SourceBrokerV2UnixService(
        endpoint=endpoint,
        peer_policy=PeerCredentialsPolicy(
            allowed_uids=frozenset({os.getuid()}),
            allowed_gids=frozenset({os.getgid()}),
        ),
        provider_service=provider_service,
    )
    with pytest.raises(SourceBrokerTransportError, match="SO_PEERCRED"):
        service.serve_forever(max_connections=1)
    assert not endpoint.path.exists()


def test_socket_directory_authority_rejects_renamed_ancestor_symlink_backreference(
    tmp_path: Path,
) -> None:
    import rquant.source_broker_v2_server as server_module

    ancestor = tmp_path / "authority-root"
    socket_parent = ancestor / "run" / "source"
    socket_parent.mkdir(parents=True)
    endpoint = SocketEndpointPolicy(
        path=socket_parent / "source-v2.sock",
        owner_uid=os.getuid(),
        group_gid=os.getgid(),
        mode=0o600,
    )
    directory = server_module._open_directory_authority(endpoint)  # noqa: SLF001
    renamed = tmp_path / "authority-root-renamed"
    try:
        ancestor.rename(renamed)
        ancestor.symlink_to(renamed, target_is_directory=True)

        with pytest.raises(SourceBrokerTransportError, match="ancestor|directory authority"):
            server_module._validate_directory_authority(  # noqa: SLF001
                directory,
                endpoint,
            )
    finally:
        os.close(directory.fd)


def test_wrong_peer_is_rejected_before_any_request_bytes_are_read(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import rquant.source_broker_v2_server as server_module
    from rquant.source_broker_v2_server import SourceBrokerV2UnixService

    events: list[object] = []
    provider_service, _keyring = _service(tmp_path, event_sink=events.append)
    server, client = socket.socketpair()
    read_calls = 0
    original_read = server_module.read_frame_before_deadline

    def counted_read(*args: object, **kwargs: object) -> bytes:
        nonlocal read_calls
        read_calls += 1
        return original_read(*args, **kwargs)

    monkeypatch.setattr(server_module, "read_frame_before_deadline", counted_read)
    monkeypatch.setattr(server_module, "_kernel_peer_credentials", lambda _socket: (123, 999, 999))
    service = SourceBrokerV2UnixService(
        endpoint=SocketEndpointPolicy(
            path=tmp_path / "unused.sock",
            owner_uid=os.getuid(),
            group_gid=os.getgid(),
            mode=0o600,
        ),
        peer_policy=PeerCredentialsPolicy(
            allowed_uids=frozenset({os.getuid()}),
            allowed_gids=frozenset({os.getgid()}),
        ),
        provider_service=provider_service,
    )
    try:
        service._serve_connection(server)
    finally:
        client.close()

    assert read_calls == 0
    peer_events = [event for event in events if event.category == "peer_rejected"]
    assert len(peer_events) == 1
    assert peer_events[0].phase == "transport"
    assert peer_events[0].operation_hash is None
    assert peer_events[0].reconcile is False


def test_server_deadline_closes_half_read_without_provider_call(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import rquant.source_broker_v2_server as server_module
    from rquant.source_broker_v2_server import SourceBrokerV2UnixService

    provider = _CountingProvider()
    external_authority = _FakeExternalDispatchAuthority()
    events: list[object] = []
    provider_service, _keyring = _service(
        tmp_path,
        provider=provider,
        external_authority=external_authority,
        event_sink=events.append,
    )
    server, client = socket.socketpair()
    monkeypatch.setattr(
        server_module,
        "_kernel_peer_credentials",
        lambda _socket: (123, os.getuid(), os.getgid()),
    )
    service = SourceBrokerV2UnixService(
        endpoint=SocketEndpointPolicy(
            path=tmp_path / "unused.sock",
            owner_uid=os.getuid(),
            group_gid=os.getgid(),
            mode=0o600,
        ),
        peer_policy=PeerCredentialsPolicy(
            allowed_uids=frozenset({os.getuid()}),
            allowed_gids=frozenset({os.getgid()}),
        ),
        provider_service=provider_service,
        total_request_deadline_seconds=0.05,
    )
    client.sendall((64).to_bytes(4, "big") + b"{")
    thread = threading.Thread(target=service._serve_connection, args=(server,), daemon=True)
    thread.start()
    thread.join(timeout=2)

    client.settimeout(0.5)
    with pytest.raises((BrokenPipeError, ConnectionResetError, TimeoutError, OSError)):
        client.sendall(b"x" * 1024 * 1024)
        client.recv(1)
    client.close()
    assert not thread.is_alive()
    assert provider.dispatch_calls == 0
    transport_events = [event for event in events if event.category == "transport_error"]
    assert len(transport_events) == 1
    assert transport_events[0].phase == "transport"
    assert transport_events[0].exception_class == "SourceBrokerTransportError"


def test_response_write_deadline_keeps_durable_dispatch_replayable(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import rquant.source_broker_v2_server as server_module
    from rquant.source_broker_v2_server import SourceBrokerV2UnixService

    provider = _CountingProvider()
    external_authority = _FakeExternalDispatchAuthority()
    events: list[object] = []
    provider_service, _keyring = _service(
        tmp_path,
        provider=provider,
        external_authority=external_authority,
        event_sink=events.append,
    )
    request = _dispatch_request()
    claim = _claim(provider_service, _claim_once_request(request))
    envelope = canonical_model_json_bytes(
        SourceBrokerV2DispatchEnvelope(request=request, claim_receipt=claim)
    )
    wire_request = SourceBrokerV2WireRequest(
        operation="dispatch",
        challenge="2" * 64,
        payload=envelope,
        payload_hash=_hash_payload(envelope),
    )
    raw_wire = canonical_model_json_bytes(wire_request)
    server, client = socket.socketpair()
    monkeypatch.setattr(
        server_module,
        "_kernel_peer_credentials",
        lambda _socket: (123, os.getuid(), os.getgid()),
    )
    write_calls = 0

    def drop_write(_connection: socket.socket, _payload: bytes, *, deadline: float) -> None:
        nonlocal write_calls
        assert deadline > time.monotonic()
        write_calls += 1
        raise SourceBrokerV2TransportDeadlineError(
            "V2 source broker server deadline expired before response write"
        )

    monkeypatch.setattr(server_module, "write_frame_before_deadline", drop_write)
    service = SourceBrokerV2UnixService(
        endpoint=SocketEndpointPolicy(
            path=tmp_path / "unused.sock",
            owner_uid=os.getuid(),
            group_gid=os.getgid(),
            mode=0o600,
        ),
        peer_policy=PeerCredentialsPolicy(
            allowed_uids=frozenset({os.getuid()}),
            allowed_gids=frozenset({os.getgid()}),
        ),
        provider_service=provider_service,
    )

    client.sendall(len(raw_wire).to_bytes(4, "big") + raw_wire)
    thread = threading.Thread(target=service._serve_connection, args=(server,), daemon=True)
    thread.start()
    thread.join(timeout=2)
    client.close()

    assert not thread.is_alive()
    assert write_calls == 1
    assert provider.dispatch_calls == 1
    assert external_authority.reserve_calls == 1
    assert external_authority.complete_calls == 1
    replay_request = SourceBrokerV2ReplayRequest(
        saga_id=request.saga_id,
        operation_id=request.operation_id,
        phase=SourceBrokerV2OutboxPhase.DISPATCH,
        operation_request_hash=request.request_hash,
        challenge="3" * 64,
    )
    replay = SourceBrokerV2ReplayResponse.model_validate_json(
        provider_service.replay(canonical_model_json_bytes(replay_request))
    )
    assert replay.status is SourceBrokerV2ReplayStatus.FOUND
    duplicated = provider_service.dispatch(envelope)
    assert duplicated == replay.result
    assert provider.dispatch_calls == 1
    write_events = [event for event in events if event.category == "write_error"]
    assert len(write_events) == 1
    assert write_events[0].operation_hash != request.operation_id
    assert write_events[0].exception_class == "SourceBrokerV2TransportDeadlineError"


@pytest.mark.parametrize(
    ("frame", "message"),
    [
        ((0).to_bytes(4, "big"), "size"),
        ((MAX_SOURCE_BROKER_FRAME_BYTES + 1).to_bytes(4, "big"), "size"),
        (b"\x00\x00", "truncated|read failed"),
        ((4).to_bytes(4, "big") + b"ab", "truncated"),
    ],
)
def test_raw_frame_reader_rejects_zero_oversize_and_truncated_frames(
    frame: bytes,
    message: str,
) -> None:
    from rquant.source_broker_v2_server import read_frame_before_deadline

    server, client = socket.socketpair()
    try:
        client.sendall(frame)
        client.close()
        with pytest.raises(SourceBrokerTransportError, match=message):
            read_frame_before_deadline(server, deadline=time.monotonic() + 0.2)
    finally:
        server.close()


def test_graceful_stop_cleanup_leaves_replaced_endpoint_untouched(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import sys

    if sys.platform == "darwin":
        pytest.skip("macOS sandbox refuses this AF_UNIX listener bind; Linux E2E covers it")

    import rquant.source_broker_v2_server as server_module
    from rquant.source_broker_v2_server import SourceBrokerV2UnixService

    provider_service, _keyring = _service(tmp_path)
    short_root = Path(tempfile.mkdtemp(prefix="rqv2-", dir="/tmp")).resolve()
    endpoint = SocketEndpointPolicy(
        path=short_root / "stop.sock",
        owner_uid=os.getuid(),
        group_gid=os.getgid(),
        mode=0o600,
    )
    stop = threading.Event()
    monkeypatch.setattr(server_module, "source_broker_peer_credentials_supported", lambda: True)
    service = SourceBrokerV2UnixService(
        endpoint=endpoint,
        peer_policy=PeerCredentialsPolicy(
            allowed_uids=frozenset({os.getuid()}),
            allowed_gids=frozenset({os.getgid()}),
        ),
        provider_service=provider_service,
        accept_timeout_seconds=0.05,
    )
    thread = threading.Thread(target=service.serve_forever, kwargs={"stop": stop}, daemon=True)
    try:
        thread.start()
        assert service.ready.wait(timeout=2)

        replacement = short_root / "replacement-target"
        replacement.write_text("replacement", encoding="utf-8")
        endpoint.path.unlink()
        endpoint.path.symlink_to(replacement)
        stop.set()
        thread.join(timeout=2)

        assert not thread.is_alive()
        assert endpoint.path.is_symlink()
        assert endpoint.path.resolve() == replacement
    finally:
        stop.set()
        thread.join(timeout=1)
        shutil.rmtree(short_root, ignore_errors=True)


def test_stale_endpoint_probe_refuses_to_unlink_live_listener() -> None:
    import sys

    if sys.platform == "darwin":
        pytest.skip("macOS sandbox refuses this AF_UNIX listener bind; Linux E2E covers it")

    import rquant.source_broker_v2_server as server_module

    short_root = Path(tempfile.mkdtemp(prefix="rqv2-", dir="/tmp")).resolve()
    endpoint = SocketEndpointPolicy(
        path=short_root / "live.sock",
        owner_uid=os.getuid(),
        group_gid=os.getgid(),
        mode=0o600,
    )
    listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    listener.bind(str(endpoint.path))
    os.chmod(endpoint.path, endpoint.mode)
    os.chown(endpoint.path, endpoint.owner_uid, endpoint.group_gid, follow_symlinks=False)
    listener.listen(1)
    listener.settimeout(1)
    directory = server_module._open_directory_authority(endpoint)  # noqa: SLF001
    try:
        with pytest.raises(SourceBrokerTransportError, match="live|stale|refused"):
            server_module._unlink_stale_endpoint(directory, endpoint)  # noqa: SLF001
        probed, _address = listener.accept()
        probed.close()
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
            client.settimeout(1)
            client.connect(str(endpoint.path))
            accepted, _address = listener.accept()
            accepted.close()
    finally:
        os.close(directory.fd)
        listener.close()
        endpoint.path.unlink(missing_ok=True)
        shutil.rmtree(short_root)


def test_openssl_signer_matches_source_authority_payload(tmp_path: Path) -> None:
    from rquant.source_broker_v2_service import OpenSslSourceBrokerV2AuthoritySigner

    private_key, public_key = _keypair(tmp_path / "keys", "current")
    signer = OpenSslSourceBrokerV2AuthoritySigner(
        authority_id="source-authority",
        key_id="current",
        private_key_path=private_key,
    )
    payload = b'{"canonical":true}'
    signature = signer.sign(payload)
    payload_path = tmp_path / "payload.bin"
    sig_path = tmp_path / "sig.bin"
    payload_path.write_bytes(source_authority_signature_payload(payload))
    sig_path.write_bytes(base64.b64decode(signature))
    public_path = tmp_path / "public.pem"
    public_path.write_bytes(public_key)
    verified = subprocess.run(
        (
            _openssl(),
            "pkeyutl",
            "-verify",
            "-pubin",
            "-inkey",
            str(public_path),
            "-rawin",
            "-in",
            str(payload_path),
            "-sigfile",
            str(sig_path),
        ),
        check=False,
        capture_output=True,
    )
    assert verified.returncode == 0, verified.stderr.decode("utf-8", errors="replace")
