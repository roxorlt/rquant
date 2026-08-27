from __future__ import annotations

import base64
import multiprocessing
import os
import shutil
import socket
import sqlite3
import subprocess
import tempfile
import time
from collections.abc import Callable, Iterator
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack, suppress
from datetime import UTC, datetime, timedelta, tzinfo
from pathlib import Path
from queue import Empty
from threading import Barrier, Event, Lock, Thread, current_thread
from threading import enumerate as enumerate_threads
from typing import Any

import pytest

import rquant.source_broker_v2 as source_broker_v2_module
from rquant.runtime_contracts import canonical_sha256
from rquant.source_broker import ReplayLineageCheckpointReceipt
from rquant.source_broker_protocol import (
    ServerCredentialsPolicy,
    SocketEndpointPolicy,
    SourceBrokerTransportError,
    read_frame,
    write_frame,
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
    SourceBrokerV2Saga,
    SourceBrokerV2SagaConflictError,
    SourceBrokerV2SagaIntegrityError,
    SourceBrokerV2SagaReconcileRequiredError,
    SourceBrokerV2SagaRepairRequiredError,
    SourceBrokerV2SagaRequest,
    SourceBrokerV2SagaState,
    SourceBrokerV2SagaUnavailableError,
    SourceBrokerV2UnixClient,
    SourceBrokerV2WireRequest,
    SourceBrokerV2WireResponse,
    source_authority_signature_payload,
    source_claim_attempt_id,
    source_effect_operation_id,
)
from rquant.source_operation_contracts import CurrentClaimConsumptionV2
from rquant.source_quota_authority import SourceQuotaParentAuthority
from rquant.source_quota_broker_adapter import (
    SourceQuotaBrokerAdapterV2,
    SourceQuotaParentBindingV2,
)
from rquant.source_quota_store import SourceQuotaStore
from rquant.strict_json import (
    canonical_json_bytes,
    canonical_model_json_bytes,
    strict_canonical_json_loads,
)

from .test_adapter_manifest import Authorities, create_test_authorities
from .test_source_operation_contracts import (
    MemoryCurrentClaimAuthority,
    _claim,
    _unsigned_issue,
)

NOW = datetime(2026, 8, 5, 4, tzinfo=UTC)

# `now=` is the saga's logical clock and drives its timestamps. The source
# attempt's `max_external_deadline` is not on it: `_ensure_source_attempt` sets
# it to `datetime.now(UTC) + source_request_deadline_seconds` and
# `_mark_invoke_started` refuses the invocation once the real clock is past it,
# with `reconcile_reason='source invocation did not start before its persisted
# deadline'`. Both ends read the real clock, so the budget is a genuine
# real-time one - it just has to cover whatever the saga does in between: the
# transport call, the lineage publish and the ledger writes.
#
# `for_nonproduction` defaults it to 0.25s, which is a convenience for the cases
# whose subject *is* that budget; they set it smaller still (0.05-0.08) to make
# a takeover or a cooldown happen quickly. A case that is about something else
# inherits the default silently and is then gated on how fast the host runs the
# saga's internals: 49ms of the 250ms on this laptop, over 250ms inside a
# 33-minute shard on a 2 vCPU runner, where it turned a COMPLETE into a
# RECONCILE_REQUIRED. This value is for those cases - large enough that no host
# can outrun it, so the budget stops being a participant.
#
# Applied at every `for_nonproduction` site whose case is not about the budget.
# Two kinds of site are left on the default and must stay there:
#
#   - the eight cases that install a `_MutableUtcClock` over the module's
#     `datetime`. There the budget is already on a clock the case drives, and
#     the arithmetic between the deadline, the takeover boundary and
#     `clock.advance()` is the scenario - `..._persists_source_window_grant_...`
#     asserts `first_finalize_takeover <= clock.now()` outright.
#   - `_spawn_initialize_same_saga`, which builds a saga in a spawned worker to
#     inspect its schema and never advances it, so the budget is never read.
_UNCONSTRAINED_SOURCE_DEADLINE_SECONDS = 30.0
_UNCONSTRAINED_SOURCE_TAKEOVER_GRACE_SECONDS = 5.0


class _MutableUtcClock:
    def __init__(self, current: datetime) -> None:
        self.current = current

    def now(self, zone: tzinfo | None = UTC) -> datetime:
        if zone is None:
            return self.current.replace(tzinfo=None)
        return self.current.astimezone(zone)

    def advance(self, seconds: float) -> None:
        self.current += timedelta(seconds=seconds)

    def install_source_broker_clock(self, monkeypatch: pytest.MonkeyPatch) -> None:
        clock = self

        class FrozenDateTime(datetime):
            @classmethod
            def now(cls, zone: tzinfo | None = None) -> datetime:
                return clock.now(zone)

        monkeypatch.setattr(source_broker_v2_module, "datetime", FrozenDateTime)


def _short_unix_socket_directory() -> tempfile.TemporaryDirectory[str]:
    for root in (Path("/private/tmp"), Path("/tmp")):
        if root.is_dir() and not root.is_symlink():
            return tempfile.TemporaryDirectory(prefix="rqv2-", dir=root)
    raise RuntimeError("no safe POSIX temporary directory is available for Unix sockets")


class _SourceAuthorityTestSecurity:
    def __init__(
        self,
        *,
        authority_id: str = "source-authority-test",
        key_id: str = "source-authority-key-v2",
    ) -> None:
        self.authority_id = authority_id
        self.key_id = key_id
        executable = shutil.which("openssl")
        if executable is None:
            pytest.skip("openssl is required for SourceBroker v2 authority tests")
        self._openssl = executable
        self._sign_lock = Lock()
        self._directory = tempfile.TemporaryDirectory(prefix="rquant-source-authority-test-")
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
        self.public_key = public_key.read_bytes()
        self.keyring = SourceAuthorityKeyring(
            expected_authority_id=self.authority_id,
            allowed_public_keys={self.key_id: self.public_key},
            expected_purpose=SOURCE_BROKER_V2_AUTHORITY_PURPOSE,
            expected_schema_version=2,
        )

    def close(self) -> None:
        self._directory.cleanup()

    def __enter__(self) -> _SourceAuthorityTestSecurity:
        return self

    def __exit__(self, *_args: object) -> None:
        self.close()

    def sign(self, signing_bytes: bytes) -> str:
        payload_path = self._private_key.with_suffix(".payload")
        signature_path = self._private_key.with_suffix(".signature")
        with self._sign_lock:
            payload_path.write_bytes(source_authority_signature_payload(signing_bytes))
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


_SOURCE_AUTHORITY_SECURITY: _SourceAuthorityTestSecurity | None = None
_SOURCE_AUTHORITY_SECURITY_LOCK = Lock()


def _source_authority_security() -> _SourceAuthorityTestSecurity:
    global _SOURCE_AUTHORITY_SECURITY
    with _SOURCE_AUTHORITY_SECURITY_LOCK:
        if _SOURCE_AUTHORITY_SECURITY is None:
            _SOURCE_AUTHORITY_SECURITY = _SourceAuthorityTestSecurity()
        return _SOURCE_AUTHORITY_SECURITY


@pytest.fixture(scope="module", autouse=True)
def _cleanup_source_authority_security() -> Iterator[None]:
    yield
    global _SOURCE_AUTHORITY_SECURITY
    with _SOURCE_AUTHORITY_SECURITY_LOCK:
        security = _SOURCE_AUTHORITY_SECURITY
        _SOURCE_AUTHORITY_SECURITY = None
    if security is not None:
        security.close()


class _QuotaSigner:
    key_id = "source-broker-v2-test"

    def sign(self, payload: bytes) -> str:
        return canonical_sha256({"payload": payload.hex(), "signer": self.key_id})

    def verify(self, payload: bytes, signature: str) -> bool:
        return signature == self.sign(payload)


class _TestTransport:
    def __init__(
        self,
        *,
        outcome: str = "SUCCESS",
        lose_dispatch_once: bool = False,
        block_dispatch: bool = False,
        claim_once_unavailable: bool = False,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self._security = _source_authority_security()
        self.source_authority_keyring = self._security.keyring
        self.outcome = outcome
        self.lose_dispatch_once = lose_dispatch_once
        self.dispatch_calls = 0
        self.finalize_calls = 0
        self.replay_calls = 0
        self.claim_once_calls = 0
        self.deadlines: list[float | None] = []
        self.claim_once_unavailable = claim_once_unavailable
        self._clock = clock or (lambda: datetime.now(UTC))
        self.dispatch_entered = Event()
        self.second_dispatch_entered = Event()
        self.release_dispatch = Event()
        if not block_dispatch:
            self.release_dispatch.set()
        self._lock = Lock()
        self._dispatch_results: dict[str, bytes] = {}
        self._finalize_results: dict[str, bytes] = {}
        self._source_claims: dict[str, SourceBrokerV2ClaimOnceRequest] = {}
        self._inflight: set[str] = set()

    def claim_once(self, payload: bytes, *, deadline: float | None = None) -> bytes:
        self.deadlines.append(deadline)
        request = SourceBrokerV2ClaimOnceRequest.model_validate_json(payload)
        effect_operation_id = source_effect_operation_id(
            saga_id=request.saga_id,
            phase=request.phase,
        )
        if request.operation_id != effect_operation_id:
            raise SourceBrokerTransportError(
                "source operation id is not the deterministic saga phase effect id"
            )
        with self._lock:
            self.claim_once_calls += 1
            if self.claim_once_unavailable:
                raise ConnectionError("source claim_once authority unavailable")
            results = (
                self._dispatch_results
                if request.phase is SourceBrokerV2OutboxPhase.DISPATCH
                else self._finalize_results
            )
            result = results.get(effect_operation_id)
            existing = self._source_claims.get(request.operation_id)
            if existing is not None and (
                existing.saga_id != request.saga_id
                or existing.phase is not request.phase
                or existing.operation_request_hash != request.operation_request_hash
                or existing.claim_binding_hash != request.claim_binding_hash
                or existing.claim_generation != request.claim_generation
                or existing.scheduler_fencing_token != request.scheduler_fencing_token
                or existing.max_external_deadline != request.max_external_deadline
                or existing.not_before_takeover_at != request.not_before_takeover_at
            ):
                raise SourceBrokerTransportError("source operation claim binding conflicts")
            if result is not None:
                if request.phase is SourceBrokerV2OutboxPhase.DISPATCH:
                    dispatch = SourceBrokerV2DispatchResponse.model_validate_json(result)
                    status = SourceBrokerV2ClaimStatus(dispatch.outcome.value)
                else:
                    status = SourceBrokerV2ClaimStatus.SUCCESS
            elif effect_operation_id in self._inflight:
                status = SourceBrokerV2ClaimStatus.INFLIGHT
            elif (
                existing is None
                or (
                    existing.executor_owner_token_hash == request.executor_owner_token_hash
                    and existing.executor_generation == request.executor_generation
                )
                or self._clock() >= existing.not_before_takeover_at
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
            observed_at=self._clock(),
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
        request: SourceBrokerV2DispatchRequest | SourceBrokerV2FinalizeRequest,
        receipt: SourceBrokerV2ClaimOnceResponse,
    ) -> None:
        granted = self._source_claims.get(receipt.operation_id)
        if (
            granted is None
            or receipt.status is not SourceBrokerV2ClaimStatus.DEFINITIVELY_ABSENT
            or receipt.request_hash != granted.request_hash
            or receipt.operation_request_hash != request.request_hash
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
                request=request,
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
                request=request,
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


class _SpawnInitTransport:
    def __init__(self, keyring: SourceAuthorityKeyring) -> None:
        self.source_authority_keyring = keyring

    def claim_once(self, payload: bytes, *, deadline: float | None = None) -> bytes:
        raise AssertionError((payload, deadline))

    def dispatch(self, payload: bytes, *, deadline: float | None = None) -> bytes:
        raise AssertionError((payload, deadline))

    def finalize(self, payload: bytes, *, deadline: float | None = None) -> bytes:
        raise AssertionError((payload, deadline))

    def replay(self, payload: bytes, *, deadline: float | None = None) -> bytes:
        raise AssertionError((payload, deadline))


def _spawn_initialize_same_saga(
    *,
    saga_path: str,
    worker_root: str,
    worker_id: int,
    authority_id: str,
    key_id: str,
    public_key: bytes,
    start: Any,
    results: Any,
) -> None:
    barrier = start
    queue = results
    try:
        keyring = SourceAuthorityKeyring(
            expected_authority_id=authority_id,
            allowed_public_keys={key_id: public_key},
            expected_purpose=SOURCE_BROKER_V2_AUTHORITY_PURPOSE,
            expected_schema_version=2,
        )
        transport = _SpawnInitTransport(keyring)
        quota = _quota_adapter(Path(worker_root))
        barrier.wait(timeout=30)
        saga = SourceBrokerV2Saga.for_nonproduction(
            Path(saga_path),
            saga_id="spawn-saga",
            current_claim_authority=object(),
            quota_adapter=quota,
            transport=transport,
            lineage_authority=object(),
            source_authority_keyring=keyring,
            busy_timeout_ms=2_000,
        )
        with sqlite3.connect(saga_path, timeout=2) as connection:
            journal_mode = str(connection.execute("PRAGMA journal_mode").fetchone()[0])
            tables = frozenset(
                str(row[0])
                for row in connection.execute(
                    "SELECT name FROM sqlite_master WHERE type = 'table'"
                ).fetchall()
            )
            connected = connection.execute("SELECT 1").fetchone() == (1,)
        expected_tables = {
            "source_broker_v2_saga",
            "source_broker_v2_outbox",
            "source_broker_v2_source_receipt",
        }
        if journal_mode != "wal" or not expected_tables <= tables or not connected:
            raise AssertionError(
                f"spawn schema validation failed: journal={journal_mode}, tables={sorted(tables)}"
            )
        queue.put((worker_id, saga.saga_id, journal_mode, connected))
    except BaseException as exc:
        queue.put((worker_id, "ERROR", type(exc).__name__, str(exc)))
        raise
    finally:
        queue.close()
        queue.join_thread()


class _V2UnixTestServer:
    def __init__(
        self,
        path: Path,
        transport: _TestTransport,
        *,
        max_connections: int,
        drop_operations_once: frozenset[str] = frozenset(),
        response_delay_seconds: float = 0,
    ) -> None:
        self.path = path
        self.transport = transport
        self.max_connections = max_connections
        self.drop_operations_once = set(drop_operations_once)
        self.response_delay_seconds = response_delay_seconds
        self.ready = Event()
        self.stop = Event()
        self.errors: list[BaseException] = []
        self._thread = Thread(target=self._serve, daemon=True)

    def __enter__(self) -> _V2UnixTestServer:
        self._thread.start()
        assert self.ready.wait(timeout=5)
        return self

    def __exit__(self, *args: object) -> None:
        self.stop.set()
        self._thread.join(timeout=5)
        assert not self._thread.is_alive()
        with suppress(FileNotFoundError):
            self.path.unlink()
        if self.errors:
            raise self.errors[0]

    def _serve(self) -> None:
        try:
            with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as listener:
                listener.bind(str(self.path))
                os.chmod(self.path, 0o600)
                os.chown(self.path, os.geteuid(), os.getegid())
                listener.listen(16)
                listener.settimeout(0.1)
                self.ready.set()
                accepted = 0
                while accepted < self.max_connections and not self.stop.is_set():
                    try:
                        connection, _address = listener.accept()
                    except TimeoutError:
                        continue
                    accepted += 1
                    with connection:
                        request = SourceBrokerV2WireRequest.model_validate_json(
                            read_frame(connection)
                        )
                        result = getattr(self.transport, request.operation)(request.payload)
                        if request.operation in self.drop_operations_once:
                            self.drop_operations_once.remove(request.operation)
                            continue
                        response = SourceBrokerV2WireResponse(
                            operation=request.operation,
                            challenge=request.challenge,
                            request_hash=request.request_hash,
                            result=result,
                            result_hash=canonical_sha256(strict_canonical_json_loads(result)),
                        )
                        if self.response_delay_seconds:
                            time.sleep(self.response_delay_seconds)
                        try:
                            write_frame(connection, canonical_model_json_bytes(response))
                        except SourceBrokerTransportError:
                            if not self.response_delay_seconds:
                                raise
        except BaseException as exc:
            self.errors.append(exc)
            self.ready.set()


class _TestLineageAuthority:
    authority_id = "lineage-test"

    def __init__(self) -> None:
        self.calls: list[str] = []
        self.verify_calls = 0
        self._receipts: dict[str, ReplayLineageCheckpointReceipt] = {}

    def compare_and_advance(self, **kwargs: object) -> ReplayLineageCheckpointReceipt:
        operation_id = str(kwargs["operation_id"])
        existing = self._receipts.get(operation_id)
        if existing is not None:
            return existing
        self.calls.append(operation_id)
        receipt = ReplayLineageCheckpointReceipt(
            schema_version=1,
            contract="rquant-source-replay-lineage-checkpoint/v1",
            authority_id=self.authority_id,
            operation_id=operation_id,
            replay_authority_id=str(kwargs["replay_authority_id"]),
            lineage_id=str(kwargs["lineage_id"]),
            previous_head_hash=str(kwargs["previous_head_hash"]),
            next_head_hash=str(kwargs["next_head_hash"]),
            sequence=int(kwargs["sequence"]),
            claim_binding_hash=str(kwargs["claim_binding_hash"]),
            outcome="applied",
            key_id="lineage-test-key",
            signature="test",
        )
        self._receipts[operation_id] = receipt
        return receipt

    def verify_current(
        self,
        *,
        replay_authority_id: str,
        lineage_id: str,
        head_hash: str,
        sequence: int,
        receipt: ReplayLineageCheckpointReceipt | None,
    ) -> None:
        self.verify_calls += 1
        if receipt is None:
            raise RuntimeError("lineage receipt is missing")
        current = self._receipts.get(receipt.operation_id)
        if (
            current != receipt
            or receipt.replay_authority_id != replay_authority_id
            or receipt.lineage_id != lineage_id
            or receipt.next_head_hash != head_hash
            or receipt.sequence != sequence
        ):
            raise RuntimeError("lineage receipt is not current")


def _quota_adapter(tmp_path: Path) -> SourceQuotaBrokerAdapterV2:
    quota_path = tmp_path / "quota.sqlite3"
    store = SourceQuotaStore(quota_path)
    store.declare_window(
        source="tushare",
        window_id="2026-08-09",
        starts_at=NOW,
        resets_at=NOW + timedelta(minutes=10),
        total_units=20,
    )
    authority = SourceQuotaParentAuthority.for_nonproduction_standalone(
        quota_path,
        authority_id="quota-authority",
        signer=_QuotaSigner(),
    )
    return SourceQuotaBrokerAdapterV2(
        tmp_path / "quota-adapter.sqlite3",
        authority=authority,
        adapter_id="v2-saga-quota-adapter",
    )


def _operation_id(saga_id: str, phase: str) -> str:
    return canonical_sha256(
        {"contract": "rquant-source-broker-saga/v2", "phase": phase, "saga_id": saga_id}
    )


def _source_claim_request(
    *,
    challenge: str = "d" * 64,
) -> SourceBrokerV2ClaimOnceRequest:
    return SourceBrokerV2ClaimOnceRequest(
        saga_id="saga-a",
        operation_id=_operation_id("saga-a", "dispatch"),
        phase=SourceBrokerV2OutboxPhase.DISPATCH,
        operation_request_hash="a" * 64,
        challenge=challenge,
        claim_binding_hash="b" * 64,
        claim_generation=3,
        scheduler_fencing_token=7,
        executor_owner_token_hash="c" * 64,
        executor_generation=2,
        max_external_deadline=NOW + timedelta(seconds=1),
        not_before_takeover_at=NOW + timedelta(seconds=2),
    )


def _signed_source_claim(
    request: SourceBrokerV2ClaimOnceRequest,
    security: _SourceAuthorityTestSecurity,
) -> SourceBrokerV2ClaimOnceResponse:
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
        authority_id=security.authority_id,
        key_id=security.key_id,
        observed_at=NOW,
        status=SourceBrokerV2ClaimStatus.DEFINITIVELY_ABSENT,
        signature=base64.b64encode(b"0" * 64).decode("ascii"),
    )
    return unsigned.model_copy(update={"signature": security.sign(unsigned.signing_bytes())})


def _signed_source_replay(
    request: SourceBrokerV2ReplayRequest,
    security: _SourceAuthorityTestSecurity,
    *,
    status: SourceBrokerV2ReplayStatus,
    result: bytes | None = None,
) -> SourceBrokerV2ReplayResponse:
    unsigned = SourceBrokerV2ReplayResponse(
        saga_id=request.saga_id,
        operation_id=request.operation_id,
        phase=request.phase,
        request_hash=request.request_hash,
        challenge=request.challenge,
        status=status,
        result=result,
        result_hash=(
            None if result is None else canonical_sha256(strict_canonical_json_loads(result))
        ),
        authority_id=security.authority_id,
        key_id=security.key_id,
        signature=base64.b64encode(b"0" * 64).decode("ascii"),
    )
    return unsigned.model_copy(update={"signature": security.sign(unsigned.signing_bytes())})


def _request(tmp_path: Path, *, saga_id: str = "saga-a") -> tuple[object, object, object]:
    authorities: Authorities = create_test_authorities(tmp_path)
    claim = _claim(authorities)
    current = MemoryCurrentClaimAuthority(claim, authorities)
    issue = _unsigned_issue(
        claim=claim,
        authority=current,
        operation_id=_operation_id(saga_id, "claim"),
    )
    binding = SourceQuotaParentBindingV2(
        parent_id=f"parent-{saga_id}",
        source="tushare",
        owner="lab-worker-a",
        claim_binding_hash=issue.binding_hash,
        claim_generation=issue.binding.attempt_binding.claim_generation,
        scheduler_fencing_token=issue.binding.attempt_binding.scheduler_fencing_token,
    )
    call_cost = issue.unsigned_plan.source_intent.resource_request.cost_per_call
    request = SourceBrokerV2SagaRequest(
        saga_id=saga_id,
        claim_issue=issue,
        quota_binding=binding,
        parent_total_cost=call_cost,
        call_id=f"call-{saga_id}",
        call_cost=call_cost,
        payload=canonical_json_bytes({"trade_date": "2026-08-09"}),
        lineage_authority_id="source-replay-test",
        lineage_id=f"lineage-{saga_id}",
    )
    return request, current, _quota_adapter(tmp_path)


def test_v2_saga_module_exposes_closed_durable_surface() -> None:
    assert SourceBrokerV2SagaState.CLAIMED.value == "claimed"
    assert SourceBrokerV2SagaState.COMPLETE.value == "complete"
    assert SourceBrokerV2Saga.__name__ == "SourceBrokerV2Saga"


def test_v2_closed_source_claim_once_contract_is_fenced_and_signed() -> None:
    request = _source_claim_request()
    security = _source_authority_security()
    response = _signed_source_claim(request, security)
    security.keyring.require_verified_claim(request=request, receipt=response)

    assert response.status is SourceBrokerV2ClaimStatus.DEFINITIVELY_ABSENT
    assert len(response.receipt_hash) == 64

    nondeterministic = request.model_copy(update={"operation_id": "f" * 64})
    with pytest.raises(SourceBrokerTransportError, match="deterministic"):
        _TestTransport().claim_once(canonical_model_json_bytes(nondeterministic))


@pytest.mark.parametrize(
    "mutation",
    (
        {"signature": base64.b64encode(b"x" * 64).decode("ascii")},
        {"authority_id": "foreign-source-authority"},
        {"key_id": "unknown-source-key"},
        {"status": SourceBrokerV2ClaimStatus.UNKNOWN},
        {"scheduler_fencing_token": 8},
    ),
)
def test_v2_source_authority_keyring_rejects_forged_or_tampered_claim(
    mutation: dict[str, object],
) -> None:
    security = _source_authority_security()
    request = _source_claim_request()
    receipt = _signed_source_claim(request, security).model_copy(update=mutation)

    with pytest.raises(SourceBrokerV2SagaIntegrityError):
        security.keyring.require_verified_claim(request=request, receipt=receipt)


def test_v2_source_authority_keyring_rejects_replayed_old_challenge() -> None:
    security = _source_authority_security()
    old_request = _source_claim_request(challenge="d" * 64)
    receipt = _signed_source_claim(old_request, security)
    fresh_request = old_request.model_copy(update={"challenge": "e" * 64})

    with pytest.raises(SourceBrokerV2SagaIntegrityError, match="binding"):
        security.keyring.require_verified_claim(
            request=fresh_request,
            receipt=receipt,
        )


def test_v2_source_authority_keyring_allows_predeclared_key_rotation() -> None:
    with ExitStack() as stack:
        old = stack.enter_context(_SourceAuthorityTestSecurity(key_id="source-authority-old"))
        active = stack.enter_context(_SourceAuthorityTestSecurity(key_id="source-authority-active"))
        keyring = SourceAuthorityKeyring(
            expected_authority_id=old.authority_id,
            allowed_public_keys={
                old.key_id: old.public_key,
                active.key_id: active.public_key,
            },
            expected_purpose=SOURCE_BROKER_V2_AUTHORITY_PURPOSE,
            expected_schema_version=2,
        )
        request = _source_claim_request()

        keyring.require_verified_claim(
            request=request,
            receipt=_signed_source_claim(request, old),
        )
        keyring.require_verified_claim(
            request=request,
            receipt=_signed_source_claim(request, active),
        )


def test_v2_production_lease_covers_client_deadline_and_safety_grace(
    tmp_path: Path,
) -> None:
    security = _source_authority_security()
    client = SourceBrokerV2UnixClient(
        endpoint=SocketEndpointPolicy(
            path=tmp_path / "source-v2.sock",
            owner_uid=os.getuid(),
            group_gid=os.getgid(),
            mode=0o600,
        ),
        server_policy=ServerCredentialsPolicy(
            expected_uid=os.getuid(),
            expected_gid=os.getgid(),
        ),
        total_request_deadline_seconds=0.08,
        source_authority_keyring=security.keyring,
    )

    with pytest.raises(ValueError, match="cover the closed source client deadline"):
        SourceBrokerV2Saga(
            tmp_path / "saga.sqlite3",
            saga_id="saga-a",
            current_claim_authority=object(),  # type: ignore[arg-type]
            quota_adapter=object(),  # type: ignore[arg-type]
            transport=client,
            lineage_authority=object(),  # type: ignore[arg-type]
            source_authority_keyring=security.keyring,
            executor_lease_seconds=0.1,
            source_takeover_grace_seconds=0.03,
        )


def _real_v2_unix_client(
    *,
    socket_path: Path,
    security: _SourceAuthorityTestSecurity,
) -> SourceBrokerV2UnixClient:
    return SourceBrokerV2UnixClient(
        endpoint=SocketEndpointPolicy(
            path=socket_path,
            owner_uid=os.getuid(),
            group_gid=os.getgid(),
            mode=0o600,
        ),
        server_policy=ServerCredentialsPolicy(
            expected_uid=os.getuid(),
            expected_gid=os.getgid(),
            expected_pid=os.getpid(),
        ),
        total_request_deadline_seconds=2,
        source_authority_keyring=security.keyring,
        max_attempts=2,
    )


def _allow_local_v2_unix_identity(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        source_broker_v2_module,
        "require_linux_source_broker_transport",
        lambda: None,
    )
    monkeypatch.setattr(
        source_broker_v2_module,
        "verify_connected_server_authority",
        lambda **_kwargs: None,
    )
    monkeypatch.setattr(
        source_broker_v2_module,
        "_v2_kernel_peer_credentials",
        lambda _connection: (os.getpid(), os.getuid(), os.getgid()),
    )


class _DeterministicMonotonic:
    def __init__(self) -> None:
        self.value = 0.0

    def __call__(self) -> float:
        return self.value

    def advance(self, seconds: float) -> None:
        self.value += seconds


class _DeadlineScriptSocket:
    def __init__(
        self,
        clock: _DeterministicMonotonic,
        *,
        connect_advance: float = 0.0,
        send_steps: tuple[tuple[int, float], ...] = (),
        recv_steps: tuple[tuple[int, float], ...] = (),
        response_loss: bool = False,
        result: bytes | None = None,
    ) -> None:
        self.clock = clock
        self.connect_advance = connect_advance
        self.send_steps = list(send_steps)
        self.recv_steps = list(recv_steps)
        self.response_loss = response_loss
        self.result = result or canonical_json_bytes({"source": "ok"})
        self.connect_calls = 0
        self.send_calls = 0
        self.recv_calls = 0
        self.timeouts: list[float] = []
        self.sent = bytearray()
        self.response = bytearray()

    def __enter__(self) -> _DeadlineScriptSocket:
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def settimeout(self, seconds: float) -> None:
        self.timeouts.append(seconds)

    def connect(self, _path: str) -> None:
        self.connect_calls += 1
        self.clock.advance(self.connect_advance)

    def sendall(self, payload: bytes) -> None:
        remaining = memoryview(payload)
        while remaining:
            sent = self.send(remaining)
            if sent <= 0:
                raise OSError("scripted socket made no write progress")
            remaining = remaining[sent:]

    def send(self, payload: bytes | memoryview) -> int:
        self.send_calls += 1
        limit, advance = self.send_steps.pop(0) if self.send_steps else (len(payload), 0.0)
        sent = min(len(payload), limit)
        self.sent.extend(payload[:sent])
        self.clock.advance(advance)
        return sent

    def recv(self, size: int) -> bytes:
        self.recv_calls += 1
        if self.response_loss:
            return b""
        if not self.response:
            self._build_response()
        limit, advance = self.recv_steps.pop(0) if self.recv_steps else (size, 0.0)
        taken = min(size, limit, len(self.response))
        chunk = bytes(self.response[:taken])
        del self.response[:taken]
        self.clock.advance(advance)
        return chunk

    def _build_response(self) -> None:
        size = int.from_bytes(self.sent[:4], "big", signed=False)
        request = SourceBrokerV2WireRequest.model_validate_json(bytes(self.sent[4 : 4 + size]))
        response = canonical_model_json_bytes(
            SourceBrokerV2WireResponse(
                operation=request.operation,
                challenge=request.challenge,
                request_hash=request.request_hash,
                result=self.result,
                result_hash=canonical_sha256(strict_canonical_json_loads(self.result)),
            )
        )
        self.response.extend(len(response).to_bytes(4, "big") + response)


def _deadline_script_client(
    monkeypatch: pytest.MonkeyPatch,
    *,
    clock: _DeterministicMonotonic,
    scripted_socket: _DeadlineScriptSocket,
    deadline_seconds: float = 0.05,
) -> SourceBrokerV2UnixClient:
    security = _source_authority_security()
    _allow_local_v2_unix_identity(monkeypatch)
    monkeypatch.setattr(source_broker_v2_module.time, "monotonic", clock)
    monkeypatch.setattr(
        source_broker_v2_module,
        "validate_socket_endpoint",
        lambda *_args, **_kwargs: object(),
    )
    monkeypatch.setattr(
        source_broker_v2_module.socket,
        "socket",
        lambda *_args, **_kwargs: scripted_socket,
    )
    return SourceBrokerV2UnixClient(
        endpoint=SocketEndpointPolicy(
            path=Path("/private/tmp/rquant-source-v2-deadline.sock"),
            owner_uid=os.geteuid(),
            group_gid=os.getegid(),
            mode=0o600,
        ),
        server_policy=ServerCredentialsPolicy(
            expected_uid=os.geteuid(),
            expected_gid=os.getegid(),
            expected_pid=os.getpid(),
        ),
        total_request_deadline_seconds=deadline_seconds,
        source_authority_keyring=security.keyring,
        max_attempts=1,
    )


def _execute_deadline_probe(client: SourceBrokerV2UnixClient) -> bytes:
    return client._execute(  # noqa: SLF001
        operation="dispatch",
        payload=canonical_json_bytes({"deadline": "probe"}),
    )


def test_v2_unix_total_deadline_rejects_connect_that_exhausts_budget(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = _DeterministicMonotonic()
    scripted = _DeadlineScriptSocket(clock, connect_advance=0.05)
    client = _deadline_script_client(
        monkeypatch,
        clock=clock,
        scripted_socket=scripted,
    )

    with pytest.raises(SourceBrokerTransportError, match="deadline.*connect"):
        _execute_deadline_probe(client)

    assert scripted.connect_calls == 1
    assert scripted.send_calls == 0


def test_v2_unix_total_deadline_rejects_segmented_write_that_exhausts_budget(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = _DeterministicMonotonic()
    scripted = _DeadlineScriptSocket(
        clock,
        send_steps=((2, 0.023), (1_000_000, 0.08)),
    )
    client = _deadline_script_client(
        monkeypatch,
        clock=clock,
        scripted_socket=scripted,
    )

    with pytest.raises(SourceBrokerTransportError, match="deadline.*write"):
        _execute_deadline_probe(client)

    assert scripted.connect_calls == 1
    assert scripted.send_calls == 2
    assert scripted.recv_calls == 0
    assert clock.value == pytest.approx(0.103)


def test_v2_unix_total_deadline_rejects_segmented_read_that_exhausts_budget(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = _DeterministicMonotonic()
    scripted = _DeadlineScriptSocket(
        clock,
        recv_steps=((2, 0.02), (2, 0.02), (1_000_000, 0.02)),
    )
    client = _deadline_script_client(
        monkeypatch,
        clock=clock,
        scripted_socket=scripted,
    )

    with pytest.raises(SourceBrokerTransportError, match="deadline.*read"):
        _execute_deadline_probe(client)

    assert scripted.recv_calls == 3


def test_v2_unix_total_deadline_rejects_response_crossing_budget_during_parse(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = _DeterministicMonotonic()
    scripted = _DeadlineScriptSocket(clock, recv_steps=((1_000_000, 0.04),))
    client = _deadline_script_client(
        monkeypatch,
        clock=clock,
        scripted_socket=scripted,
    )
    original_decode = source_broker_v2_module._decode_v2_wire_response

    def delayed_decode(payload: bytes) -> object:
        decoded = original_decode(payload)
        clock.advance(0.02)
        return decoded

    monkeypatch.setattr(
        source_broker_v2_module,
        "_decode_v2_wire_response",
        delayed_decode,
    )

    with pytest.raises(SourceBrokerTransportError, match="deadline.*pars"):
        _execute_deadline_probe(client)


def test_v2_unix_total_deadline_rejects_response_crossing_budget_during_verification(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    security = _source_authority_security()
    request = _source_claim_request()
    receipt = _signed_source_claim(request, security)
    clock = _DeterministicMonotonic()
    scripted = _DeadlineScriptSocket(
        clock,
        recv_steps=((4, 0.01), (1_000_000, 0.02)),
        result=canonical_model_json_bytes(receipt),
    )
    client = _deadline_script_client(
        monkeypatch,
        clock=clock,
        scripted_socket=scripted,
    )
    original_verify = security.keyring.require_verified_claim

    def delayed_verify(**kwargs: object) -> None:
        original_verify(**kwargs)  # type: ignore[arg-type]
        clock.advance(0.03)

    monkeypatch.setattr(security.keyring, "require_verified_claim", delayed_verify)

    with pytest.raises(SourceBrokerTransportError, match="deadline.*verif"):
        client.claim_once(canonical_model_json_bytes(request))


def test_v2_unix_total_deadline_accepts_response_with_normal_budget(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = _DeterministicMonotonic()
    scripted = _DeadlineScriptSocket(
        clock,
        connect_advance=0.005,
        send_steps=((1_000_000, 0.01),),
        recv_steps=((4, 0.01), (1_000_000, 0.01)),
    )
    client = _deadline_script_client(
        monkeypatch,
        clock=clock,
        scripted_socket=scripted,
    )

    result = _execute_deadline_probe(client)

    assert strict_canonical_json_loads(result) == {"source": "ok"}
    assert clock.value == pytest.approx(0.035)
    assert all(timeout <= 0.05 for timeout in scripted.timeouts)


def test_v2_unix_total_deadline_preserves_response_loss_failure_without_retry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = _DeterministicMonotonic()
    scripted = _DeadlineScriptSocket(clock, response_loss=True)
    client = _deadline_script_client(
        monkeypatch,
        clock=clock,
        scripted_socket=scripted,
    )

    with pytest.raises(SourceBrokerV2SagaUnavailableError):
        _execute_deadline_probe(client)

    assert scripted.connect_calls == 1
    assert scripted.send_calls == 1
    assert scripted.recv_calls == 1


def test_v2_unix_client_real_roundtrip_claim_dispatch_finalize_and_replay(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _allow_local_v2_unix_identity(monkeypatch)
    security = _source_authority_security()
    transport = _TestTransport()
    dispatch_request = SourceBrokerV2DispatchRequest(
        saga_id="saga-a",
        operation_id=_operation_id("saga-a", "dispatch"),
        call_id="call-a",
        attempt_identity_hash="1" * 64,
        claim_plan_hash="2" * 64,
        claim_binding_hash="b" * 64,
        manifest_hash="3" * 64,
        payload=canonical_json_bytes({"trade_date": "2026-08-09"}),
        claim_payload_hash="4" * 64,
        dispatch_payload_hash=canonical_sha256({"trade_date": "2026-08-09"}),
    )
    dispatch_claim = _source_claim_request().model_copy(
        update={"operation_request_hash": dispatch_request.request_hash}
    )

    with _short_unix_socket_directory() as socket_root:
        socket_path = Path(socket_root) / "s"
        with _V2UnixTestServer(socket_path, transport, max_connections=5):
            client = _real_v2_unix_client(socket_path=socket_path, security=security)
            grant = SourceBrokerV2ClaimOnceResponse.model_validate_json(
                client.claim_once(canonical_model_json_bytes(dispatch_claim))
            )
            dispatch = SourceBrokerV2DispatchResponse.model_validate_json(
                client.dispatch(
                    canonical_model_json_bytes(
                        SourceBrokerV2DispatchEnvelope(
                            request=dispatch_request,
                            claim_receipt=grant,
                        )
                    )
                )
            )
            finalize_request = SourceBrokerV2FinalizeRequest(
                saga_id="saga-a",
                operation_id=_operation_id("saga-a", "source_finalize"),
                dispatch_evidence_hash=dispatch.evidence_hash,
                claim_binding_hash="b" * 64,
            )
            finalize_claim = dispatch_claim.model_copy(
                update={
                    "operation_id": finalize_request.operation_id,
                    "phase": SourceBrokerV2OutboxPhase.SOURCE_FINALIZE,
                    "operation_request_hash": finalize_request.request_hash,
                    "challenge": "e" * 64,
                }
            )
            finalize_grant = SourceBrokerV2ClaimOnceResponse.model_validate_json(
                client.claim_once(canonical_model_json_bytes(finalize_claim))
            )
            finalized = SourceBrokerV2FinalizeResponse.model_validate_json(
                client.finalize(
                    canonical_model_json_bytes(
                        SourceBrokerV2FinalizeEnvelope(
                            request=finalize_request,
                            claim_receipt=finalize_grant,
                        )
                    )
                )
            )
            replay_request = SourceBrokerV2ReplayRequest(
                saga_id="saga-a",
                operation_id=dispatch_request.operation_id,
                phase=SourceBrokerV2OutboxPhase.DISPATCH,
                operation_request_hash=dispatch_request.request_hash,
                challenge="f" * 64,
            )
            replay = SourceBrokerV2ReplayResponse.model_validate_json(
                client.replay(canonical_model_json_bytes(replay_request))
            )

        slow_socket_path = Path(socket_root) / "slow"
        with _V2UnixTestServer(
            slow_socket_path,
            transport,
            max_connections=1,
            response_delay_seconds=0.2,
        ):
            slow_client = _real_v2_unix_client(
                socket_path=slow_socket_path,
                security=security,
            )
            started = time.monotonic()
            with pytest.raises(SourceBrokerTransportError, match="deadline"):
                slow_client.replay(
                    canonical_model_json_bytes(replay_request),
                    deadline=started + 0.04,
                )
            elapsed = time.monotonic() - started

    assert dispatch.outcome is SourceBrokerV2DispatchOutcome.SUCCESS
    assert finalized.request_hash == finalize_request.request_hash
    assert replay.status is SourceBrokerV2ReplayStatus.FOUND
    assert transport.dispatch_calls == 1
    assert transport.finalize_calls == 1
    assert 0.02 <= elapsed < 0.15


def test_v2_unix_response_loss_restart_recovers_by_signed_lookup_without_redispatch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _allow_local_v2_unix_identity(monkeypatch)
    security = _source_authority_security()
    transport = _TestTransport()
    dispatch_request = SourceBrokerV2DispatchRequest(
        saga_id="saga-a",
        operation_id=_operation_id("saga-a", "dispatch"),
        call_id="call-a",
        attempt_identity_hash="1" * 64,
        claim_plan_hash="2" * 64,
        claim_binding_hash="b" * 64,
        manifest_hash="3" * 64,
        payload=canonical_json_bytes({"trade_date": "2026-08-09"}),
        claim_payload_hash="4" * 64,
        dispatch_payload_hash=canonical_sha256({"trade_date": "2026-08-09"}),
    )
    claim = _source_claim_request().model_copy(
        update={"operation_request_hash": dispatch_request.request_hash}
    )
    with _short_unix_socket_directory() as socket_root:
        first_socket = Path(socket_root) / "a"
        with _V2UnixTestServer(
            first_socket,
            transport,
            max_connections=2,
            drop_operations_once=frozenset({"dispatch"}),
        ):
            client = _real_v2_unix_client(socket_path=first_socket, security=security)
            grant = SourceBrokerV2ClaimOnceResponse.model_validate_json(
                client.claim_once(canonical_model_json_bytes(claim))
            )
            with pytest.raises(SourceBrokerV2SagaUnavailableError):
                client.dispatch(
                    canonical_model_json_bytes(
                        SourceBrokerV2DispatchEnvelope(
                            request=dispatch_request,
                            claim_receipt=grant,
                        )
                    )
                )
        restarted_socket = Path(socket_root) / "b"
        fresh_lookup = claim.model_copy(update={"challenge": "e" * 64})
        with _V2UnixTestServer(restarted_socket, transport, max_connections=1):
            restarted = _real_v2_unix_client(
                socket_path=restarted_socket,
                security=security,
            )
            terminal = SourceBrokerV2ClaimOnceResponse.model_validate_json(
                restarted.claim_once(canonical_model_json_bytes(fresh_lookup))
            )

    assert terminal.status is SourceBrokerV2ClaimStatus.SUCCESS
    assert transport.dispatch_calls == 1


def test_v2_saga_rejects_non_canonical_sqlite_state_before_external_effect(
    tmp_path: Path,
) -> None:
    assert issubclass(SourceBrokerV2SagaIntegrityError, RuntimeError)


def test_v2_saga_happy_path_persists_all_effects_before_lineage(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    request, current, quota = _request(tmp_path)
    clock = _MutableUtcClock(datetime.now(UTC))
    clock.install_source_broker_clock(monkeypatch)
    transport = _TestTransport(clock=lambda: clock.now())
    lineage = _TestLineageAuthority()
    saga = SourceBrokerV2Saga.for_nonproduction(
        tmp_path / "saga.sqlite3",
        saga_id="saga-a",
        current_claim_authority=current,
        quota_adapter=quota,
        transport=transport,
        lineage_authority=lineage,
    )

    result = saga.advance(request, now=NOW + timedelta(seconds=1))

    assert result.state is SourceBrokerV2SagaState.COMPLETE
    assert result.dispatch_outcome is not None
    assert transport.dispatch_calls == 1
    assert transport.finalize_calls == 1
    assert transport.claim_once_calls >= 2
    assert len(lineage.calls) == 1


def test_v2_saga_does_not_compensate_after_dispatch_response_loss(tmp_path: Path) -> None:
    request, current, quota = _request(tmp_path)
    transport = _TestTransport(lose_dispatch_once=True)
    saga = SourceBrokerV2Saga.for_nonproduction(
        tmp_path / "saga.sqlite3",
        saga_id="saga-a",
        current_claim_authority=current,
        quota_adapter=quota,
        transport=transport,
        lineage_authority=_TestLineageAuthority(),
        source_request_deadline_seconds=_UNCONSTRAINED_SOURCE_DEADLINE_SECONDS,
        source_takeover_grace_seconds=_UNCONSTRAINED_SOURCE_TAKEOVER_GRACE_SECONDS,
    )

    first = saga.advance(request, now=NOW + timedelta(seconds=1))
    assert first.state is SourceBrokerV2SagaState.RECONCILE_REQUIRED
    with pytest.raises(SourceBrokerV2SagaReconcileRequiredError):
        saga.compensate_before_dispatch(request, now=NOW + timedelta(seconds=2))
    resumed = saga.reconcile(request, now=NOW + timedelta(seconds=3))
    assert resumed.state is SourceBrokerV2SagaState.COMPLETE
    assert transport.dispatch_calls == 1


def test_v2_saga_replays_claim_after_authority_commit_response_loss(tmp_path: Path) -> None:
    request, current, quota = _request(tmp_path)
    current.fail_after_commit_once = True
    path = tmp_path / "saga.sqlite3"
    first = SourceBrokerV2Saga.for_nonproduction(
        path,
        saga_id="saga-a",
        current_claim_authority=current,
        quota_adapter=quota,
        transport=_TestTransport(),
        lineage_authority=_TestLineageAuthority(),
        source_request_deadline_seconds=_UNCONSTRAINED_SOURCE_DEADLINE_SECONDS,
        source_takeover_grace_seconds=_UNCONSTRAINED_SOURCE_TAKEOVER_GRACE_SECONDS,
    )
    with pytest.raises(SourceBrokerV2SagaUnavailableError):
        first.advance(request, now=NOW + timedelta(seconds=1))

    restarted = SourceBrokerV2Saga.for_nonproduction(
        path,
        saga_id="saga-a",
        current_claim_authority=current,
        quota_adapter=quota,
        transport=_TestTransport(),
        lineage_authority=_TestLineageAuthority(),
        source_request_deadline_seconds=_UNCONSTRAINED_SOURCE_DEADLINE_SECONDS,
        source_takeover_grace_seconds=_UNCONSTRAINED_SOURCE_TAKEOVER_GRACE_SECONDS,
    )
    result = restarted.advance(request, now=NOW + timedelta(seconds=2))

    assert result.state is SourceBrokerV2SagaState.COMPLETE
    assert current.signing_calls == 1


def test_v2_saga_compensates_only_with_pre_dispatch_evidence(tmp_path: Path) -> None:
    request, current, quota = _request(tmp_path)
    lineage = _TestLineageAuthority()
    saga = SourceBrokerV2Saga.for_nonproduction(
        tmp_path / "saga.sqlite3",
        saga_id="saga-a",
        current_claim_authority=current,
        quota_adapter=quota,
        transport=_TestTransport(),
        lineage_authority=lineage,
        source_request_deadline_seconds=_UNCONSTRAINED_SOURCE_DEADLINE_SECONDS,
        source_takeover_grace_seconds=_UNCONSTRAINED_SOURCE_TAKEOVER_GRACE_SECONDS,
    )

    result = saga.compensate_before_dispatch(request, now=NOW + timedelta(seconds=1))

    assert result.state is SourceBrokerV2SagaState.COMPLETE
    assert result.dispatch_outcome is None
    assert lineage.calls == []


def test_v2_saga_compensation_recovers_release_after_call_terminal_response_loss(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    request, current, quota = _request(tmp_path)
    path = tmp_path / "saga.sqlite3"
    transport = _TestTransport()
    lineage = _TestLineageAuthority()
    first = SourceBrokerV2Saga.for_nonproduction(
        path,
        saga_id="saga-a",
        current_claim_authority=current,
        quota_adapter=quota,
        transport=transport,
        lineage_authority=lineage,
        source_request_deadline_seconds=_UNCONSTRAINED_SOURCE_DEADLINE_SECONDS,
        source_takeover_grace_seconds=_UNCONSTRAINED_SOURCE_TAKEOVER_GRACE_SECONDS,
    )
    lost_release = False

    def lose_release(phase: SourceBrokerV2OutboxPhase) -> None:
        nonlocal lost_release
        if phase is SourceBrokerV2OutboxPhase.RELEASE_UNUSED and not lost_release:
            lost_release = True
            raise ConnectionError("release committed before response loss")

    monkeypatch.setattr(first, "_after_external_effect", lose_release)
    with pytest.raises(ConnectionError, match="release committed"):
        first.compensate_before_dispatch(request, now=NOW + timedelta(seconds=1))

    assert first.snapshot().state is SourceBrokerV2SagaState.CALL_TERMINALIZED
    with sqlite3.connect(path) as connection:
        statuses = dict(
            connection.execute(
                "SELECT phase, status FROM source_broker_v2_outbox "
                "WHERE phase IN ('unknown_before_dispatch', 'release_unused')"
            ).fetchall()
        )
    assert statuses == {
        "unknown_before_dispatch": "applied",
        "release_unused": "pending",
    }

    restarted = SourceBrokerV2Saga.for_nonproduction(
        path,
        saga_id="saga-a",
        current_claim_authority=current,
        quota_adapter=quota,
        transport=transport,
        lineage_authority=lineage,
        source_request_deadline_seconds=_UNCONSTRAINED_SOURCE_DEADLINE_SECONDS,
        source_takeover_grace_seconds=_UNCONSTRAINED_SOURCE_TAKEOVER_GRACE_SECONDS,
    )
    recovered = restarted.compensate_before_dispatch(
        request,
        now=NOW + timedelta(seconds=2),
    )

    assert recovered.state is SourceBrokerV2SagaState.COMPLETE
    with sqlite3.connect(path) as connection:
        release_status = connection.execute(
            "SELECT status FROM source_broker_v2_outbox WHERE phase = 'release_unused'"
        ).fetchone()[0]
    assert release_status == "applied"


def test_v2_saga_rejects_tampered_applied_outbox_before_replay(tmp_path: Path) -> None:
    request, current, quota = _request(tmp_path)
    path = tmp_path / "saga.sqlite3"
    saga = SourceBrokerV2Saga.for_nonproduction(
        path,
        saga_id="saga-a",
        current_claim_authority=current,
        quota_adapter=quota,
        transport=_TestTransport(),
        lineage_authority=_TestLineageAuthority(),
        source_request_deadline_seconds=_UNCONSTRAINED_SOURCE_DEADLINE_SECONDS,
        source_takeover_grace_seconds=_UNCONSTRAINED_SOURCE_TAKEOVER_GRACE_SECONDS,
    )
    saga.advance(request, now=NOW + timedelta(seconds=1))
    with sqlite3.connect(path) as connection:
        connection.execute(
            "UPDATE source_broker_v2_outbox SET result_json = '{}' WHERE phase = 'lineage'"
        )

    restarted = SourceBrokerV2Saga.for_nonproduction(
        path,
        saga_id="saga-a",
        current_claim_authority=current,
        quota_adapter=quota,
        transport=_TestTransport(),
        lineage_authority=_TestLineageAuthority(),
        source_request_deadline_seconds=_UNCONSTRAINED_SOURCE_DEADLINE_SECONDS,
        source_takeover_grace_seconds=_UNCONSTRAINED_SOURCE_TAKEOVER_GRACE_SECONDS,
    )
    with pytest.raises(SourceBrokerV2SagaIntegrityError):
        restarted.advance(request, now=NOW + timedelta(seconds=2))


def test_v2_saga_concurrent_same_attempt_has_one_durable_source_effect(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    request, current, quota = _request(tmp_path)
    path = tmp_path / "saga.sqlite3"
    clock = _MutableUtcClock(datetime.now(UTC))
    clock.install_source_broker_clock(monkeypatch)
    transport = _TestTransport(block_dispatch=True, clock=lambda: clock.now())
    lineage = _TestLineageAuthority()

    probe = SourceBrokerV2Saga.for_nonproduction(
        path,
        saga_id="saga-a",
        current_claim_authority=current,
        quota_adapter=quota,
        transport=transport,
        lineage_authority=lineage,
    )
    statements: list[str] = []
    real_connect = source_broker_v2_module.sqlite3.connect

    def traced_connect(*args: object, **kwargs: object) -> sqlite3.Connection:
        connection = real_connect(*args, **kwargs)
        connection.set_trace_callback(statements.append)
        return connection

    with monkeypatch.context() as patch:
        patch.setattr(source_broker_v2_module.sqlite3, "connect", traced_connect)
        with probe._connect():  # noqa: SLF001
            pass

    normalized = tuple(statement.upper() for statement in statements)
    assert any("PRAGMA FOREIGN_KEYS" in statement for statement in normalized)
    assert not any("PRAGMA JOURNAL_MODE" in statement for statement in normalized)

    context = multiprocessing.get_context("spawn")
    security = _source_authority_security()
    for round_index in range(50):
        round_root = tmp_path / "spawn" / f"round-{round_index:02d}"
        saga_path = round_root / "saga.sqlite3"
        start = context.Barrier(9)
        results = context.Queue()
        processes = tuple(
            context.Process(
                target=_spawn_initialize_same_saga,
                kwargs={
                    "saga_path": str(saga_path),
                    "worker_root": str(round_root / f"worker-{worker_id}"),
                    "worker_id": worker_id,
                    "authority_id": security.authority_id,
                    "key_id": security.key_id,
                    "public_key": security.public_key,
                    "start": start,
                    "results": results,
                },
            )
            for worker_id in range(8)
        )
        round_results: list[tuple[object, ...]] = []
        started_processes: list[multiprocessing.Process] = []
        try:
            for process in processes:
                process.start()
                started_processes.append(process)
            start.wait(timeout=30)
            join_deadline = time.monotonic() + 30
            for process in processes:
                process.join(timeout=max(0, join_deadline - time.monotonic()))
            result_deadline = time.monotonic() + 5
            while len(round_results) < len(processes):
                try:
                    round_results.append(
                        results.get(timeout=max(0.01, result_deadline - time.monotonic()))
                    )
                except Empty:
                    break
            assert [process.exitcode for process in processes] == [0] * 8
            assert sorted(round_results) == [
                (worker_id, "spawn-saga", "wal", True) for worker_id in range(8)
            ]
        finally:
            for process in started_processes:
                if process.is_alive():
                    process.terminate()
            for process in started_processes:
                process.join(timeout=5)
                process.close()
            for process in processes[len(started_processes) :]:
                process.close()
            results.close()
            results.join_thread()

    start = Barrier(3)

    def run() -> object:
        start.wait(timeout=5)
        return SourceBrokerV2Saga.for_nonproduction(
            path,
            saga_id="saga-a",
            current_claim_authority=current,
            quota_adapter=quota,
            transport=transport,
            lineage_authority=lineage,
        ).advance(request, now=NOW + timedelta(seconds=1))

    with ThreadPoolExecutor(max_workers=2) as executor:
        first = executor.submit(run)
        second = executor.submit(run)
        start.wait(timeout=5)
        dispatch_wait_deadline = time.monotonic() + 5
        while not transport.dispatch_entered.wait(timeout=0.01):
            for future in (first, second):
                if future.done():
                    future.result()
            if time.monotonic() >= dispatch_wait_deadline:
                pytest.fail("concurrent saga workers did not reach source dispatch")
        assert not transport.second_dispatch_entered.wait(timeout=0.25)
        transport.release_dispatch.set()
        results = [first.result(timeout=10), second.result(timeout=10)]

    assert {result.state for result in results} == {SourceBrokerV2SagaState.COMPLETE}
    assert current.signing_calls == 1
    assert transport.dispatch_calls == 1


def test_v2_saga_heartbeat_schedule_protects_only_before_lease_expiry(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def run_schedule(
        scenario_root: Path,
        *,
        heartbeat_delay_seconds: float,
        expect_renewal: bool,
    ) -> None:
        scenario_root.mkdir()
        request, current, quota = _request(scenario_root)
        path = scenario_root / "saga.sqlite3"
        clock = _MutableUtcClock(datetime.now(UTC))
        initial_time = clock.now()
        heartbeat_time = initial_time + timedelta(seconds=heartbeat_delay_seconds)
        original_lease_boundary = initial_time + timedelta(seconds=0.05)
        competitor_time = initial_time + timedelta(seconds=0.06)
        expected_renewed_boundary = heartbeat_time + timedelta(seconds=0.05)
        transport = _TestTransport(block_dispatch=True, clock=lambda: clock.now())
        lineage = _TestLineageAuthority()
        wake_ready = Event()
        wake_release = Event()
        heartbeat_observed = Event()
        competitor_release = Event()
        competitor_observed_lease = Event()
        competitor_can_continue = Event()
        heartbeat_intervals: list[float] = []
        heartbeat_results: list[BaseException | None] = []
        competitor_lease_rows: list[tuple[datetime, str | None, str | None]] = []

        def saga() -> SourceBrokerV2Saga:
            return SourceBrokerV2Saga.for_nonproduction(
                path,
                saga_id="saga-a",
                current_claim_authority=current,
                quota_adapter=quota,
                transport=transport,
                lineage_authority=lineage,
                executor_lease_seconds=0.05,
                executor_wait_seconds=1.0,
            )

        first_saga = saga()
        competitor_saga = saga()
        original_first_heartbeat = first_saga._heartbeat_outbox
        original_competitor_read = competitor_saga._read_outbox

        def controlled_heartbeat_wait(
            stop: Event,
            interval: float,
            *,
            phase: SourceBrokerV2OutboxPhase,
        ) -> bool:
            if phase is not SourceBrokerV2OutboxPhase.DISPATCH:
                return stop.wait(interval)
            heartbeat_intervals.append(interval)
            if len(heartbeat_intervals) == 1:
                wake_ready.set()
                if not wake_release.wait(timeout=5):
                    raise TimeoutError("test scheduler did not release the heartbeat wake")
                return stop.is_set()
            return stop.wait(timeout=5)

        def observe_first_heartbeat(**kwargs: object) -> None:
            is_background_dispatch = kwargs.get(
                "phase"
            ) is SourceBrokerV2OutboxPhase.DISPATCH and current_thread().name.startswith(
                "rquant-source-broker-v2-heartbeat-"
            )
            try:
                original_first_heartbeat(**kwargs)  # type: ignore[arg-type]
            except BaseException as exc:
                if is_background_dispatch:
                    heartbeat_results.append(exc)
                    heartbeat_observed.set()
                raise
            else:
                if is_background_dispatch:
                    heartbeat_results.append(None)
                    heartbeat_observed.set()

        def observe_competitor_lease(
            connection: sqlite3.Connection,
            *,
            operation_id: str,
            phase: SourceBrokerV2OutboxPhase,
        ) -> sqlite3.Row:
            row = original_competitor_read(
                connection,
                operation_id=operation_id,
                phase=phase,
            )
            if phase is SourceBrokerV2OutboxPhase.DISPATCH and not competitor_lease_rows:
                heartbeat_raw = row["executor_heartbeat_at"]
                expiry_raw = row["executor_lease_expires_at"]
                competitor_lease_rows.append((clock.now(), heartbeat_raw, expiry_raw))
                competitor_observed_lease.set()
                if not competitor_can_continue.wait(timeout=5):
                    raise TimeoutError("test scheduler did not release the competitor")
            return row

        def run_competitor() -> object:
            if not competitor_release.wait(timeout=5):
                raise TimeoutError("test scheduler did not start the competitor")
            return competitor_saga.advance(request, now=NOW + timedelta(seconds=2))

        with monkeypatch.context() as scenario_patch:
            clock.install_source_broker_clock(scenario_patch)
            scenario_patch.setattr(
                first_saga,
                "_wait_for_heartbeat",
                controlled_heartbeat_wait,
            )
            scenario_patch.setattr(
                first_saga,
                "_heartbeat_outbox",
                observe_first_heartbeat,
            )
            scenario_patch.setattr(
                competitor_saga,
                "_read_outbox",
                observe_competitor_lease,
            )
            with ThreadPoolExecutor(max_workers=2) as executor:
                first = executor.submit(
                    first_saga.advance,
                    request,
                    now=NOW + timedelta(seconds=1),
                )
                competitor = executor.submit(run_competitor)
                try:
                    assert transport.dispatch_entered.wait(timeout=5)
                    assert wake_ready.wait(timeout=5)
                    expected_interval = 0.05 / 3
                    assert heartbeat_intervals == [pytest.approx(expected_interval)]
                    assert heartbeat_intervals[0] < 0.05

                    if expect_renewal:
                        clock.current = heartbeat_time
                        wake_release.set()
                        assert heartbeat_observed.wait(timeout=5)
                        assert heartbeat_results == [None]

                    clock.current = competitor_time
                    competitor_release.set()
                    assert competitor_observed_lease.wait(timeout=5)
                    observed_at, observed_heartbeat_raw, observed_expiry_raw = (
                        competitor_lease_rows[0]
                    )
                    expected_heartbeat = heartbeat_time if expect_renewal else initial_time
                    expected_expiry = (
                        expected_renewed_boundary if expect_renewal else original_lease_boundary
                    )
                    assert observed_at == competitor_time
                    assert datetime.fromisoformat(observed_heartbeat_raw) == expected_heartbeat
                    assert datetime.fromisoformat(observed_expiry_raw) == expected_expiry
                    competitor_can_continue.set()

                    if expect_renewal:
                        assert heartbeat_time < original_lease_boundary
                        assert competitor_time < expected_renewed_boundary
                        transport.release_dispatch.set()
                        results = [first.result(timeout=10), competitor.result(timeout=10)]
                        assert {result.state for result in results} == {
                            SourceBrokerV2SagaState.COMPLETE
                        }
                    else:
                        assert original_lease_boundary < competitor_time < heartbeat_time
                        competitor_result = competitor.result(timeout=10)
                        assert competitor_result.state is SourceBrokerV2SagaState.RECONCILE_REQUIRED
                        clock.current = heartbeat_time
                        wake_release.set()
                        assert heartbeat_observed.wait(timeout=5)
                        transport.release_dispatch.set()
                        first_result = first.result(timeout=10)
                        assert first_result.state is SourceBrokerV2SagaState.RECONCILE_REQUIRED
                        assert any(
                            isinstance(error, SourceBrokerV2SagaConflictError)
                            for error in heartbeat_results
                            if error is not None
                        )
                finally:
                    wake_release.set()
                    competitor_release.set()
                    competitor_can_continue.set()
                    transport.release_dispatch.set()

        assert transport.dispatch_calls == 1
        assert heartbeat_intervals
        assert all(interval == pytest.approx(0.05 / 3) for interval in heartbeat_intervals)
        assert not any(
            thread.name.startswith("rquant-source-broker-v2-heartbeat-")
            for thread in enumerate_threads()
        )

    run_schedule(
        tmp_path / "late-heartbeat",
        heartbeat_delay_seconds=0.10,
        expect_renewal=False,
    )
    run_schedule(
        tmp_path / "on-time-heartbeat",
        heartbeat_delay_seconds=0.04,
        expect_renewal=True,
    )
    with pytest.raises(TypeError, match="unexpected keyword argument 'heartbeat_scheduler'"):
        SourceBrokerV2Saga.for_production(
            tmp_path / "production.sqlite3",
            runtime=object(),
            scheduler_clients=object(),
            heartbeat_scheduler=object(),  # type: ignore[call-arg]
        )


def test_v2_saga_persists_source_window_grant_and_terminal_observation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    request, current, quota = _request(tmp_path)
    path = tmp_path / "saga.sqlite3"
    clock = _MutableUtcClock(datetime.now(UTC))
    clock.install_source_broker_clock(monkeypatch)
    saga = SourceBrokerV2Saga.for_nonproduction(
        path,
        saga_id="saga-a",
        current_claim_authority=current,
        quota_adapter=quota,
        transport=_TestTransport(clock=lambda: clock.now()),
        lineage_authority=_TestLineageAuthority(),
    )

    result = saga.advance(request, now=NOW + timedelta(seconds=1))

    assert result.state is SourceBrokerV2SagaState.COMPLETE
    with sqlite3.connect(path) as connection:
        row = connection.execute(
            "SELECT dispatch_started_at, max_external_deadline, "
            "not_before_takeover_at, source_grant_json, source_grant_hash, "
            "source_observation_json, source_observation_hash "
            "FROM source_broker_v2_outbox WHERE phase = 'dispatch'"
        ).fetchone()
        receipt_count = connection.execute(
            "SELECT COUNT(*) FROM source_broker_v2_source_receipt "
            "WHERE operation_id = (SELECT operation_id FROM source_broker_v2_outbox "
            "WHERE phase = 'dispatch')"
        ).fetchone()[0]
    assert row is not None
    assert all(type(value) is str and value for value in row)
    started_at, deadline, takeover_at = (
        datetime.fromisoformat(str(row[index])) for index in range(3)
    )
    assert started_at <= deadline <= takeover_at
    assert receipt_count == 2

    expired_root = tmp_path / "expired-before-invoke"
    expired_root.mkdir()
    expired_request, expired_current, expired_quota = _request(
        expired_root,
        saga_id="saga-expired-before-invoke",
    )
    expired_transport = _TestTransport(clock=lambda: clock.now())
    original_claim_once = expired_transport.claim_once

    def expire_after_claim(payload: bytes, *, deadline: float | None = None) -> bytes:
        response = original_claim_once(payload, deadline=deadline)
        clock.advance(0.30)
        return response

    monkeypatch.setattr(expired_transport, "claim_once", expire_after_claim)
    expired_path = expired_root / "saga.sqlite3"
    expired_saga = SourceBrokerV2Saga.for_nonproduction(
        expired_path,
        saga_id="saga-expired-before-invoke",
        current_claim_authority=expired_current,
        quota_adapter=expired_quota,
        transport=expired_transport,
        lineage_authority=_TestLineageAuthority(),
    )

    expired = expired_saga.advance(expired_request, now=NOW + timedelta(seconds=1))

    assert expired.state is SourceBrokerV2SagaState.RECONCILE_REQUIRED
    assert expired_transport.dispatch_calls == 0
    with sqlite3.connect(expired_path) as connection:
        invoke_started, dispatch_started, owner, lease_expiry = connection.execute(
            "SELECT invoke_started, dispatch_started_at, executor_owner_token, "
            "executor_lease_expires_at FROM source_broker_v2_outbox "
            "WHERE phase = 'dispatch'"
        ).fetchone()
    assert (invoke_started, dispatch_started, owner, lease_expiry) == (0, None, None, None)

    finalize_root = tmp_path / "finalize-expired-before-invoke"
    finalize_root.mkdir()
    finalize_request, finalize_current, finalize_quota = _request(
        finalize_root,
        saga_id="saga-finalize-expired-before-invoke",
    )
    finalize_transport = _TestTransport(clock=lambda: clock.now())
    finalize_lineage = _TestLineageAuthority()
    from .test_source_broker_v2_service import (
        _CountingProvider,
        _FakeExternalDispatchAuthority,
        _service,
    )

    finalize_provider = _CountingProvider()
    finalize_authority = _FakeExternalDispatchAuthority()
    finalize_claim_operations: list[tuple[SourceBrokerV2OutboxPhase, str]] = []
    finalize_service, finalize_keyring = _service(
        finalize_root,
        provider=finalize_provider,
        external_authority=finalize_authority,
        clock=lambda: clock.now(),
    )

    class _ExpiringProviderTransport:
        source_authority_keyring = finalize_keyring

        def claim_once(self, payload: bytes, *, deadline: float | None = None) -> bytes:
            nonlocal finalize_claim_expired
            claim_request = SourceBrokerV2ClaimOnceRequest.model_validate_json(payload)
            finalize_claim_operations.append((claim_request.phase, claim_request.operation_id))
            response = finalize_service.claim_once(payload, deadline=deadline)
            if (
                claim_request.phase is SourceBrokerV2OutboxPhase.SOURCE_FINALIZE
                and not finalize_claim_expired
            ):
                finalize_claim_expired = True
                clock.advance(0.30)
            return response

        def dispatch(self, payload: bytes, *, deadline: float | None = None) -> bytes:
            return finalize_service.dispatch(payload, deadline=deadline)

        def finalize(self, payload: bytes, *, deadline: float | None = None) -> bytes:
            return finalize_service.finalize(payload, deadline=deadline)

        def replay(self, payload: bytes, *, deadline: float | None = None) -> bytes:
            return finalize_service.replay(payload, deadline=deadline)

    finalize_claim_expired = False
    finalize_transport = _ExpiringProviderTransport()
    finalize_path = finalize_root / "saga.sqlite3"
    finalize_saga = SourceBrokerV2Saga.for_nonproduction(
        finalize_path,
        saga_id="saga-finalize-expired-before-invoke",
        current_claim_authority=finalize_current,
        quota_adapter=finalize_quota,
        transport=finalize_transport,
        lineage_authority=finalize_lineage,
    )

    pending_finalize = finalize_saga.advance(
        finalize_request,
        now=NOW + timedelta(seconds=1),
    )

    assert pending_finalize.state.value == "source_finalize_reconcile_required"
    assert finalize_provider.dispatch_calls == 1
    assert finalize_provider.finalize_calls == 0
    assert finalize_lineage.calls == []
    with sqlite3.connect(finalize_path) as connection:
        finalize_row = connection.execute(
            "SELECT invoke_started, dispatch_started_at, max_external_deadline, "
            "not_before_takeover_at, executor_owner_token, executor_lease_expires_at "
            "FROM source_broker_v2_outbox WHERE phase = 'source_finalize'"
        ).fetchone()
    assert finalize_row is not None
    assert finalize_row[:2] == (0, None)
    assert finalize_row[4:] == (None, None)
    first_finalize_deadline = datetime.fromisoformat(str(finalize_row[2]))
    first_finalize_takeover = datetime.fromisoformat(str(finalize_row[3]))
    assert first_finalize_deadline < first_finalize_takeover <= clock.now()

    recovered_finalize = finalize_saga.reconcile(
        finalize_request,
        now=NOW + timedelta(seconds=2),
    )

    assert recovered_finalize.state is SourceBrokerV2SagaState.COMPLETE
    assert recovered_finalize.reconcile_reason is None
    assert finalize_provider.dispatch_calls == 1
    assert finalize_provider.finalize_calls == 1
    assert len(finalize_lineage.calls) == 1
    assert finalize_claim_operations
    assert all(
        operation_id
        == source_effect_operation_id(
            saga_id="saga-finalize-expired-before-invoke",
            phase=phase,
        )
        for phase, operation_id in finalize_claim_operations
    )
    with sqlite3.connect(finalize_path) as connection:
        (
            active_attempt_id,
            active_owner_hash,
            active_generation,
            active_deadline,
            active_takeover,
            finalize_status,
            effect_operation_id,
        ) = connection.execute(
            "SELECT source_attempt_id, source_attempt_owner_hash, "
            "source_attempt_generation, max_external_deadline, "
            "not_before_takeover_at, status, operation_id "
            "FROM source_broker_v2_outbox WHERE phase = 'source_finalize'"
        ).fetchone()
        finalize_receipts = dict(
            connection.execute(
                "SELECT status, COUNT(*) FROM source_broker_v2_source_receipt "
                "WHERE phase = 'source_finalize' GROUP BY status"
            ).fetchall()
        )
        local_attempt_table = connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' "
            "AND name = 'source_broker_v2_source_attempt'"
        ).fetchone()
    assert local_attempt_table is None
    assert datetime.fromisoformat(str(active_deadline)) > first_finalize_deadline
    assert active_attempt_id == source_claim_attempt_id(
        effect_operation_id=effect_operation_id,
        executor_owner_token_hash=active_owner_hash,
        executor_generation=active_generation,
        max_external_deadline=datetime.fromisoformat(str(active_deadline)),
        not_before_takeover_at=datetime.fromisoformat(str(active_takeover)),
    )
    assert finalize_status == "applied"
    assert finalize_receipts == {"DEFINITIVELY_ABSENT": 2, "SUCCESS": 1}
    with sqlite3.connect(finalize_service.ledger_path) as connection:
        provider_effect = connection.execute(
            "SELECT max_external_deadline, active_claim_attempt_id "
            "FROM source_broker_v2_provider_operation WHERE operation_id = ?",
            (effect_operation_id,),
        ).fetchone()
        provider_attempts = connection.execute(
            "SELECT attempt_id, executor_owner_token_hash, executor_generation, "
            "max_external_deadline, not_before_takeover_at "
            "FROM source_broker_v2_provider_claim_attempt "
            "WHERE effect_operation_id = ? ORDER BY created_at",
            (effect_operation_id,),
        ).fetchall()
    assert provider_effect == (first_finalize_deadline.isoformat(), active_attempt_id)
    assert len(provider_attempts) == 2
    assert provider_attempts[0][0] != provider_attempts[1][0]
    for attempt_row in provider_attempts:
        assert attempt_row[0] == source_claim_attempt_id(
            effect_operation_id=effect_operation_id,
            executor_owner_token_hash=attempt_row[1],
            executor_generation=attempt_row[2],
            max_external_deadline=datetime.fromisoformat(attempt_row[3]),
            not_before_takeover_at=datetime.fromisoformat(attempt_row[4]),
        )
    assert provider_attempts[-1][0] == active_attempt_id


def test_v2_saga_rejects_tampered_historical_source_authority_receipt(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    request, current, quota = _request(tmp_path)
    path = tmp_path / "saga.sqlite3"
    transport = _TestTransport()
    lineage = _TestLineageAuthority()
    saga = SourceBrokerV2Saga.for_nonproduction(
        path,
        saga_id="saga-a",
        current_claim_authority=current,
        quota_adapter=quota,
        transport=transport,
        lineage_authority=lineage,
        source_request_deadline_seconds=_UNCONSTRAINED_SOURCE_DEADLINE_SECONDS,
        source_takeover_grace_seconds=_UNCONSTRAINED_SOURCE_TAKEOVER_GRACE_SECONDS,
    )
    saga.advance(request, now=NOW + timedelta(seconds=1))
    with sqlite3.connect(path) as connection:
        rowid, raw = connection.execute(
            "SELECT rowid, receipt_json FROM source_broker_v2_source_receipt ORDER BY rowid LIMIT 1"
        ).fetchone()
        receipt = SourceBrokerV2ClaimOnceResponse.model_validate_json(raw)
        forged = receipt.model_copy(
            update={"signature": base64.b64encode(b"0" * 64).decode("ascii")}
        )
        forged_raw = canonical_model_json_bytes(forged)
        connection.execute(
            "UPDATE source_broker_v2_source_receipt SET receipt_hash = ?, "
            "receipt_json = ? WHERE rowid = ?",
            (forged.receipt_hash, forged_raw.decode("utf-8"), rowid),
        )

    restarted = SourceBrokerV2Saga.for_nonproduction(
        path,
        saga_id="saga-a",
        current_claim_authority=current,
        quota_adapter=quota,
        transport=transport,
        lineage_authority=lineage,
        source_request_deadline_seconds=_UNCONSTRAINED_SOURCE_DEADLINE_SECONDS,
        source_takeover_grace_seconds=_UNCONSTRAINED_SOURCE_TAKEOVER_GRACE_SECONDS,
    )
    with pytest.raises(SourceBrokerV2SagaIntegrityError, match="source receipt history"):
        restarted.advance(request, now=NOW + timedelta(seconds=2))

    replay_root = tmp_path / "replay-forgery"
    replay_root.mkdir()
    replay_request, replay_current, replay_quota = _request(
        replay_root,
        saga_id="saga-replay-forgery",
    )
    replay_transport = _TestTransport()
    replay_lineage = _TestLineageAuthority()
    SourceBrokerV2Saga.for_nonproduction(
        replay_root / "authority-seed.sqlite3",
        saga_id="saga-replay-forgery",
        current_claim_authority=replay_current,
        quota_adapter=replay_quota,
        transport=replay_transport,
        lineage_authority=replay_lineage,
        source_request_deadline_seconds=_UNCONSTRAINED_SOURCE_DEADLINE_SECONDS,
        source_takeover_grace_seconds=_UNCONSTRAINED_SOURCE_TAKEOVER_GRACE_SECONDS,
    ).advance(replay_request, now=NOW + timedelta(seconds=1))
    original_replay = replay_transport.replay
    security = replay_transport._security

    def assert_forged_replay_rejected(
        *,
        label: str,
        forge: Callable[
            [SourceBrokerV2ReplayResponse],
            SourceBrokerV2ReplayResponse,
        ],
    ) -> None:
        case_root = replay_root / label
        case_root.mkdir()

        def forged_replay(
            payload: bytes,
            *,
            deadline: float | None = None,
        ) -> bytes:
            valid = SourceBrokerV2ReplayResponse.model_validate_json(
                original_replay(payload, deadline=deadline)
            )
            return canonical_model_json_bytes(forge(valid))

        with monkeypatch.context() as patch:
            patch.setattr(replay_transport, "replay", forged_replay)
            forged_saga = SourceBrokerV2Saga.for_nonproduction(
                case_root / "saga.sqlite3",
                saga_id="saga-replay-forgery",
                current_claim_authority=replay_current,
                quota_adapter=replay_quota,
                transport=replay_transport,
                lineage_authority=replay_lineage,
                source_request_deadline_seconds=_UNCONSTRAINED_SOURCE_DEADLINE_SECONDS,
                source_takeover_grace_seconds=_UNCONSTRAINED_SOURCE_TAKEOVER_GRACE_SECONDS,
            )
            with pytest.raises(SourceBrokerV2SagaIntegrityError):
                forged_saga.advance(
                    replay_request,
                    now=NOW + timedelta(seconds=2),
                )
        assert replay_transport.dispatch_calls == 1
        assert replay_transport.finalize_calls == 1

    outer_mutations: tuple[tuple[str, dict[str, object], bool], ...] = (
        (
            "signature",
            {"signature": base64.b64encode(b"x" * 64).decode("ascii")},
            False,
        ),
        ("challenge", {"challenge": "a" * 64}, True),
        ("request-hash", {"request_hash": "b" * 64}, True),
        ("binding", {"operation_id": "d" * 64}, True),
    )

    for label, updates, resign in outer_mutations:

        def forge_outer(
            valid: SourceBrokerV2ReplayResponse,
            *,
            updates: dict[str, object] = updates,
            resign: bool = resign,
        ) -> SourceBrokerV2ReplayResponse:
            forged = valid.model_copy(update=updates)
            if resign:
                forged = forged.model_copy(
                    update={"signature": security.sign(forged.signing_bytes())}
                )
            return forged

        assert_forged_replay_rejected(label=label, forge=forge_outer)

    result_binding_mutations: tuple[tuple[SourceBrokerV2OutboxPhase, str, object], ...] = (
        (SourceBrokerV2OutboxPhase.DISPATCH, "saga_id", "forged-saga"),
        (SourceBrokerV2OutboxPhase.DISPATCH, "operation_id", "e" * 64),
        (SourceBrokerV2OutboxPhase.DISPATCH, "request_hash", "f" * 64),
        (SourceBrokerV2OutboxPhase.SOURCE_FINALIZE, "saga_id", "forged-saga"),
        (SourceBrokerV2OutboxPhase.SOURCE_FINALIZE, "operation_id", "e" * 64),
        (SourceBrokerV2OutboxPhase.SOURCE_FINALIZE, "request_hash", "f" * 64),
    )

    for phase, field, replacement in result_binding_mutations:

        def forge_result_binding(
            valid: SourceBrokerV2ReplayResponse,
            *,
            phase: SourceBrokerV2OutboxPhase = phase,
            field: str = field,
            replacement: object = replacement,
        ) -> SourceBrokerV2ReplayResponse:
            if valid.phase is not phase:
                return valid
            assert valid.result is not None
            response_type = (
                SourceBrokerV2DispatchResponse
                if phase is SourceBrokerV2OutboxPhase.DISPATCH
                else SourceBrokerV2FinalizeResponse
            )
            embedded = response_type.model_validate_json(valid.result)
            forged_result = canonical_model_json_bytes(
                embedded.model_copy(update={field: replacement})
            )
            forged = valid.model_copy(
                update={
                    "result": forged_result,
                    "result_hash": canonical_sha256(strict_canonical_json_loads(forged_result)),
                }
            )
            return forged.model_copy(update={"signature": security.sign(forged.signing_bytes())})

        case_root = replay_root / f"result-{phase.value}-{field.replace('_', '-')}"
        case_root.mkdir()
        operation_id = source_effect_operation_id(
            saga_id="saga-replay-forgery",
            phase=phase,
        )
        stored_result = (
            replay_transport._dispatch_results
            if phase is SourceBrokerV2OutboxPhase.DISPATCH
            else replay_transport._finalize_results
        )[operation_id]
        response_type = (
            SourceBrokerV2DispatchResponse
            if phase is SourceBrokerV2OutboxPhase.DISPATCH
            else SourceBrokerV2FinalizeResponse
        )
        operation_request_hash = response_type.model_validate_json(stored_result).request_hash

        def forged_result_replay(
            payload: bytes,
            *,
            deadline: float | None = None,
        ) -> bytes:
            valid = SourceBrokerV2ReplayResponse.model_validate_json(
                original_replay(payload, deadline=deadline)
            )
            return canonical_model_json_bytes(forge_result_binding(valid))

        with monkeypatch.context() as patch:
            patch.setattr(replay_transport, "replay", forged_result_replay)
            forged_saga = SourceBrokerV2Saga.for_nonproduction(
                case_root / "saga.sqlite3",
                saga_id="saga-replay-forgery",
                current_claim_authority=replay_current,
                quota_adapter=replay_quota,
                transport=replay_transport,
                lineage_authority=replay_lineage,
                source_request_deadline_seconds=_UNCONSTRAINED_SOURCE_DEADLINE_SECONDS,
                source_takeover_grace_seconds=_UNCONSTRAINED_SOURCE_TAKEOVER_GRACE_SECONDS,
            )
            with pytest.raises(
                SourceBrokerV2SagaIntegrityError,
                match="source terminal result is invalid",
            ):
                forged_saga._replay_source_operation(
                    phase=phase,
                    operation_id=operation_id,
                    operation_request_hash=operation_request_hash,
                )


def test_v2_saga_lookup_unavailable_never_dispatches_and_requires_reconcile(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    request, current, quota = _request(tmp_path)
    transport = _TestTransport(claim_once_unavailable=True)
    saga = SourceBrokerV2Saga.for_nonproduction(
        tmp_path / "saga.sqlite3",
        saga_id="saga-a",
        current_claim_authority=current,
        quota_adapter=quota,
        transport=transport,
        lineage_authority=_TestLineageAuthority(),
        source_request_deadline_seconds=_UNCONSTRAINED_SOURCE_DEADLINE_SECONDS,
        source_takeover_grace_seconds=_UNCONSTRAINED_SOURCE_TAKEOVER_GRACE_SECONDS,
    )

    blocked = saga.advance(request, now=NOW + timedelta(seconds=1))

    # Named, not just counted: a RECONCILE_REQUIRED whose reason is the source
    # deadline would satisfy the state assertion while proving nothing about the
    # unavailable lookup this case is here for.
    assert blocked.state is SourceBrokerV2SagaState.RECONCILE_REQUIRED
    assert blocked.reconcile_reason == (
        "source claim_once authority is unavailable; dispatch is forbidden"
    )
    assert transport.dispatch_calls == 0
    transport.claim_once_unavailable = False
    recovered = saga.reconcile(request, now=NOW + timedelta(seconds=2))
    assert recovered.state is SourceBrokerV2SagaState.COMPLETE
    assert transport.dispatch_calls == 1

    unknown_root = tmp_path / "signed-unknown"
    unknown_root.mkdir()
    unknown_request, unknown_current, unknown_quota = _request(
        unknown_root,
        saga_id="saga-signed-unknown",
    )
    unknown_transport = _TestTransport()
    unknown_replay_calls = 0

    def signed_unknown_replay(
        payload: bytes,
        *,
        deadline: float | None = None,
    ) -> bytes:
        nonlocal unknown_replay_calls
        del deadline
        unknown_replay_calls += 1
        replay_request = SourceBrokerV2ReplayRequest.model_validate_json(payload)
        return canonical_model_json_bytes(
            _signed_source_replay(
                replay_request,
                unknown_transport._security,
                status=SourceBrokerV2ReplayStatus.UNKNOWN,
            )
        )

    monkeypatch.setattr(unknown_transport, "replay", signed_unknown_replay)
    unknown_saga = SourceBrokerV2Saga.for_nonproduction(
        unknown_root / "saga.sqlite3",
        saga_id="saga-signed-unknown",
        current_claim_authority=unknown_current,
        quota_adapter=unknown_quota,
        transport=unknown_transport,
        lineage_authority=_TestLineageAuthority(),
        source_request_deadline_seconds=_UNCONSTRAINED_SOURCE_DEADLINE_SECONDS,
        source_takeover_grace_seconds=_UNCONSTRAINED_SOURCE_TAKEOVER_GRACE_SECONDS,
    )

    first_unknown = unknown_saga.advance(
        unknown_request,
        now=NOW + timedelta(seconds=1),
    )
    second_unknown = unknown_saga.reconcile(
        unknown_request,
        now=NOW + timedelta(seconds=2),
    )

    # Same reasoning as above, and it bites harder here: this half expects
    # RECONCILE_REQUIRED, so a deadline expiry would have passed silently.
    unknown_reason = "source authority cannot determine the dispatch outcome"
    assert first_unknown.state is SourceBrokerV2SagaState.RECONCILE_REQUIRED
    assert first_unknown.reconcile_reason == unknown_reason
    assert second_unknown.state is SourceBrokerV2SagaState.RECONCILE_REQUIRED
    assert second_unknown.reconcile_reason == unknown_reason
    assert unknown_replay_calls == 2
    assert unknown_transport.claim_once_calls == 0
    assert unknown_transport.dispatch_calls == 0


def test_v2_saga_takeover_waits_for_persisted_source_cooldown(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    request, current, quota = _request(tmp_path)
    path = tmp_path / "saga.sqlite3"
    clock = _MutableUtcClock(datetime.now(UTC))
    clock.install_source_broker_clock(monkeypatch)
    transport = _TestTransport(clock=lambda: clock.now())
    lineage = _TestLineageAuthority()
    first = SourceBrokerV2Saga.for_nonproduction(
        path,
        saga_id="saga-a",
        current_claim_authority=current,
        quota_adapter=quota,
        transport=transport,
        lineage_authority=lineage,
        executor_lease_seconds=0.05,
        executor_wait_seconds=0.01,
        source_request_deadline_seconds=0.08,
        source_takeover_grace_seconds=0.04,
    )

    def crash_before_invoke(phase: SourceBrokerV2OutboxPhase) -> None:
        if phase is SourceBrokerV2OutboxPhase.DISPATCH:
            raise SystemExit("owner stopped before source transport")

    monkeypatch.setattr(first, "_before_external_effect", crash_before_invoke)
    with pytest.raises(SystemExit, match="before source transport"):
        first.advance(request, now=NOW + timedelta(seconds=1))
    clock.advance(0.055)

    takeover = SourceBrokerV2Saga.for_nonproduction(
        path,
        saga_id="saga-a",
        current_claim_authority=current,
        quota_adapter=quota,
        transport=transport,
        lineage_authority=lineage,
        executor_lease_seconds=0.05,
        executor_wait_seconds=0.1,
        source_request_deadline_seconds=0.08,
        source_takeover_grace_seconds=0.04,
    )
    waiting = takeover.advance(request, now=NOW + timedelta(seconds=2))

    assert waiting.state is SourceBrokerV2SagaState.RECONCILE_REQUIRED
    assert transport.dispatch_calls == 0
    clock.advance(0.08)
    recovered = takeover.reconcile(request, now=NOW + timedelta(seconds=3))
    assert recovered.state is SourceBrokerV2SagaState.COMPLETE
    assert transport.dispatch_calls == 1


def test_v2_saga_heartbeat_failure_late_result_recovers_without_thread_leak(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    request, current, quota = _request(tmp_path)
    path = tmp_path / "saga.sqlite3"
    clock = _MutableUtcClock(datetime.now(UTC))
    clock.install_source_broker_clock(monkeypatch)
    transport = _TestTransport(block_dispatch=True, clock=lambda: clock.now())
    lineage = _TestLineageAuthority()
    first = SourceBrokerV2Saga.for_nonproduction(
        path,
        saga_id="saga-a",
        current_claim_authority=current,
        quota_adapter=quota,
        transport=transport,
        lineage_authority=lineage,
        executor_lease_seconds=0.05,
        executor_wait_seconds=0.2,
        source_request_deadline_seconds=0.08,
        source_takeover_grace_seconds=0.04,
    )
    original_heartbeat = first._heartbeat_outbox
    heartbeat_failed = Event()

    def fail_background_heartbeat(**kwargs: object) -> None:
        if kwargs.get(
            "phase"
        ) is SourceBrokerV2OutboxPhase.DISPATCH and current_thread().name.startswith(
            "rquant-source-broker-v2-heartbeat-"
        ):
            heartbeat_failed.set()
            raise ConnectionError("heartbeat authority unavailable")
        original_heartbeat(**kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(first, "_heartbeat_outbox", fail_background_heartbeat)

    with ThreadPoolExecutor(max_workers=2) as executor:
        first_future = executor.submit(
            first.advance,
            request,
            now=NOW + timedelta(seconds=1),
        )
        if not transport.dispatch_entered.wait(timeout=5):
            first_future.result(timeout=1)
            pytest.fail("source dispatch was not entered")
        assert heartbeat_failed.wait(timeout=5)
        clock.advance(0.07)
        second = SourceBrokerV2Saga.for_nonproduction(
            path,
            saga_id="saga-a",
            current_claim_authority=current,
            quota_adapter=quota,
            transport=transport,
            lineage_authority=lineage,
            executor_lease_seconds=0.05,
            executor_wait_seconds=0.2,
            source_request_deadline_seconds=0.08,
            source_takeover_grace_seconds=0.04,
        )
        second_future = executor.submit(
            second.advance,
            request,
            now=NOW + timedelta(seconds=2),
        )
        second_result = second_future.result(timeout=5)
        assert second_result.state is SourceBrokerV2SagaState.RECONCILE_REQUIRED
        assert transport.dispatch_calls == 1
        transport.release_dispatch.set()
        first_result = first_future.result(timeout=5)

    assert first_result.state is SourceBrokerV2SagaState.RECONCILE_REQUIRED
    recovered = second.reconcile(request, now=NOW + timedelta(seconds=3))
    assert recovered.state is SourceBrokerV2SagaState.COMPLETE
    assert transport.dispatch_calls == 1
    deadline = time.monotonic() + 1
    while time.monotonic() < deadline and any(
        thread.name.startswith("rquant-source-broker-v2-heartbeat-")
        for thread in enumerate_threads()
    ):
        time.sleep(0.01)
    assert not any(
        thread.name.startswith("rquant-source-broker-v2-heartbeat-")
        for thread in enumerate_threads()
    )


def test_v2_saga_owner_crash_before_dispatch_invoke_is_taken_over_after_lease(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    request, current, quota = _request(tmp_path)
    path = tmp_path / "saga.sqlite3"
    clock = _MutableUtcClock(datetime.now(UTC))
    clock.install_source_broker_clock(monkeypatch)
    transport = _TestTransport(clock=lambda: clock.now())
    lineage = _TestLineageAuthority()
    first = SourceBrokerV2Saga.for_nonproduction(
        path,
        saga_id="saga-a",
        current_claim_authority=current,
        quota_adapter=quota,
        transport=transport,
        lineage_authority=lineage,
        executor_lease_seconds=0.05,
        executor_wait_seconds=0.01,
        source_request_deadline_seconds=0.05,
        source_takeover_grace_seconds=0.0,
    )

    def crash_before_invoke(phase: SourceBrokerV2OutboxPhase) -> None:
        if phase is SourceBrokerV2OutboxPhase.DISPATCH:
            raise SystemExit("owner crashed before source invoke")

    monkeypatch.setattr(first, "_before_external_effect", crash_before_invoke)
    with pytest.raises(SystemExit, match="before source invoke"):
        first.advance(request, now=NOW + timedelta(seconds=1))

    clock.advance(0.06)
    restarted = SourceBrokerV2Saga.for_nonproduction(
        path,
        saga_id="saga-a",
        current_claim_authority=current,
        quota_adapter=quota,
        transport=transport,
        lineage_authority=lineage,
        executor_lease_seconds=0.05,
        executor_wait_seconds=0.2,
        source_request_deadline_seconds=0.05,
        source_takeover_grace_seconds=0.0,
    )
    result = restarted.advance(request, now=NOW + timedelta(seconds=2))

    assert result.state is SourceBrokerV2SagaState.COMPLETE
    assert transport.dispatch_calls == 1
    with sqlite3.connect(path) as connection:
        generation = connection.execute(
            "SELECT executor_generation FROM source_broker_v2_outbox WHERE phase = 'dispatch'"
        ).fetchone()[0]
    assert generation == 2


def test_v2_saga_owner_crash_after_dispatch_invoke_recovers_without_redispatch(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    request, current, quota = _request(tmp_path)
    path = tmp_path / "saga.sqlite3"
    clock = _MutableUtcClock(datetime.now(UTC))
    clock.install_source_broker_clock(monkeypatch)
    transport = _TestTransport(clock=lambda: clock.now())
    lineage = _TestLineageAuthority()
    first = SourceBrokerV2Saga.for_nonproduction(
        path,
        saga_id="saga-a",
        current_claim_authority=current,
        quota_adapter=quota,
        transport=transport,
        lineage_authority=lineage,
        executor_lease_seconds=0.05,
        executor_wait_seconds=0.01,
    )

    def crash_after_invoke(phase: SourceBrokerV2OutboxPhase) -> None:
        if phase is SourceBrokerV2OutboxPhase.DISPATCH:
            raise ConnectionError("owner crashed after source invoke")

    monkeypatch.setattr(first, "_after_external_effect", crash_after_invoke)
    first_result = first.advance(request, now=NOW + timedelta(seconds=1))
    assert first_result.state is SourceBrokerV2SagaState.RECONCILE_REQUIRED
    clock.advance(0.06)

    restarted = SourceBrokerV2Saga.for_nonproduction(
        path,
        saga_id="saga-a",
        current_claim_authority=current,
        quota_adapter=quota,
        transport=transport,
        lineage_authority=lineage,
        executor_lease_seconds=0.05,
        executor_wait_seconds=0.2,
    )
    result = restarted.reconcile(request, now=NOW + timedelta(seconds=2))

    assert result.state is SourceBrokerV2SagaState.COMPLETE
    assert transport.dispatch_calls == 1
    assert transport.claim_once_calls > 0


def test_v2_saga_complete_reverifies_current_claim_and_rejects_new_generation(
    tmp_path: Path,
) -> None:
    request, current, quota = _request(tmp_path)
    saga = SourceBrokerV2Saga.for_nonproduction(
        tmp_path / "saga.sqlite3",
        saga_id="saga-a",
        current_claim_authority=current,
        quota_adapter=quota,
        transport=_TestTransport(),
        lineage_authority=_TestLineageAuthority(),
        source_request_deadline_seconds=_UNCONSTRAINED_SOURCE_DEADLINE_SECONDS,
        source_takeover_grace_seconds=_UNCONSTRAINED_SOURCE_TAKEOVER_GRACE_SECONDS,
    )
    saga.advance(request, now=NOW + timedelta(seconds=1))
    old = current.current_claim
    current.replace_current(
        old.model_copy(
            update={
                "claim_generation": old.claim_generation + 1,
                "scheduler_fencing_token": old.scheduler_fencing_token + 1,
            }
        )
    )

    with pytest.raises(SourceBrokerV2SagaConflictError, match="current"):
        saga.advance(request, now=NOW + timedelta(seconds=2))


def test_v2_saga_deleted_local_db_recovers_authority_effects_without_redispatch(
    tmp_path: Path,
) -> None:
    request, current, quota = _request(tmp_path)
    path = tmp_path / "saga.sqlite3"
    transport = _TestTransport()
    lineage = _TestLineageAuthority()
    SourceBrokerV2Saga.for_nonproduction(
        path,
        saga_id="saga-a",
        current_claim_authority=current,
        quota_adapter=quota,
        transport=transport,
        lineage_authority=lineage,
        source_request_deadline_seconds=_UNCONSTRAINED_SOURCE_DEADLINE_SECONDS,
        source_takeover_grace_seconds=_UNCONSTRAINED_SOURCE_TAKEOVER_GRACE_SECONDS,
    ).advance(request, now=NOW + timedelta(seconds=1))
    path.unlink()

    recovered = SourceBrokerV2Saga.for_nonproduction(
        path,
        saga_id="saga-a",
        current_claim_authority=current,
        quota_adapter=quota,
        transport=transport,
        lineage_authority=lineage,
        source_request_deadline_seconds=_UNCONSTRAINED_SOURCE_DEADLINE_SECONDS,
        source_takeover_grace_seconds=_UNCONSTRAINED_SOURCE_TAKEOVER_GRACE_SECONDS,
    ).advance(request, now=NOW + timedelta(seconds=2))

    assert recovered.state is SourceBrokerV2SagaState.COMPLETE
    assert transport.dispatch_calls == 1
    assert transport.finalize_calls == 1
    assert transport.claim_once_calls > 0
    with sqlite3.connect(path) as connection:
        observation_json, observation_hash = connection.execute(
            "SELECT source_observation_json, source_observation_hash "
            "FROM source_broker_v2_outbox WHERE phase = 'dispatch'"
        ).fetchone()
        replay_history = connection.execute(
            "SELECT receipt_hash, attempt_id, status, receipt_json "
            "FROM source_broker_v2_source_receipt "
            "WHERE phase = 'dispatch' AND status = 'FOUND' ORDER BY rowid"
        ).fetchall()
    observation = SourceBrokerV2ReplayResponse.model_validate_json(observation_json)
    assert observation.status is SourceBrokerV2ReplayStatus.FOUND
    assert observation.receipt_hash == observation_hash
    assert observation.result is not None
    assert len(replay_history) == 2
    replay_receipts = [
        SourceBrokerV2ReplayResponse.model_validate_json(row[3]) for row in replay_history
    ]
    assert all(row[1:3] == (None, "FOUND") for row in replay_history)
    assert all(
        row[0] == receipt.receipt_hash
        for row, receipt in zip(replay_history, replay_receipts, strict=True)
    )
    assert len({receipt.challenge for receipt in replay_receipts}) == 2
    assert all(receipt.operation_id == observation.operation_id for receipt in replay_receipts)
    assert all(receipt.result_hash == observation.result_hash for receipt in replay_receipts)
    assert replay_history[-1][3] == observation_json


def test_v2_saga_old_local_snapshot_recovers_authority_head_without_redispatch(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    request, current, quota = _request(tmp_path)
    path = tmp_path / "saga.sqlite3"
    snapshot_path = tmp_path / "old-saga.sqlite3"
    transport = _TestTransport()
    lineage = _TestLineageAuthority()
    first = SourceBrokerV2Saga.for_nonproduction(
        path,
        saga_id="saga-a",
        current_claim_authority=current,
        quota_adapter=quota,
        transport=transport,
        lineage_authority=lineage,
        executor_lease_seconds=0.05,
        source_request_deadline_seconds=_UNCONSTRAINED_SOURCE_DEADLINE_SECONDS,
        source_takeover_grace_seconds=_UNCONSTRAINED_SOURCE_TAKEOVER_GRACE_SECONDS,
    )
    captured = False

    def capture_after_dispatch(phase: SourceBrokerV2OutboxPhase) -> None:
        nonlocal captured
        if phase is SourceBrokerV2OutboxPhase.DISPATCH and not captured:
            captured = True
            with (
                sqlite3.connect(path) as source,
                sqlite3.connect(snapshot_path) as target,
            ):
                source.backup(target)
            raise ConnectionError("captured old local snapshot after source effect")

    monkeypatch.setattr(first, "_after_external_effect", capture_after_dispatch)
    first_result = first.advance(request, now=NOW + timedelta(seconds=1))
    assert first_result.state is SourceBrokerV2SagaState.RECONCILE_REQUIRED
    completed = first.reconcile(request, now=NOW + timedelta(seconds=2))
    assert completed.state is SourceBrokerV2SagaState.COMPLETE
    with sqlite3.connect(snapshot_path) as legacy:
        legacy.execute("ALTER TABLE source_broker_v2_outbox DROP COLUMN source_attempt_id")
        legacy.execute("ALTER TABLE source_broker_v2_outbox DROP COLUMN source_attempt_owner_hash")
        legacy.execute("ALTER TABLE source_broker_v2_outbox DROP COLUMN source_attempt_generation")
        legacy.execute("ALTER TABLE source_broker_v2_source_receipt DROP COLUMN attempt_id")
    with (
        sqlite3.connect(snapshot_path) as source,
        sqlite3.connect(path) as target,
    ):
        source.backup(target)

    recovered = SourceBrokerV2Saga.for_nonproduction(
        path,
        saga_id="saga-a",
        current_claim_authority=current,
        quota_adapter=quota,
        transport=transport,
        lineage_authority=lineage,
        executor_lease_seconds=0.05,
        source_request_deadline_seconds=_UNCONSTRAINED_SOURCE_DEADLINE_SECONDS,
        source_takeover_grace_seconds=_UNCONSTRAINED_SOURCE_TAKEOVER_GRACE_SECONDS,
    ).advance(request, now=NOW + timedelta(seconds=3))

    assert recovered.state is SourceBrokerV2SagaState.COMPLETE
    assert transport.dispatch_calls == 1
    assert transport.claim_once_calls > 0
    with sqlite3.connect(path) as connection:
        migrated_attempt = connection.execute(
            "SELECT source_attempt_id FROM source_broker_v2_outbox WHERE phase = 'dispatch'"
        ).fetchone()[0]
        migrated_receipts = connection.execute(
            "SELECT status, attempt_id FROM source_broker_v2_source_receipt "
            "WHERE phase = 'dispatch' ORDER BY rowid"
        ).fetchall()
    assert type(migrated_attempt) is str
    assert migrated_receipts[0] == ("DEFINITIVELY_ABSENT", migrated_attempt)
    assert all(attempt_id is None for status, attempt_id in migrated_receipts if status == "FOUND")


def test_v2_saga_rejects_rehashed_forged_claim_receipt_against_authority(
    tmp_path: Path,
) -> None:
    request, current, quota = _request(tmp_path)
    path = tmp_path / "saga.sqlite3"
    saga = SourceBrokerV2Saga.for_nonproduction(
        path,
        saga_id="saga-a",
        current_claim_authority=current,
        quota_adapter=quota,
        transport=_TestTransport(),
        lineage_authority=_TestLineageAuthority(),
        source_request_deadline_seconds=_UNCONSTRAINED_SOURCE_DEADLINE_SECONDS,
        source_takeover_grace_seconds=_UNCONSTRAINED_SOURCE_TAKEOVER_GRACE_SECONDS,
    )
    saga.advance(request, now=NOW + timedelta(seconds=1))
    with sqlite3.connect(path) as connection:
        raw = connection.execute(
            "SELECT result_json FROM source_broker_v2_outbox WHERE phase = 'claim'"
        ).fetchone()[0]
        receipt = CurrentClaimConsumptionV2.model_validate_json(raw)
        forged = receipt.model_copy(
            update={"committed_at": receipt.committed_at + timedelta(microseconds=1)}
        )
        encoded = canonical_model_json_bytes(forged)
        connection.execute(
            "UPDATE source_broker_v2_outbox SET result_json = ?, result_hash = ? "
            "WHERE phase = 'claim'",
            (
                encoded.decode("utf-8"),
                canonical_sha256(strict_canonical_json_loads(encoded)),
            ),
        )

    with pytest.raises(SourceBrokerV2SagaIntegrityError, match="claim|authority"):
        saga.advance(request, now=NOW + timedelta(seconds=2))


def test_v2_saga_rejects_rehashed_forged_quota_receipt_against_native_chain(
    tmp_path: Path,
) -> None:
    request, current, quota = _request(tmp_path)
    path = tmp_path / "saga.sqlite3"
    saga = SourceBrokerV2Saga.for_nonproduction(
        path,
        saga_id="saga-a",
        current_claim_authority=current,
        quota_adapter=quota,
        transport=_TestTransport(),
        lineage_authority=_TestLineageAuthority(),
        source_request_deadline_seconds=_UNCONSTRAINED_SOURCE_DEADLINE_SECONDS,
        source_takeover_grace_seconds=_UNCONSTRAINED_SOURCE_TAKEOVER_GRACE_SECONDS,
    )
    saga.advance(request, now=NOW + timedelta(seconds=1))
    with sqlite3.connect(path) as connection:
        raw = connection.execute(
            "SELECT result_json FROM source_broker_v2_outbox WHERE phase = 'reserve_parent'"
        ).fetchone()[0]
        forged = strict_canonical_json_loads(raw.encode("utf-8"))
        forged["adapter_id"] = "forged-adapter"
        encoded = canonical_json_bytes(forged)
        connection.execute(
            "UPDATE source_broker_v2_outbox SET result_json = ?, result_hash = ? "
            "WHERE phase = 'reserve_parent'",
            (
                encoded.decode("utf-8"),
                canonical_sha256(strict_canonical_json_loads(encoded)),
            ),
        )

    with pytest.raises(SourceBrokerV2SagaIntegrityError, match="quota|native"):
        saga.advance(request, now=NOW + timedelta(seconds=2))


def test_v2_saga_rejects_rehashed_forged_lineage_receipt_against_authority(
    tmp_path: Path,
) -> None:
    request, current, quota = _request(tmp_path)
    path = tmp_path / "saga.sqlite3"
    lineage = _TestLineageAuthority()
    saga = SourceBrokerV2Saga.for_nonproduction(
        path,
        saga_id="saga-a",
        current_claim_authority=current,
        quota_adapter=quota,
        transport=_TestTransport(),
        lineage_authority=lineage,
        source_request_deadline_seconds=_UNCONSTRAINED_SOURCE_DEADLINE_SECONDS,
        source_takeover_grace_seconds=_UNCONSTRAINED_SOURCE_TAKEOVER_GRACE_SECONDS,
    )
    saga.advance(request, now=NOW + timedelta(seconds=1))
    with sqlite3.connect(path) as connection:
        raw = connection.execute(
            "SELECT result_json FROM source_broker_v2_outbox WHERE phase = 'lineage'"
        ).fetchone()[0]
        receipt = ReplayLineageCheckpointReceipt.model_validate_json(raw)
        forged = receipt.model_copy(update={"signature": "forged"})
        encoded = canonical_model_json_bytes(forged)
        connection.execute(
            "UPDATE source_broker_v2_outbox SET result_json = ?, result_hash = ? "
            "WHERE phase = 'lineage'",
            (
                encoded.decode("utf-8"),
                canonical_sha256(strict_canonical_json_loads(encoded)),
            ),
        )

    with pytest.raises(SourceBrokerV2SagaIntegrityError, match="lineage|authority"):
        saga.advance(request, now=NOW + timedelta(seconds=2))


def test_v2_saga_missing_native_source_after_local_complete_requires_repair(
    tmp_path: Path,
) -> None:
    request, current, quota = _request(tmp_path)
    path = tmp_path / "saga.sqlite3"
    transport = _TestTransport()
    saga = SourceBrokerV2Saga.for_nonproduction(
        path,
        saga_id="saga-a",
        current_claim_authority=current,
        quota_adapter=quota,
        transport=transport,
        lineage_authority=_TestLineageAuthority(),
        source_request_deadline_seconds=_UNCONSTRAINED_SOURCE_DEADLINE_SECONDS,
        source_takeover_grace_seconds=_UNCONSTRAINED_SOURCE_TAKEOVER_GRACE_SECONDS,
    )
    saga.advance(request, now=NOW + timedelta(seconds=1))
    transport._dispatch_results.clear()

    with pytest.raises(SourceBrokerV2SagaRepairRequiredError, match="source"):
        saga.advance(request, now=NOW + timedelta(seconds=2))


@pytest.mark.parametrize(
    "phase",
    (
        "claim",
        "reserve_parent",
        "record_intent",
        "authorize_dispatch",
        "dispatch",
        "source_finalize",
        "quota_finalize",
        "release_unused",
        "lineage",
    ),
)
def test_v2_saga_recovers_each_external_effect_after_commit_response_loss(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    phase: str,
) -> None:
    request, current, quota = _request(tmp_path)
    path = tmp_path / "saga.sqlite3"
    transport = _TestTransport()
    lineage = _TestLineageAuthority()
    first = SourceBrokerV2Saga.for_nonproduction(
        path,
        saga_id="saga-a",
        current_claim_authority=current,
        quota_adapter=quota,
        transport=transport,
        lineage_authority=lineage,
        source_request_deadline_seconds=_UNCONSTRAINED_SOURCE_DEADLINE_SECONDS,
        source_takeover_grace_seconds=_UNCONSTRAINED_SOURCE_TAKEOVER_GRACE_SECONDS,
    )
    target = SourceBrokerV2OutboxPhase(phase)
    fired = False

    def lose_after_effect(observed: SourceBrokerV2OutboxPhase) -> None:
        nonlocal fired
        if observed is target and not fired:
            fired = True
            raise ConnectionError(f"{phase} committed before response loss")

    monkeypatch.setattr(first, "_after_external_effect", lose_after_effect)
    first_result: object | None = None
    with suppress(ConnectionError):
        first_result = first.advance(request, now=NOW + timedelta(seconds=1))

    restarted = SourceBrokerV2Saga.for_nonproduction(
        path,
        saga_id="saga-a",
        current_claim_authority=current,
        quota_adapter=quota,
        transport=transport,
        lineage_authority=lineage,
        source_request_deadline_seconds=_UNCONSTRAINED_SOURCE_DEADLINE_SECONDS,
        source_takeover_grace_seconds=_UNCONSTRAINED_SOURCE_TAKEOVER_GRACE_SECONDS,
    )
    if (
        first_result is not None
        and first_result.state is SourceBrokerV2SagaState.RECONCILE_REQUIRED
    ):
        result = restarted.reconcile(request, now=NOW + timedelta(seconds=2))
    else:
        result = restarted.advance(request, now=NOW + timedelta(seconds=2))

    assert fired
    assert result.state is SourceBrokerV2SagaState.COMPLETE
    assert transport.dispatch_calls == 1
