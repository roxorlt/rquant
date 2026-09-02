from __future__ import annotations

import ast
import base64
import fcntl
import gc
import json
import multiprocessing
import os
import re
import select
import shutil
import socket
import sqlite3
import subprocess
import sys
import tempfile
import time
import weakref
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
import rquant.source_broker_v2_heartbeat as source_broker_v2_heartbeat
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


# The floor this package removed.  ``max(0.1, (lease / 3) * 2)`` is 0.1s for
# every lease below 0.15s, and what that floor had to cover - one in-flight
# ``_heartbeat_outbox`` - is bounded by ``busy_timeout_ms``, not by it.
_LEGACY_HEARTBEAT_SHUTDOWN_BUDGET_SECONDS = 0.1
# How long the competitor keeps the SQLite write lock once the barrier has put
# the heartbeat inside its write.  `time.sleep` never returns early, so this is
# a guaranteed lower bound on the in-flight write, not a window to hit: the
# interleaving itself is fixed by the barrier, and the case asserts the
# observed write duration afterwards rather than trusting this number.
_PINNED_WRITE_HOLD_SECONDS = 0.6
_HEARTBEAT_THREAD_PREFIX = "rquant-source-broker-v2-heartbeat-"


def _is_background_dispatch_heartbeat(kwargs: dict[str, Any]) -> bool:
    return kwargs.get(
        "phase"
    ) is SourceBrokerV2OutboxPhase.DISPATCH and current_thread().name.startswith(
        _HEARTBEAT_THREAD_PREFIX
    )


def _live_heartbeat_threads() -> list[Thread]:
    return [
        thread for thread in enumerate_threads() if thread.name.startswith(_HEARTBEAT_THREAD_PREFIX)
    ]


# A read long enough to still be running when a 0.08s budget expires, and
# short enough (~15s at the ~13M rows/s this tree measures) to end by itself if
# nothing ever interrupts it, so a tree without the interrupt leaves a bounded
# mess behind instead of an endless one.
_STALLING_SCAN_ROWS = 200_000_000
_STALLING_SCAN_SQL = (
    "WITH RECURSIVE stall(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM stall "
    f"WHERE x < {_STALLING_SCAN_ROWS}) SELECT count(*) FROM stall"
)
# Virtual-machine steps between progress callbacks: small enough that the first
# one lands within milliseconds of the scan starting, large enough not to slow
# it down.  The callback is how a case learns the statement is really running.
_SCAN_PROGRESS_OPS = 500_000
# The lock-wait case contends on a database of its own so the wait can be given
# a window wider than the saga's shutdown budget - the saga's own
# `busy_timeout_ms` sets both the budget and its writes' waits, so a lock wait
# on the saga file can never outlive the budget derived from it.
_CONTENDED_BUSY_TIMEOUT_MS = 600


def _record_heartbeat_threads(monkeypatch: pytest.MonkeyPatch) -> list[Thread]:
    """Every heartbeat thread `_invoke_with_heartbeat` starts, in order."""

    created: list[Thread] = []
    real_thread = source_broker_v2_module.Thread

    def factory(*args: Any, **kwargs: Any) -> Thread:
        thread = real_thread(*args, **kwargs)
        if str(kwargs.get("name", "")).startswith(_HEARTBEAT_THREAD_PREFIX):
            created.append(thread)
        return thread

    monkeypatch.setattr(source_broker_v2_module, "Thread", factory)
    return created


def _assert_heartbeats_end_at_the_boundary(
    saga: SourceBrokerV2Saga,
    monkeypatch: pytest.MonkeyPatch,
    threads: list[Thread],
) -> None:
    """Check for a live heartbeat where `_invoke_with_heartbeat` leaves.

    Both assertions run before anything else can touch the thread - the one in
    `except` is the first statement to execute after the raise - so a thread
    still alive there is one the method itself handed back.  No case releases,
    joins or reclaims a heartbeat afterwards to make this true.
    """

    original = saga._invoke_with_heartbeat

    def bounded(**kwargs: Any) -> bytes:
        started = len(threads)
        try:
            result = original(**kwargs)
        except BaseException:
            assert not [thread for thread in threads[started:] if thread.is_alive()]
            raise
        assert not [thread for thread in threads[started:] if thread.is_alive()]
        return result

    monkeypatch.setattr(saga, "_invoke_with_heartbeat", bounded)


def _stall_the_second_dispatch_renewal(
    saga: SourceBrokerV2Saga,
    monkeypatch: pytest.MonkeyPatch,
    *,
    scanning: Event,
) -> tuple[list[float], list[str]]:
    """Park the second dispatch renewal inside a SQLite read it cannot finish.

    The scan runs on the connection `_heartbeat_outbox` opened and published,
    inside the write transaction that body had already begun and after its
    `UPDATE`, so the shutdown's interrupt has to end a statement the production
    body owns - and SQLite has to roll that renewal back rather than leave it
    half applied.  `scanning` is set from inside the running statement by a
    progress handler, so no case has to guess when the thread got there.

    Returns the durations of the stalled scan and the `executor_heartbeat_at`
    values the renewals read before writing their own.
    """

    connections: list[sqlite3.Connection] = []
    scans: list[float] = []
    read_heartbeat_at: list[str] = []
    completed: list[float] = []
    stalled: list[bool] = []
    original_read = saga._read_outbox
    original_heartbeat = saga._heartbeat_outbox

    def capturing_read(connection: sqlite3.Connection, **kwargs: Any) -> sqlite3.Row:
        row = original_read(connection, **kwargs)
        # Phase-scoped through the same predicate the rest of the file uses:
        # every outbox phase runs a heartbeat thread of its own, and on a slow
        # machine the claim phase's renewal lands a tick here too.  That tick
        # reads a different outbox row, and counting it made this case fail
        # about 3% of the time.
        if _is_background_dispatch_heartbeat(kwargs):
            connections.append(connection)
            read_heartbeat_at.append(row["executor_heartbeat_at"])
        return row

    def report_progress() -> int:
        scanning.set()
        return 0

    def stalling_heartbeat(**kwargs: Any) -> None:
        if not _is_background_dispatch_heartbeat(kwargs) or stalled:
            original_heartbeat(**kwargs)
            return
        if not completed:
            original_heartbeat(**kwargs)
            completed.append(time.monotonic())
            return
        stalled.append(True)
        mark_stage = kwargs["mark_stage"]

        def marking(stage: str) -> None:
            mark_stage(stage)
            if stage != "commit":
                return
            connection = connections[-1]
            connection.set_progress_handler(report_progress, _SCAN_PROGRESS_OPS)
            started = time.monotonic()
            try:
                connection.execute(_STALLING_SCAN_SQL).fetchone()
            finally:
                scans.append(time.monotonic() - started)
                connection.set_progress_handler(None, 0)

        original_heartbeat(**{**kwargs, "mark_stage": marking})

    monkeypatch.setattr(saga, "_read_outbox", capturing_read)
    monkeypatch.setattr(saga, "_heartbeat_outbox", stalling_heartbeat)
    return scans, read_heartbeat_at


def _outbox_heartbeat_at(path: Path, operation_id: str) -> str:
    connection = sqlite3.connect(path, isolation_level=None)
    try:
        row = connection.execute(
            "SELECT executor_heartbeat_at FROM source_broker_v2_outbox WHERE operation_id = ?",
            (operation_id,),
        ).fetchone()
    finally:
        connection.close()
    assert row is not None
    return str(row[0])


def _heartbeat_shutdown_saga(
    path: Path,
    *,
    current: object,
    quota: object,
    transport: _TestTransport,
    lineage: _TestLineageAuthority,
    busy_timeout_ms: int,
    executor_lease_seconds: float = 0.05,
) -> SourceBrokerV2Saga:
    # lease 0.05 keeps `interval * 2` (0.033s) under the removed 0.1s floor, so
    # the shutdown budget is decided by `busy_timeout_ms` alone; the source
    # deadline is left unconstrained because these cases are about the budget.
    return SourceBrokerV2Saga.for_nonproduction(
        path,
        saga_id="saga-a",
        current_claim_authority=current,
        quota_adapter=quota,  # type: ignore[arg-type]
        transport=transport,  # type: ignore[arg-type]
        lineage_authority=lineage,
        busy_timeout_ms=busy_timeout_ms,
        executor_lease_seconds=executor_lease_seconds,
        executor_wait_seconds=0.2,
        source_request_deadline_seconds=_UNCONSTRAINED_SOURCE_DEADLINE_SECONDS,
        source_takeover_grace_seconds=_UNCONSTRAINED_SOURCE_TAKEOVER_GRACE_SECONDS,
    )


def _expected_shutdown_budget(busy_timeout_ms: int) -> float:
    return source_broker_v2_module._HEARTBEAT_SHUTDOWN_LOCK_WINDOWS * busy_timeout_ms / 1_000


def test_v2_saga_shutdown_covers_a_heartbeat_pinned_inside_a_busy_timeout_bounded_write(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """In-flight ending: the renewal is inside its write when `stop` is set."""

    request, current, quota = _request(tmp_path)
    path = tmp_path / "saga.sqlite3"
    transport = _TestTransport(block_dispatch=True)
    lineage = _TestLineageAuthority()
    saga = _heartbeat_shutdown_saga(
        path,
        current=current,
        quota=quota,
        transport=transport,
        lineage=lineage,
        busy_timeout_ms=5_000,
    )
    threads = _record_heartbeat_threads(monkeypatch)
    write_barrier = Barrier(2)
    pinned: list[bool] = []
    write_durations: list[float] = []
    original_heartbeat = saga._heartbeat_outbox

    def pinned_heartbeat(**kwargs: Any) -> None:
        if not _is_background_dispatch_heartbeat(kwargs) or pinned:
            original_heartbeat(**kwargs)
            return
        pinned.append(True)
        # Rendezvous: the test already holds the SQLite write lock when this
        # returns, so the delegate below is guaranteed to enter the
        # `busy_timeout`-bounded wait at `BEGIN IMMEDIATE`.
        write_barrier.wait(timeout=5)
        started = time.monotonic()
        try:
            original_heartbeat(**kwargs)
        finally:
            write_durations.append(time.monotonic() - started)

    monkeypatch.setattr(saga, "_heartbeat_outbox", pinned_heartbeat)
    _assert_heartbeats_end_at_the_boundary(saga, monkeypatch, threads)

    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(saga.advance, request, now=NOW + timedelta(seconds=1))
        try:
            assert transport.dispatch_entered.wait(timeout=5)
            competitor = sqlite3.connect(path, timeout=5.0, isolation_level=None)
            try:
                competitor.execute("BEGIN IMMEDIATE")
                write_barrier.wait(timeout=5)
                transport.release_dispatch.set()
                time.sleep(_PINNED_WRITE_HOLD_SECONDS)
                competitor.execute("ROLLBACK")
            finally:
                competitor.close()
        finally:
            transport.release_dispatch.set()
        snapshot = future.result(timeout=30)

    assert write_durations
    assert max(write_durations) > _LEGACY_HEARTBEAT_SHUTDOWN_BUDGET_SECONDS
    assert snapshot.reconcile_reason is None
    assert snapshot.state is SourceBrokerV2SagaState.COMPLETE
    assert transport.dispatch_calls == 1
    assert transport.finalize_calls == 1
    assert not _live_heartbeat_threads()


def test_v2_saga_shutdown_returns_at_once_when_the_heartbeat_is_idle(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Normal ending: `stop` lands while the renewal is parked between ticks."""

    request, current, quota = _request(tmp_path)
    path = tmp_path / "saga.sqlite3"
    transport = _TestTransport(block_dispatch=True)
    lineage = _TestLineageAuthority()
    saga = _heartbeat_shutdown_saga(
        path,
        current=current,
        quota=quota,
        transport=transport,
        lineage=lineage,
        busy_timeout_ms=5_000,
    )
    threads = _record_heartbeat_threads(monkeypatch)
    parked = Event()
    dispatch_ticks: list[float] = []
    original_heartbeat = saga._heartbeat_outbox

    def parked_wait(stop: Event, interval: float, *, phase: SourceBrokerV2OutboxPhase) -> bool:
        if phase is not SourceBrokerV2OutboxPhase.DISPATCH:
            return stop.wait(interval)
        parked.set()
        return stop.wait(30.0)

    def count_dispatch_ticks(**kwargs: Any) -> None:
        if _is_background_dispatch_heartbeat(kwargs):
            dispatch_ticks.append(time.monotonic())
        original_heartbeat(**kwargs)

    monkeypatch.setattr(saga, "_wait_for_heartbeat", parked_wait)
    monkeypatch.setattr(saga, "_heartbeat_outbox", count_dispatch_ticks)
    _assert_heartbeats_end_at_the_boundary(saga, monkeypatch, threads)

    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(saga.advance, request, now=NOW + timedelta(seconds=1))
        try:
            assert transport.dispatch_entered.wait(timeout=5)
            assert parked.wait(timeout=5)
            released_at = time.monotonic()
            transport.release_dispatch.set()
            snapshot = future.result(timeout=30)
        finally:
            transport.release_dispatch.set()
    shutdown_elapsed = time.monotonic() - released_at

    assert dispatch_ticks == []
    assert shutdown_elapsed < _expected_shutdown_budget(5_000)
    assert snapshot.reconcile_reason is None
    assert snapshot.state is SourceBrokerV2SagaState.COMPLETE
    assert transport.dispatch_calls == 1
    assert not _live_heartbeat_threads()


def test_v2_saga_heartbeat_starts_no_further_round_once_stop_is_requested(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The wait may report a timeout at the instant `stop` is set; that is not a tick."""

    request, current, quota = _request(tmp_path)
    path = tmp_path / "saga.sqlite3"
    transport = _TestTransport(block_dispatch=True)
    lineage = _TestLineageAuthority()
    saga = _heartbeat_shutdown_saga(
        path,
        current=current,
        quota=quota,
        transport=transport,
        lineage=lineage,
        busy_timeout_ms=5_000,
    )
    threads = _record_heartbeat_threads(monkeypatch)
    raced = Event()
    parked = Event()
    dispatch_ticks: list[float] = []
    original_heartbeat = saga._heartbeat_outbox

    def racing_wait(stop: Event, interval: float, *, phase: SourceBrokerV2OutboxPhase) -> bool:
        if phase is not SourceBrokerV2OutboxPhase.DISPATCH or raced.is_set():
            return stop.wait(interval)
        raced.set()
        parked.set()
        # Return only once the shutdown has set `stop`, and report a timeout
        # anyway: exactly the race a `stop.wait(interval)` expiry can lose.
        stop.wait(30.0)
        return False

    def count_dispatch_ticks(**kwargs: Any) -> None:
        if _is_background_dispatch_heartbeat(kwargs):
            dispatch_ticks.append(time.monotonic())
        original_heartbeat(**kwargs)

    monkeypatch.setattr(saga, "_wait_for_heartbeat", racing_wait)
    monkeypatch.setattr(saga, "_heartbeat_outbox", count_dispatch_ticks)
    _assert_heartbeats_end_at_the_boundary(saga, monkeypatch, threads)

    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(saga.advance, request, now=NOW + timedelta(seconds=1))
        try:
            assert transport.dispatch_entered.wait(timeout=5)
            assert parked.wait(timeout=5)
            transport.release_dispatch.set()
            snapshot = future.result(timeout=30)
        finally:
            transport.release_dispatch.set()

    assert dispatch_ticks == []
    assert snapshot.reconcile_reason is None
    assert snapshot.state is SourceBrokerV2SagaState.COMPLETE
    assert transport.dispatch_calls == 1
    assert not _live_heartbeat_threads()


def test_v2_saga_shutdown_ends_a_heartbeat_that_outlives_its_derived_budget(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Real timeout: the renewal is still inside SQLite when the budget expires.

    Past the budget the shutdown interrupts the connection that renewal opened
    and joins with no timeout, so the method raises with the thread already
    dead - the `except` in `_assert_heartbeats_end_at_the_boundary` checks that
    before anything else runs.  The interrupted renewal is rolled back whole:
    the row still carries the timestamp the last completed tick wrote.
    """

    request, current, quota = _request(tmp_path)
    path = tmp_path / "saga.sqlite3"
    transport = _TestTransport(block_dispatch=True)
    lineage = _TestLineageAuthority()
    busy_timeout_ms = 40
    saga = _heartbeat_shutdown_saga(
        path,
        current=current,
        quota=quota,
        transport=transport,
        lineage=lineage,
        busy_timeout_ms=busy_timeout_ms,
    )
    budget = _expected_shutdown_budget(busy_timeout_ms)
    threads = _record_heartbeat_threads(monkeypatch)
    scanning = Event()
    scans, read_heartbeat_at = _stall_the_second_dispatch_renewal(
        saga,
        monkeypatch,
        scanning=scanning,
    )
    _assert_heartbeats_end_at_the_boundary(saga, monkeypatch, threads)

    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(saga.advance, request, now=NOW + timedelta(seconds=1))
        try:
            assert transport.dispatch_entered.wait(timeout=5)
            assert scanning.wait(timeout=10)
            transport.release_dispatch.set()
            snapshot = future.result(timeout=30)
        finally:
            transport.release_dispatch.set()

    assert len(scans) == 1
    assert scans[0] > budget
    assert snapshot.state is SourceBrokerV2SagaState.RECONCILE_REQUIRED
    reason = snapshot.reconcile_reason
    assert reason is not None
    assert reason.startswith("outbox heartbeat did not stop after dispatch")
    assert f"of a {budget:.3f}s budget" in reason
    assert "1 completed tick" in reason
    assert "stage 'commit'" in reason
    assert "interrupt issued on the connection it held" in reason
    assert "ending in stage 'interrupted'" in reason
    assert transport.dispatch_calls == 1
    assert not _live_heartbeat_threads()
    # SQLite rolled the interrupted renewal back: the row kept what the tick
    # before it committed, and never a half of what the interrupted one began.
    assert len(read_heartbeat_at) == 2
    dispatch_operation_id = source_effect_operation_id(
        saga_id=request.saga_id,
        phase=SourceBrokerV2OutboxPhase.DISPATCH,
    )
    assert _outbox_heartbeat_at(path, dispatch_operation_id) == read_heartbeat_at[-1]


def test_v2_saga_shutdown_ends_a_heartbeat_interrupted_inside_a_lock_wait(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A lock wait is the stall `interrupt` cannot cut short - it still ends.

    SQLite's busy handler sleeps through an interrupt, so what ends this
    renewal is the `busy_timeout` of the connection it is waiting on, not the
    abort.  That is the point: the wait the shutdown inherits is bounded by a
    window SQLite states, so joining without a timeout still terminates, and
    the message says how much of the wait came after the interrupt.  The
    contended file is not the saga's, so the shutdown that follows is never
    itself blocked by the competitor.
    """

    transport = _TestTransport()
    lineage = _TestLineageAuthority()
    saga = _heartbeat_shutdown_saga(
        tmp_path / "saga.sqlite3",
        current=object(),
        quota=_quota_adapter(tmp_path),
        transport=transport,
        lineage=lineage,
        busy_timeout_ms=40,
    )
    budget = _expected_shutdown_budget(40)
    contended = tmp_path / "contended.sqlite3"
    competitor = sqlite3.connect(contended, timeout=5.0, isolation_level=None)
    competitor.execute("PRAGMA journal_mode = WAL")
    competitor.execute("CREATE TABLE held(k INTEGER PRIMARY KEY)")
    threads = _record_heartbeat_threads(monkeypatch)
    waiting = Event()
    lock_waits: list[float] = []

    def lock_waiting_heartbeat(**kwargs: Any) -> None:
        bind_connection = kwargs["bind_connection"]
        mark_stage = kwargs["mark_stage"]
        connection = sqlite3.connect(
            contended,
            timeout=_CONTENDED_BUSY_TIMEOUT_MS / 1_000,
            isolation_level=None,
        )
        connection.execute(f"PRAGMA busy_timeout = {_CONTENDED_BUSY_TIMEOUT_MS}")
        bind_connection(connection)
        try:
            mark_stage("lock-wait")
            waiting.set()
            started = time.monotonic()
            try:
                connection.execute("BEGIN IMMEDIATE")
            finally:
                lock_waits.append(time.monotonic() - started)
        finally:
            bind_connection(None)
            connection.close()

    monkeypatch.setattr(saga, "_heartbeat_outbox", lock_waiting_heartbeat)
    monkeypatch.setattr(
        saga,
        "_wait_for_heartbeat",
        lambda stop, interval, *, phase: stop.wait(interval),
    )
    _assert_heartbeats_end_at_the_boundary(saga, monkeypatch, threads)

    def invoke(payload: bytes) -> bytes:
        assert waiting.wait(timeout=5)
        return payload

    competitor.execute("BEGIN IMMEDIATE")
    try:
        with pytest.raises(SourceBrokerV2SagaUnavailableError) as raised:
            saga._invoke_with_heartbeat(
                phase=SourceBrokerV2OutboxPhase.DISPATCH,
                operation_id="0" * 64,
                owner_generation=1,
                payload=b'{"probe":1}',
                invoke=invoke,
            )
    finally:
        competitor.execute("ROLLBACK")
        competitor.close()

    reason = str(raised.value)
    assert reason.startswith("outbox heartbeat did not stop after dispatch")
    assert f"of a {budget:.3f}s budget" in reason
    assert "interrupt issued on the connection it held" in reason
    assert len(lock_waits) == 1
    contended_window = _CONTENDED_BUSY_TIMEOUT_MS / 1_000
    # The wait outlived the budget, and ended within the window its own
    # connection states rather than running on for as long as the lock is held.
    assert lock_waits[0] > budget
    assert lock_waits[0] < contended_window * 2
    joined = float(re.search(r"then joined (\d+\.\d+)s more", reason).group(1))
    assert joined < contended_window * 2
    assert not _live_heartbeat_threads()


def test_v2_saga_shutdown_surfaces_a_heartbeat_that_failed_in_flight(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Heartbeat-failure ending: the renewal raises, and the shutdown stays clean.

    The injected error is an `OperationalError` SQLite never produced, so it
    carries no `sqlite_errorcode` at all: only a real `SQLITE_INTERRUPT` is a
    stop signal, and everything else - including this - is still a failure.
    """

    request, current, quota = _request(tmp_path)
    path = tmp_path / "saga.sqlite3"
    transport = _TestTransport(block_dispatch=True)
    lineage = _TestLineageAuthority()
    saga = _heartbeat_shutdown_saga(
        path,
        current=current,
        quota=quota,
        transport=transport,
        lineage=lineage,
        busy_timeout_ms=5_000,
    )
    threads = _record_heartbeat_threads(monkeypatch)
    failed = Event()
    original_heartbeat = saga._heartbeat_outbox

    def failing_heartbeat(**kwargs: Any) -> None:
        if _is_background_dispatch_heartbeat(kwargs):
            failed.set()
            raise sqlite3.OperationalError("heartbeat authority unavailable")
        original_heartbeat(**kwargs)

    monkeypatch.setattr(saga, "_heartbeat_outbox", failing_heartbeat)
    _assert_heartbeats_end_at_the_boundary(saga, monkeypatch, threads)

    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(saga.advance, request, now=NOW + timedelta(seconds=1))
        try:
            assert transport.dispatch_entered.wait(timeout=5)
            assert failed.wait(timeout=5)
            transport.release_dispatch.set()
            snapshot = future.result(timeout=30)
        finally:
            transport.release_dispatch.set()

    assert snapshot.state is SourceBrokerV2SagaState.RECONCILE_REQUIRED
    assert snapshot.reconcile_reason == "outbox heartbeat failed during dispatch"
    assert transport.dispatch_calls == 1
    assert not _live_heartbeat_threads()


def test_v2_saga_shutdown_ends_an_overdue_heartbeat_when_the_body_raises(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`invoke`'s own exception wins, and the renewal is still ended first.

    The `finally` interrupts and joins on the way out, so the caller's
    exception leaves with no heartbeat behind it; the heartbeat report is not
    raised here, because raising it would displace the exception the caller
    actually needs to see.
    """

    request, current, quota = _request(tmp_path)
    path = tmp_path / "saga.sqlite3"
    transport = _TestTransport(block_dispatch=True, lose_dispatch_once=True)
    lineage = _TestLineageAuthority()
    saga = _heartbeat_shutdown_saga(
        path,
        current=current,
        quota=quota,
        transport=transport,
        lineage=lineage,
        busy_timeout_ms=40,
    )
    threads = _record_heartbeat_threads(monkeypatch)
    scanning = Event()
    scans, _ = _stall_the_second_dispatch_renewal(saga, monkeypatch, scanning=scanning)
    _assert_heartbeats_end_at_the_boundary(saga, monkeypatch, threads)

    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(saga.advance, request, now=NOW + timedelta(seconds=1))
        try:
            assert transport.dispatch_entered.wait(timeout=5)
            assert scanning.wait(timeout=10)
            transport.release_dispatch.set()
            snapshot = future.result(timeout=30)
        finally:
            transport.release_dispatch.set()

    assert len(scans) == 1
    assert scans[0] > _expected_shutdown_budget(40)
    assert snapshot.state is SourceBrokerV2SagaState.RECONCILE_REQUIRED
    assert snapshot.reconcile_reason == "source dispatch response is unknown"
    assert transport.dispatch_calls == 1
    assert not _live_heartbeat_threads()


def test_v2_saga_shutdown_notes_an_overdue_heartbeat_on_the_caller_s_exception(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The caller's exception leaves untouched, carrying the shutdown's report.

    On this path the report is never raised - doing so would displace what the
    caller has to see - so it rides along as a PEP 678 note instead.  The
    exception's type, `args` and `str()` are unchanged; only `__notes__` grows,
    which is what keeps an interrupted renewal from being invisible here.
    """

    transport = _TestTransport()
    lineage = _TestLineageAuthority()
    busy_timeout_ms = 40
    saga = _heartbeat_shutdown_saga(
        tmp_path / "saga.sqlite3",
        current=object(),
        quota=_quota_adapter(tmp_path),
        transport=transport,
        lineage=lineage,
        busy_timeout_ms=busy_timeout_ms,
    )
    budget = _expected_shutdown_budget(busy_timeout_ms)
    scratch = tmp_path / "scratch.sqlite3"
    threads = _record_heartbeat_threads(monkeypatch)
    scanning = Event()
    scans: list[float] = []

    def report_progress() -> int:
        scanning.set()
        return 0

    def scanning_heartbeat(**kwargs: Any) -> None:
        bind_connection = kwargs["bind_connection"]
        mark_stage = kwargs["mark_stage"]
        connection = sqlite3.connect(scratch, isolation_level=None)
        bind_connection(connection)
        try:
            mark_stage("scan")
            connection.set_progress_handler(report_progress, _SCAN_PROGRESS_OPS)
            started = time.monotonic()
            try:
                connection.execute(_STALLING_SCAN_SQL).fetchone()
            finally:
                scans.append(time.monotonic() - started)
                connection.set_progress_handler(None, 0)
        finally:
            bind_connection(None)
            connection.close()

    monkeypatch.setattr(saga, "_heartbeat_outbox", scanning_heartbeat)
    monkeypatch.setattr(
        saga,
        "_wait_for_heartbeat",
        lambda stop, interval, *, phase: stop.wait(interval),
    )
    _assert_heartbeats_end_at_the_boundary(saga, monkeypatch, threads)

    def invoke(payload: bytes) -> bytes:
        assert scanning.wait(timeout=10)
        raise ConnectionError("transport committed but response was lost")

    with pytest.raises(ConnectionError) as raised:
        saga._invoke_with_heartbeat(
            phase=SourceBrokerV2OutboxPhase.DISPATCH,
            operation_id="0" * 64,
            owner_generation=1,
            payload=b'{"probe":1}',
            invoke=invoke,
        )

    assert len(scans) == 1
    assert str(raised.value) == "transport committed but response was lost"
    assert raised.value.args == ("transport committed but response was lost",)
    notes = getattr(raised.value, "__notes__", [])
    assert len(notes) == 1
    assert notes[0].startswith("outbox heartbeat did not stop after dispatch")
    assert f"of a {budget:.3f}s budget" in notes[0]
    assert "interrupt issued on the connection it held" in notes[0]
    assert not _live_heartbeat_threads()


def test_v2_saga_shutdown_ends_a_production_renewal_the_interrupt_cannot_abort(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The interrupt lands between two statements of the production body.

    SQLite documents that as a no-op that does not carry over, so the renewal
    goes on to a real `BEGIN IMMEDIATE` against a lock somebody else holds.
    Nothing aborts it; its own `busy_timeout` ends it, and the shutdown waits
    that out rather than handing the thread back - the same guarantee as the
    lock-wait case, but on the body production actually runs.
    """

    request, current, quota = _request(tmp_path)
    path = tmp_path / "saga.sqlite3"
    transport = _TestTransport(block_dispatch=True)
    lineage = _TestLineageAuthority()
    busy_timeout_ms = 400
    saga = _heartbeat_shutdown_saga(
        path,
        current=current,
        quota=quota,
        transport=transport,
        lineage=lineage,
        busy_timeout_ms=busy_timeout_ms,
    )
    budget = _expected_shutdown_budget(busy_timeout_ms)
    threads = _record_heartbeat_threads(monkeypatch)
    interrupted = Event()
    original_interrupt = source_broker_v2_module._HeartbeatConnection.interrupt

    def watched_interrupt(slot: Any) -> Any:
        outcome = original_interrupt(slot)
        interrupted.set()
        return outcome

    monkeypatch.setattr(
        source_broker_v2_module._HeartbeatConnection, "interrupt", watched_interrupt
    )
    # Held across the whole shutdown by the competitor, and released from
    # inside the heartbeat thread itself: the saga writes again right after the
    # raise, and that write must never race the release.
    competitor = sqlite3.connect(path, timeout=10.0, isolation_level=None, check_same_thread=False)
    parked = Event()
    lock_waits: list[float] = []
    completed: list[float] = []
    stalled: list[bool] = []
    original_heartbeat = saga._heartbeat_outbox

    def parking_heartbeat(**kwargs: Any) -> None:
        if not _is_background_dispatch_heartbeat(kwargs) or stalled:
            original_heartbeat(**kwargs)
            return
        if not completed:
            original_heartbeat(**kwargs)
            completed.append(time.monotonic())
            return
        stalled.append(True)
        mark_stage = kwargs["mark_stage"]
        started: list[float] = []

        def marking(stage: str) -> None:
            mark_stage(stage)
            if stage != "lock-wait":
                return
            # `BEGIN IMMEDIATE` has not run yet and the reads before it are
            # finished, so no statement is running when the interrupt arrives.
            parked.set()
            assert interrupted.wait(timeout=20)
            started.append(time.monotonic())

        try:
            original_heartbeat(**{**kwargs, "mark_stage": marking})
        finally:
            lock_waits.append(time.monotonic() - started[-1])
            competitor.execute("ROLLBACK")

    monkeypatch.setattr(saga, "_heartbeat_outbox", parking_heartbeat)
    _assert_heartbeats_end_at_the_boundary(saga, monkeypatch, threads)

    try:
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(saga.advance, request, now=NOW + timedelta(seconds=1))
            try:
                assert transport.dispatch_entered.wait(timeout=5)
                assert parked.wait(timeout=10)
                competitor.execute("BEGIN IMMEDIATE")
                transport.release_dispatch.set()
                snapshot = future.result(timeout=60)
            finally:
                transport.release_dispatch.set()
    finally:
        with suppress(sqlite3.Error):
            competitor.execute("ROLLBACK")
        competitor.close()

    assert snapshot.state is SourceBrokerV2SagaState.RECONCILE_REQUIRED
    reason = snapshot.reconcile_reason
    assert reason is not None
    assert reason.startswith("outbox heartbeat did not stop after dispatch")
    assert f"of a {budget:.3f}s budget" in reason
    assert "stage 'lock-wait'" in reason
    assert "interrupt issued on the connection it held" in reason
    # Issued, but nothing was running to abort: the wait ran its own window out.
    assert len(lock_waits) == 1
    window = busy_timeout_ms / 1_000
    assert lock_waits[0] < window * 2
    joined = float(re.search(r"then joined (\d+\.\d+)s more", reason).group(1))
    assert joined < window * 2
    assert transport.dispatch_calls == 1
    assert not _live_heartbeat_threads()


def test_v2_saga_shutdown_budget_absorbs_a_non_finite_lease(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A NaN lease must not turn the join budget into NaN.

    `_configure` only rejects `executor_lease_seconds <= 0`, so NaN reaches the
    budget through `for_nonproduction`; the finite terms therefore have to come
    first in `max()`, which absorbs NaN.  Ordered the other way the budget is
    NaN and `join(timeout=nan)` raises inside the `finally`, where `is_alive()`
    can never be reached.  The shutdown is exercised directly because a NaN
    lease cannot reach it through `advance`: `_acquire_outbox_lease` builds a
    `timedelta(seconds=nan)` first and raises there.
    """

    transport = _TestTransport()
    lineage = _TestLineageAuthority()
    saga = _heartbeat_shutdown_saga(
        tmp_path / "saga.sqlite3",
        current=object(),
        quota=_quota_adapter(tmp_path),
        transport=transport,
        lineage=lineage,
        busy_timeout_ms=40,
        executor_lease_seconds=float("nan"),
    )
    threads = _record_heartbeat_threads(monkeypatch)
    monkeypatch.setattr(
        saga,
        "_wait_for_heartbeat",
        lambda stop, interval, *, phase: stop.wait(30.0),
    )
    _assert_heartbeats_end_at_the_boundary(saga, monkeypatch, threads)

    result = saga._invoke_with_heartbeat(
        phase=SourceBrokerV2OutboxPhase.DISPATCH,
        operation_id="0" * 64,
        owner_generation=1,
        payload=b'{"probe":1}',
        invoke=lambda payload: payload,
    )

    assert result == b'{"probe":1}'
    assert not _live_heartbeat_threads()


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


# ---------------------------------------------------------------------------
# WP9: the heartbeat now runs in a helper process this saga can actually kill.
# ---------------------------------------------------------------------------

_WP9_FAULT_HELPER = Path(__file__).with_name("_wp9_heartbeat_faults.py")
_WP9_PARENT_DEATH_LAYER = Path(__file__).with_name("_wp9_parent_death_layer.py")

# Calibrations, written down rather than derived, because every one of them is
# a statement about what fits inside a window and a reader has to be able to
# check the arithmetic:
#
#   busy_timeout 100ms  =>  T1 (the shutdown budget) is 2 x 100ms = 0.2s, and
#                           the whole bound is 0.2 + 0.25 + 5.0 + 0.05 = 5.50s.
#                           Production runs 5s, giving 15.30s; the shape of the
#                           arithmetic is what these cases check, not the size.
#   lease 0.3s          =>  interval 0.1s, so a window that outlives one gets
#                           renewals, and the external witness only has to hold
#                           the write lock for 0.2s rather than the 60s two
#                           default leases would need.  T10 needs three
#                           renewals inside one window; the fault helper orders
#                           those itself, so this only has to make the first
#                           one due promptly.
_WP9_BUSY_TIMEOUT_MS = 100
_WP9_SHUTDOWN_BUDGET_SECONDS = 0.2
_WP9_SHUTDOWN_BOUND_SECONDS = 0.2 + 0.25 + 5.0 + 0.05
_WP9_TICKING_LEASE_SECONDS = 0.3
_WP9_TAMPER_LEASE_SECONDS = 0.3
# A window big enough that no renewal is due inside it, which is the ordinary
# production shape: the default lease is 30s and the default source deadline is
# 10s, so almost every real invocation ends with zero ticks.
_WP9_QUIET_LEASE_SECONDS = 120.0
# Every wait a case performs has its own deadline; none of them may hang the
# suite if the thing they wait for never happens.
_WP9_RENDEZVOUS_TIMEOUT_SECONDS = 30.0
_WP9_HELPER_STDLIB_IMPORTS = frozenset(
    {
        "argparse",
        "datetime",
        "errno",
        "fcntl",
        "hashlib",
        "json",
        "os",
        "select",
        "signal",
        "sqlite3",
        "struct",
        "sys",
        "time",
        "typing",
        "__future__",
    }
)


def _wp9_saga(
    tmp_path: Path,
    *,
    lease_seconds: float = _WP9_TICKING_LEASE_SECONDS,
    busy_timeout_ms: int = _WP9_BUSY_TIMEOUT_MS,
) -> tuple[SourceBrokerV2Saga, _TestTransport]:
    """A saga built only as far as these cases need it.

    Deliberately not `_request`: the claim authority and the lineage authority
    are never reached from a heartbeat window, and building the signed claim
    evidence for them is most of what constructing one costs.  The quota bridge
    is real because the constructor demands that exact type.
    """

    transport = _TestTransport()
    saga = _heartbeat_shutdown_saga(
        tmp_path / "saga.sqlite3",
        current=object(),
        quota=_quota_adapter(tmp_path),
        transport=transport,
        lineage=_TestLineageAuthority(),
        busy_timeout_ms=busy_timeout_ms,
        executor_lease_seconds=lease_seconds,
    )
    return saga, transport


def _wp9_window(saga: SourceBrokerV2Saga) -> tuple[SourceBrokerV2OutboxPhase, str, int, bytes]:
    """A real pending outbox effect this executor owns, ready to be invoked.

    Built through the production methods rather than by hand - `_begin_outbox`,
    `_acquire_outbox_lease`, `_mark_invoke_started` - so the row the helper
    renews is the row the saga would have written.  `LINEAGE` is a non-source
    phase, which keeps the fixture free of the grant and attempt evidence a
    dispatch row would also have to carry.
    """

    phase = SourceBrokerV2OutboxPhase.LINEAGE
    operation_id = _operation_id(saga.saga_id, "wp9-window")
    body = {"wp9": "window"}
    payload = canonical_json_bytes(body)
    with saga._connect() as connection:
        connection.execute("BEGIN IMMEDIATE")
        connection.execute(
            "INSERT OR IGNORE INTO source_broker_v2_saga "
            "(saga_id, request_json, request_hash, state) VALUES (?, ?, ?, ?)",
            (saga.saga_id, "{}", canonical_sha256({}), "claimed"),
        )
        connection.commit()
    saga._begin_outbox(
        phase=phase,
        operation_id=operation_id,
        payload=payload,
        payload_hash=canonical_sha256(body),
        idempotency_hash=source_broker_v2_module._outbox_payload_hash(payload),
    )
    generation, stored, _ = saga._acquire_outbox_lease(phase=phase, operation_id=operation_id)
    saga._mark_invoke_started(
        phase=phase, operation_id=operation_id, owner_generation=generation
    )
    return phase, operation_id, generation, stored


def _wp9_fault_command(mode: str, marker: Path | None = None) -> tuple[str, ...]:
    command = ("-I", str(_WP9_FAULT_HELPER), "--fault", mode)
    if marker is not None:
        command += ("--fault-marker", str(marker))
    return command


def _wp9_use_fault_helper(
    saga: SourceBrokerV2Saga,
    monkeypatch: pytest.MonkeyPatch,
    mode: str,
    marker: Path | None = None,
) -> None:
    command = _wp9_fault_command(mode, marker)
    monkeypatch.setattr(saga, "_heartbeat_helper_command", lambda: command)


def _wp9_count_helper_starts(
    saga: SourceBrokerV2Saga, monkeypatch: pytest.MonkeyPatch
) -> list[int]:
    """Every `Popen` this saga attempts, counted at the one place it happens."""

    starts: list[int] = []
    original = saga._start_helper_once

    def counting(**kwargs: Any) -> Any:
        starts.append(len(starts) + 1)
        return original(**kwargs)

    monkeypatch.setattr(saga, "_start_helper_once", counting)
    return starts


def _wp9_heartbeat_columns(path: Path, operation_id: str) -> tuple[Any, Any]:
    connection = sqlite3.connect(path, isolation_level=None)
    try:
        row = connection.execute(
            "SELECT executor_heartbeat_at, executor_lease_expires_at "
            "FROM source_broker_v2_outbox WHERE operation_id = ?",
            (operation_id,),
        ).fetchone()
    finally:
        connection.close()
    assert row is not None
    return row[0], row[1]


def _wp9_outbox_bytes(path: Path, operation_id: str) -> tuple[Any, ...]:
    connection = sqlite3.connect(path, isolation_level=None)
    try:
        row = connection.execute(
            "SELECT * FROM source_broker_v2_outbox WHERE operation_id = ?",
            (operation_id,),
        ).fetchone()
    finally:
        connection.close()
    assert row is not None
    return tuple(row)


def _wp9_outbox_column(path: Path, operation_id: str, column: str) -> Any:
    connection = sqlite3.connect(path, isolation_level=None)
    try:
        row = connection.execute(
            f"SELECT {column} FROM source_broker_v2_outbox WHERE operation_id = ?",
            (operation_id,),
        ).fetchone()
    finally:
        connection.close()
    assert row is not None
    return row[0]


def _wp9_write(path: Path, statement: str, parameters: tuple[Any, ...]) -> None:
    connection = sqlite3.connect(path, timeout=5.0, isolation_level=None)
    try:
        connection.execute("BEGIN IMMEDIATE")
        connection.execute(statement, parameters)
        connection.commit()
    finally:
        connection.close()


def _wp9_integrity_check(path: Path) -> str:
    connection = sqlite3.connect(path, isolation_level=None)
    try:
        return str(connection.execute("PRAGMA integrity_check").fetchone()[0])
    finally:
        connection.close()


def _wp9_open_descriptors() -> list[str]:
    directory = "/proc/self/fd" if sys.platform == "linux" else "/dev/fd"
    return sorted(os.listdir(directory))


def _wp9_wait_for(predicate: Callable[[], bool], *, what: str) -> None:
    """Poll with a deadline of its own; a case may never wait indefinitely."""

    deadline = time.monotonic() + _WP9_RENDEZVOUS_TIMEOUT_SECONDS
    while not predicate():
        if time.monotonic() > deadline:
            raise AssertionError(f"timed out waiting for {what}")
        time.sleep(0.002)


def _wp9_hold_fault_lock(marker: Path) -> int:
    """Take the lock the fault helper will block on, before it is started."""

    handle = os.open(str(marker) + ".lock", os.O_RDWR | os.O_CREAT, 0o600)
    fcntl.flock(handle, fcntl.LOCK_EX)
    return handle


def _wp9_open_window(marker: Path) -> None:
    """Let a gated fault fire, now that the invocation is genuinely running."""

    Path(str(marker) + ".go").write_bytes(b"")


def _wp9_probe_write_lock(path: Path, *, timeout_ms: int) -> tuple[bool, int | None, float]:
    """Try to take the file's write lock; report what happened and how long."""

    connection = sqlite3.connect(path, timeout=timeout_ms / 1_000, isolation_level=None)
    connection.execute(f"PRAGMA busy_timeout = {timeout_ms}")
    started = time.monotonic()
    try:
        connection.execute("BEGIN IMMEDIATE")
    except sqlite3.OperationalError as exc:
        return False, getattr(exc, "sqlite_errorcode", None), time.monotonic() - started
    else:
        connection.execute("ROLLBACK")
        return True, None, time.monotonic() - started
    finally:
        connection.close()


def _wp9_boundary_state(saga: SourceBrokerV2Saga) -> dict[str, Any]:
    """Read the session witness at the exact edge of `_invoke_with_heartbeat`.

    `_live_heartbeat_threads` is kept as a regression guard rather than as
    load-bearing evidence: it is now always empty because this process starts no
    heartbeat thread at all, and it goes red the day somebody puts one back.
    `active_children` is the same shape of guard for `multiprocessing`, which
    this design does not use and must not start using - its exit handler joins
    without a timeout, which is the bound this whole change exists to get.
    """

    assert not _live_heartbeat_threads()
    assert multiprocessing.active_children() == []
    popen = saga._heartbeat_state.popen
    return {
        "outcome": saga._last_heartbeat_session,
        "helper_returncode": None if popen is None else popen.poll(),
        "orphans": list(saga._orphaned_heartbeat_children),
    }


def _wp9_watch_boundary(
    saga: SourceBrokerV2Saga, monkeypatch: pytest.MonkeyPatch
) -> list[dict[str, Any]]:
    """Record the boundary state on both exits, before anything else runs."""

    seen: list[dict[str, Any]] = []
    original = saga._invoke_with_heartbeat

    def bounded(**kwargs: Any) -> bytes:
        started = time.monotonic()
        try:
            result = original(**kwargs)
        except BaseException:
            state = _wp9_boundary_state(saga)
            state["seconds"] = time.monotonic() - started
            seen.append(state)
            raise
        state = _wp9_boundary_state(saga)
        state["seconds"] = time.monotonic() - started
        seen.append(state)
        return result

    monkeypatch.setattr(saga, "_invoke_with_heartbeat", bounded)
    return seen


def test_wp9_heartbeat_sql_and_digest_come_from_one_place() -> None:
    """The renewal guard and the window digest are objects, not copies.

    Identity rather than equality: two equal strings drift the moment one of
    them is edited, and this guard is the whole of the lease's fencing.  The
    source scan is the other half - it refuses a second literal anywhere under
    `src/`, which is how a copy would get made in the first place.
    """

    assert (
        source_broker_v2_module.HEARTBEAT_UPDATE_SQL
        is source_broker_v2_heartbeat.HEARTBEAT_UPDATE_SQL
    )
    assert (
        source_broker_v2_module.HEARTBEAT_SELECT_SQL
        is source_broker_v2_heartbeat.HEARTBEAT_SELECT_SQL
    )
    assert (
        source_broker_v2_module.stable_row_digest is source_broker_v2_heartbeat.stable_row_digest
    )
    assert (
        source_broker_v2_module.open_saga_connection
        is source_broker_v2_heartbeat.open_saga_connection
    )
    needle = "UPDATE source_broker_v2_outbox SET executor_heartbeat_at"
    source_root = Path(source_broker_v2_module.__file__).parent
    occurrences = sum(
        path.read_text(encoding="utf-8").count(needle) for path in source_root.rglob("*.py")
    )
    assert occurrences == 1


def test_wp9_heartbeat_modules_import_no_multiprocessing() -> None:
    """Neither module may reach for `multiprocessing`, now or later.

    Its exit handler joins every registered child without a timeout, so a
    process that outlives a SIGKILL - the exact case this design has to
    survive - can hang the interpreter on the way out.  `subprocess` registers
    nothing and its only cleanup is a non-blocking `waitpid`.
    """

    for module in (source_broker_v2_module, source_broker_v2_heartbeat):
        tree = ast.parse(Path(module.__file__).read_text(encoding="utf-8"))
        imported: set[str] = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imported.update(alias.name.split(".")[0] for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module:
                imported.add(node.module.split(".")[0])
        assert "multiprocessing" not in imported


def test_wp9_helper_module_imports_only_its_stdlib_allowlist() -> None:
    """The helper's capability surface is a list you can read in one screen."""

    tree = ast.parse(Path(source_broker_v2_heartbeat.__file__).read_text(encoding="utf-8"))
    imported: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported.update(alias.name.split(".")[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported.add(node.module.split(".")[0])
    assert imported <= _WP9_HELPER_STDLIB_IMPORTS
    assert not any(name.startswith("rquant") for name in imported)
    assert "subprocess" not in imported


def test_wp9_helper_import_closure_pulls_in_no_other_rquant_module() -> None:
    """Structural, not promised: the code simply is not in that process.

    A helper that imported `rquant.source_broker_v2` would be carrying the
    transport, quota, authority and lineage modules - exactly the code the
    threat model is about - into the process that renews a lease.  Measured by
    importing it in a fresh isolated interpreter and listing what arrived.
    """

    completed = subprocess.run(
        (
            sys.executable,
            "-I",
            "-c",
            "import sys, json;"
            " import rquant.source_broker_v2_heartbeat;"
            " print(json.dumps(sorted(n for n in sys.modules if n.startswith('rquant'))))",
        ),
        check=True,
        capture_output=True,
        env={"PATH": "/usr/bin:/bin"},
    )
    modules = json.loads(completed.stdout.decode("utf-8").strip().splitlines()[-1])
    assert modules == ["rquant", "rquant.source_broker_v2_heartbeat"]


def test_wp9_helper_import_opens_no_dotenv() -> None:
    """Nothing in that process reads configuration off the filesystem."""

    probe = (
        "import sys, json\n"
        "opened = []\n"
        "def hook(event, args):\n"
        "    if event == 'open':\n"
        "        opened.append(str(args[0]))\n"
        "sys.addaudithook(hook)\n"
        "import rquant.source_broker_v2_heartbeat\n"
        "print(json.dumps([p for p in opened if '.env' in p]))\n"
    )
    completed = subprocess.run(
        (sys.executable, "-I", "-c", probe),
        check=True,
        capture_output=True,
        env={"PATH": "/usr/bin:/bin"},
    )
    assert json.loads(completed.stdout.decode("utf-8").strip().splitlines()[-1]) == []


def test_wp9_helper_environment_is_an_allowlist_built_from_nothing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("RQUANT_SECRET_TOKEN", "must-not-travel")
    monkeypatch.setenv("HOME", "/somewhere")
    monkeypatch.setenv("TMPDIR", "/tmp/wp9-private")
    environ = source_broker_v2_module._heartbeat_environ()
    assert set(environ) <= {"PATH", "LC_ALL", "LANG", "TMPDIR", "SQLITE_TMPDIR"}
    assert environ["PATH"] == "/usr/bin:/bin"
    assert environ["TMPDIR"] == "/tmp/wp9-private"
    assert "HOME" not in environ
    assert not any(key.startswith("RQUANT") for key in environ)
    monkeypatch.delenv("SQLITE_TMPDIR", raising=False)
    assert "SQLITE_TMPDIR" not in source_broker_v2_module._heartbeat_environ()


def test_wp9_helper_argv_carries_no_secret() -> None:
    """Only descriptor numbers travel on the command line.

    `ps` and `/proc/<pid>/cmdline` are readable by every process of this user,
    so the owner token and the database path go in the config frame instead.
    """

    assert source_broker_v2_module._HEARTBEAT_HELPER_COMMAND == (
        "-I",
        "-m",
        "rquant.source_broker_v2_heartbeat",
    )
    assert (
        source_broker_v2_module._FROZEN_HELPER_COMMAND
        is source_broker_v2_module._HEARTBEAT_HELPER_COMMAND
    )


@pytest.mark.parametrize("replacement", ["method", "module-constant"])
def test_wp9_production_saga_refuses_a_replaced_helper_command(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    replacement: str,
) -> None:
    """Both ways of moving the seam, because one of them moves both sides.

    Rebinding the module constant would change what the seam returns *and*
    what a naive guard compared it against, so the comparison is against a
    tuple captured at import time.
    """

    saga, _ = _wp9_saga(tmp_path)
    monkeypatch.setattr(saga, "_production_graph", object())
    evil = ("-I", str(_WP9_FAULT_HELPER), "--fault", "exit-immediately")
    if replacement == "method":
        monkeypatch.setattr(saga, "_heartbeat_helper_command", lambda: evil)
    else:
        monkeypatch.setattr(source_broker_v2_module, "_HEARTBEAT_HELPER_COMMAND", evil)
    with pytest.raises(TypeError, match="helper command was replaced"):
        saga._start_helper_once(operation_id="wp9")
    assert saga._heartbeat_state.popen is None
    assert not saga._orphaned_heartbeat_children


def test_wp9_fault_helper_digest_matches_the_production_digest(tmp_path: Path) -> None:
    """The fault helper's copy of the digest is pinned to the real one.

    It cannot import `rquant` - `-I` and a single-file rule see to that - so it
    carries its own implementation, and a copy that drifts would make T10 pass
    for the wrong reason.
    """

    from . import _wp9_heartbeat_faults

    path = tmp_path / "digest.sqlite3"
    connection = sqlite3.connect(path, isolation_level=None)
    connection.row_factory = sqlite3.Row
    try:
        connection.execute(
            "CREATE TABLE source_broker_v2_outbox (operation_id TEXT, executor_heartbeat_at TEXT,"
            " executor_lease_expires_at TEXT, text_column TEXT, int_column INTEGER,"
            " real_column REAL, blob_column BLOB, null_column TEXT)"
        )
        connection.execute(
            "INSERT INTO source_broker_v2_outbox VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            ("op", "then", "later", "a value", 7, 1.5, b"\x00\x01", None),
        )
        cursor = connection.execute("SELECT * FROM source_broker_v2_outbox")
        row = cursor.fetchone()
        assert source_broker_v2_heartbeat.stable_row_digest(
            row, cursor.description
        ) == _wp9_heartbeat_faults.stable_row_digest(row, cursor.description)
        assert (
            _wp9_heartbeat_faults.DIGEST_EXCLUDED_COLUMNS
            == source_broker_v2_heartbeat.DIGEST_EXCLUDED_COLUMNS
        )
        assert _wp9_heartbeat_faults.UPDATE_SQL == source_broker_v2_heartbeat.HEARTBEAT_UPDATE_SQL
    finally:
        connection.close()


def test_wp9_digest_ignores_only_the_two_columns_a_renewal_moves(tmp_path: Path) -> None:
    path = tmp_path / "digest.sqlite3"
    connection = sqlite3.connect(path, isolation_level=None)
    connection.row_factory = sqlite3.Row
    try:
        connection.execute(
            "CREATE TABLE source_broker_v2_outbox (operation_id TEXT,"
            " executor_heartbeat_at TEXT, executor_lease_expires_at TEXT, payload_hash TEXT)"
        )
        connection.execute(
            "INSERT INTO source_broker_v2_outbox VALUES ('op', 'a', 'b', 'hash')"
        )
        cursor = connection.execute("SELECT * FROM source_broker_v2_outbox")
        before = source_broker_v2_heartbeat.stable_row_digest(cursor.fetchone(), cursor.description)
        connection.execute(
            "UPDATE source_broker_v2_outbox SET executor_heartbeat_at = 'c',"
            " executor_lease_expires_at = 'd'"
        )
        cursor = connection.execute("SELECT * FROM source_broker_v2_outbox")
        assert (
            source_broker_v2_heartbeat.stable_row_digest(cursor.fetchone(), cursor.description)
            == before
        )
        connection.execute("UPDATE source_broker_v2_outbox SET payload_hash = 'other'")
        cursor = connection.execute("SELECT * FROM source_broker_v2_outbox")
        assert (
            source_broker_v2_heartbeat.stable_row_digest(cursor.fetchone(), cursor.description)
            != before
        )
    finally:
        connection.close()


@pytest.mark.parametrize(
    ("lease", "message"),
    [
        (float("nan"), "must be finite"),
        (float("inf"), "must be finite"),
        # A negative infinity is caught one line earlier, by the ordinary
        # positivity check, which is the point: only NaN needs the new guard.
        (float("-inf"), "must be positive"),
    ],
    ids=["nan", "inf", "negative-inf"],
)
def test_wp9_saga_rejects_a_non_finite_lease_at_construction(
    tmp_path: Path, lease: float, message: str
) -> None:
    """A NaN passes every ordering comparison, so it needs its own guard.

    It is neither `<= 0` nor `> 0`, so the positivity check waves it through,
    and it would then be divided into a renewal interval and handed to `select`
    as a timeout.
    """

    with pytest.raises(ValueError, match=message):
        _heartbeat_shutdown_saga(
            tmp_path / "saga.sqlite3",
            current=object(),
            quota=_quota_adapter(tmp_path),
            transport=_TestTransport(),
            lineage=_TestLineageAuthority(),
            busy_timeout_ms=_WP9_BUSY_TIMEOUT_MS,
            executor_lease_seconds=lease,
        )


def test_wp9_frame_encoder_refuses_a_non_finite_number() -> None:
    """The second of the three guards: `NaN` is not JSON, and never travels."""

    with pytest.raises(ValueError):
        source_broker_v2_heartbeat.encode_frame({"t": "session", "interval_seconds": float("nan")})
    with pytest.raises(ValueError):
        source_broker_v2_heartbeat.encode_frame({"t": "session", "interval_seconds": float("inf")})


@pytest.mark.parametrize(
    ("fault", "renewed"),
    [("stall-before-connect", False), ("stall-after-commit", True)],
    ids=["before-its-connection-exists", "closing-a-committed-connection"],
)
def test_v2_saga_shutdown_reaps_a_heartbeat_stuck_in_a_window_no_interrupt_reaches(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    fault: str,
    renewed: bool,
) -> None:
    """T1 and T2: the two windows a thread could never be pulled out of.

    `stall-before-connect` stands for a `sqlite3.connect` that never returns -
    there is no connection to interrupt and nothing has been written.
    `stall-after-commit` stands for the passive checkpoint inside `close` - the
    renewal is already durable and the write lock is already gone, so a
    competitor gets it immediately, but the process is still in a system call.

    Whether the renewal happened is a property of where the injection sits in
    the helper's own code, not of any timing, so both are asserted outright.
    The stall is a real `flock` this case holds, and the helper announces its
    arrival by creating a file, so nothing here waits on a duration.
    """

    saga, _ = _wp9_saga(tmp_path)
    marker = tmp_path / f"{fault}.marker"
    lock = _wp9_hold_fault_lock(marker)
    boundary = _wp9_watch_boundary(saga, monkeypatch)
    _wp9_use_fault_helper(saga, monkeypatch, fault, marker)
    phase, operation_id, generation, payload = _wp9_window(saga)
    columns: dict[str, tuple[Any, Any]] = {}

    def invoke(_: bytes) -> bytes:
        columns["pre"] = _wp9_heartbeat_columns(saga.path, operation_id)
        _wp9_open_window(marker)
        _wp9_wait_for(marker.exists, what=f"the {fault} helper to block")
        columns["stuck"] = _wp9_heartbeat_columns(saga.path, operation_id)
        if fault == "stall-after-commit":
            taken, code, _ = _wp9_probe_write_lock(saga.path, timeout_ms=_WP9_BUSY_TIMEOUT_MS)
            # Committing released the write lock; the stall is in the tail
            # behind it, which holds nothing.
            assert taken, code
        return canonical_json_bytes({"wp9": "result"})

    try:
        started = time.monotonic()
        with pytest.raises(SourceBrokerV2SagaUnavailableError):
            saga._invoke_with_heartbeat(
                phase=phase,
                operation_id=operation_id,
                owner_generation=generation,
                payload=payload,
                invoke=invoke,
            )
        elapsed = time.monotonic() - started
    finally:
        os.close(lock)

    outcome = boundary[-1]["outcome"]
    assert outcome is not None
    assert outcome.acked is False
    assert outcome.child_returncode == -9
    assert outcome.escalation == "sigkill"
    assert outcome.orphaned is False
    assert outcome.child_alive is False
    # Both sides: long enough to have really waited out the end-frame budget,
    # short enough that the wait ended at a number written down rather than at
    # whatever the blocked system call was going to do.
    assert elapsed >= _WP9_SHUTDOWN_BUDGET_SECONDS
    assert elapsed <= _WP9_SHUTDOWN_BOUND_SECONDS
    # The helper is reaped and released, so there is no process left for the
    # boundary to report on and nothing recorded as unaccounted for.
    assert boundary[-1]["helper_returncode"] is None
    assert saga._heartbeat_state.popen is None
    assert not boundary[-1]["orphans"]
    # Whether a renewal happened is decided by where the injection sits in the
    # helper's own code, so it is asserted rather than observed: stuck before
    # connecting means nothing was written, stuck after committing means it was
    # written and made durable.
    assert (columns["stuck"] != columns["pre"]) is renewed
    # And nothing at all happened after the process died.
    assert _wp9_heartbeat_columns(saga.path, operation_id) == columns["stuck"]
    assert _wp9_integrity_check(saga.path) == "ok"
    # The lock is free the instant the process is gone, which is the thing a
    # timed-out thread could never give: a real write succeeds right away.
    saga._release_outbox_lease(
        phase=phase, operation_id=operation_id, owner_generation=generation
    )


def test_v2_saga_shutdown_reaps_a_heartbeat_stuck_in_its_durable_tail(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """T5: stuck between the update and the commit, holding the write lock.

    This is the window where a thread is unreachable *and* is standing on the
    file: a competitor waits out its whole `busy_timeout` and gets SQLITE_BUSY.
    After the kill the same probe succeeds, and the row shows the renewal was
    rolled back whole - never half applied.

    Stated plainly: this models a process stuck in a durable tail, not a kernel
    D state.  SIGKILL does not preempt uninterruptible I/O either, and the
    answer to that case is the orphan registry and fail-closed entry, not a
    claim that the process can be killed.
    """

    saga, _ = _wp9_saga(tmp_path)
    marker = tmp_path / "durable-tail.marker"
    lock = _wp9_hold_fault_lock(marker)
    boundary = _wp9_watch_boundary(saga, monkeypatch)
    _wp9_use_fault_helper(saga, monkeypatch, "stall-before-commit", marker)
    phase, operation_id, generation, payload = _wp9_window(saga)
    before: list[tuple[Any, Any]] = []
    contended: list[tuple[bool, int | None, float]] = []

    def invoke(_: bytes) -> bytes:
        before.append(_wp9_heartbeat_columns(saga.path, operation_id))
        _wp9_open_window(marker)
        _wp9_wait_for(marker.exists, what="the durable-tail helper to block")
        contended.append(_wp9_probe_write_lock(saga.path, timeout_ms=_WP9_BUSY_TIMEOUT_MS))
        return canonical_json_bytes({"wp9": "result"})

    try:
        with pytest.raises(SourceBrokerV2SagaUnavailableError):
            saga._invoke_with_heartbeat(
                phase=phase,
                operation_id=operation_id,
                owner_generation=generation,
                payload=payload,
                invoke=invoke,
            )
    finally:
        os.close(lock)

    taken, code, waited = contended[0]
    assert taken is False
    assert code == sqlite3.SQLITE_BUSY
    assert waited >= _WP9_BUSY_TIMEOUT_MS / 1_000 * 0.5
    outcome = boundary[-1]["outcome"]
    assert outcome is not None
    assert outcome.child_returncode == -9
    assert outcome.escalation == "sigkill"
    # The renewal that was in flight when the process died left nothing: two
    # columns unmoved, not one moved and one not.
    assert _wp9_heartbeat_columns(saga.path, operation_id) == before[0]
    assert _wp9_integrity_check(saga.path) == "ok"
    released, code, elapsed = _wp9_probe_write_lock(saga.path, timeout_ms=_WP9_BUSY_TIMEOUT_MS)
    assert released, code
    assert elapsed < _WP9_BUSY_TIMEOUT_MS / 1_000


def test_v2_saga_shutdown_reaps_the_helper_when_the_body_raises(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """T4: the caller's exception is the one that survives, verbatim.

    The teardown that runs while it is in flight is full of things that can
    fail - a broken pipe, an EOF, a timeout, a kill - and not one of them may
    replace what the caller raised.  A note is the only thing allowed to be
    added, because it changes nothing an `except` can see.
    """

    saga, _ = _wp9_saga(tmp_path)
    marker = tmp_path / "body-raises.marker"
    lock = _wp9_hold_fault_lock(marker)
    boundary = _wp9_watch_boundary(saga, monkeypatch)
    _wp9_use_fault_helper(saga, monkeypatch, "stall-after-commit", marker)
    phase, operation_id, generation, payload = _wp9_window(saga)
    raised = ConnectionError("boom", 7)

    def invoke(_: bytes) -> bytes:
        _wp9_open_window(marker)
        _wp9_wait_for(marker.exists, what="the stalled helper to block")
        raise raised

    try:
        with pytest.raises(ConnectionError) as caught:
            saga._invoke_with_heartbeat(
                phase=phase,
                operation_id=operation_id,
                owner_generation=generation,
                payload=payload,
                invoke=invoke,
            )
    finally:
        os.close(lock)

    assert caught.value is raised
    assert type(caught.value) is ConnectionError
    assert caught.value.args == ("boom", 7)
    assert str(caught.value) == str(ConnectionError("boom", 7))
    notes = getattr(caught.value, "__notes__", [])
    assert len(notes) == 1
    assert "returncode -9" in notes[0]
    assert "last stage" in notes[0]
    outcome = boundary[-1]["outcome"]
    assert outcome is not None
    assert outcome.child_returncode == -9
    assert outcome.escalation == "sigkill"
    saga._release_outbox_lease(
        phase=phase, operation_id=operation_id, owner_generation=generation
    )
    assert _wp9_integrity_check(saga.path) == "ok"


@pytest.mark.parametrize("body_raises", [False, True], ids=["invoke-returns", "invoke-raises"])
def test_v2_saga_surfaces_a_helper_that_died_during_an_invocation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    body_raises: bool,
) -> None:
    """T7 and T8: an invocation that ran unprotected is never called a success.

    The helper exits straight after acknowledging the session, so the window
    had no renewal behind it.  When the body returns normally that has to
    surface as a failure; when the body raises, its own exception outranks the
    diagnosis and comes out untouched.

    The rendezvous is a fact, not a race: the body polls the helper's
    `returncode` until it is set, so the death has already happened before the
    teardown under test begins.
    """

    saga, _ = _wp9_saga(tmp_path)
    boundary = _wp9_watch_boundary(saga, monkeypatch)
    _wp9_use_fault_helper(saga, monkeypatch, "exit-after-ack")
    phase, operation_id, generation, payload = _wp9_window(saga)
    calls: list[int] = []
    raised = ConnectionError("boom", 7)

    def invoke(_: bytes) -> bytes:
        calls.append(1)
        popen = saga._heartbeat_state.popen
        assert popen is not None
        _wp9_wait_for(lambda: popen.poll() is not None, what="the helper to exit")
        if body_raises:
            raise raised
        return canonical_json_bytes({"wp9": "result"})

    expected: type[BaseException] = (
        ConnectionError if body_raises else SourceBrokerV2SagaUnavailableError
    )
    with pytest.raises(expected) as caught:
        saga._invoke_with_heartbeat(
            phase=phase,
            operation_id=operation_id,
            owner_generation=generation,
            payload=payload,
            invoke=invoke,
        )

    assert calls == [1]
    outcome = boundary[-1]["outcome"]
    assert outcome is not None
    assert outcome.acked is False
    assert outcome.child_alive is False
    assert outcome.orphaned is False
    if body_raises:
        assert caught.value is raised
        assert caught.value.args == ("boom", 7)
        assert str(caught.value) == str(ConnectionError("boom", 7))
        assert caught.value.__cause__ is None
        assert len(getattr(caught.value, "__notes__", [])) == 1
    else:
        assert not isinstance(caught.value, BrokenPipeError | OSError)
    saga._release_outbox_lease(
        phase=phase, operation_id=operation_id, owner_generation=generation
    )
    assert _wp9_outbox_column(saga.path, operation_id, "status") == "pending"
    assert _wp9_outbox_column(saga.path, operation_id, "invoke_started") == 1


def _wp9_wait_for_renewals(saga: SourceBrokerV2Saga, operation_id: str, count: int) -> list[Any] :
    """Wait until the helper has moved the heartbeat column `count` times.

    An external observation, so it says the renewals really happened rather
    than that the code under test says they did - and it replaces a sleep with
    a fact, so no case is gated on how fast the host is.
    """

    seen: list[Any] = [_wp9_heartbeat_columns(saga.path, operation_id)[0]]

    def progressed() -> bool:
        current = _wp9_heartbeat_columns(saga.path, operation_id)[0]
        if current != seen[-1]:
            seen.append(current)
        return len(seen) > count

    _wp9_wait_for(progressed, what=f"{count} renewal(s) from the helper")
    return seen[1:]


def _wp9_witness_no_renewal_after_the_session(
    saga: SourceBrokerV2Saga, operation_id: str, *, interval: float
) -> None:
    """Evidence that does not come from the code under test.

    `acked`, `ticks` and the digests are all self-reported.  This holds the
    write lock for longer than two renewal intervals and reads the row inside
    that window and again after it - the second read matters, because a helper
    blocked on the lock would land its renewal the moment the lock is dropped.
    """

    before = _wp9_heartbeat_columns(saga.path, operation_id)
    connection = sqlite3.connect(saga.path, timeout=5.0, isolation_level=None)
    try:
        connection.execute("BEGIN IMMEDIATE")
        time.sleep(interval * 2)
        connection.execute("ROLLBACK")
    finally:
        connection.close()
    assert _wp9_heartbeat_columns(saga.path, operation_id) == before
    time.sleep(interval)
    assert _wp9_heartbeat_columns(saga.path, operation_id) == before
    assert not _live_heartbeat_threads()


@pytest.mark.parametrize("first_session_fails", [False, True], ids=["clean", "after-a-failure"])
def test_v2_saga_reuses_one_helper_across_sessions_and_closes_it_explicitly(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    first_session_fails: bool,
) -> None:
    """T3: one process serves many windows, and an ordinary failure is ordinary.

    The second parametrisation is the load-bearing one.  A renewal that fails is
    a normal outcome - the helper closes the session, stays alive and goes back
    to idle - and folding "did the renewal work" together with "did the session
    end cleanly" would terminate and kill it instead: a healthy process
    destroyed, a restart paid for, and a shutdown that took the whole budget
    plus two signal timeouts rather than one pipe round trip.  Same pid across
    both sessions is what says that did not happen.
    """

    saga, _ = _wp9_saga(tmp_path)
    interval = _WP9_TICKING_LEASE_SECONDS / 3
    marker = tmp_path / "reuse.marker"
    boundary = _wp9_watch_boundary(saga, monkeypatch)
    if first_session_fails:
        _wp9_use_fault_helper(saga, monkeypatch, "fail-renewal", marker)
    phase, operation_id, generation, payload = _wp9_window(saga)

    def first_invoke(_: bytes) -> bytes:
        if first_session_fails:
            _wp9_open_window(marker)
            _wp9_wait_for(marker.exists, what="the injected renewal failure")
        else:
            _wp9_wait_for_renewals(saga, operation_id, 1)
        return canonical_json_bytes({"wp9": "first"})

    if first_session_fails:
        with pytest.raises(SourceBrokerV2SagaUnavailableError) as caught:
            saga._invoke_with_heartbeat(
                phase=phase,
                operation_id=operation_id,
                owner_generation=generation,
                payload=payload,
                invoke=first_invoke,
            )
        cause = caught.value.__cause__
        assert cause is not None
        assert getattr(cause, "sqlite_errorcode", None) == sqlite3.SQLITE_BUSY
    else:
        saga._invoke_with_heartbeat(
            phase=phase,
            operation_id=operation_id,
            owner_generation=generation,
            payload=payload,
            invoke=first_invoke,
        )

    first = boundary[-1]["outcome"]
    assert first is not None
    # Whatever happened to the renewal, the session itself was acknowledged and
    # the helper is still there.
    assert first.acked is True
    assert first.renewal_ok is not first_session_fails
    assert first.escalation == "clean"
    assert first.child_alive is True
    assert first.restarts == 0
    assert first.orphaned is False
    if first_session_fails:
        assert first.failure is not None
        assert first.failure.errorcode == sqlite3.SQLITE_BUSY
    else:
        assert first.ticks >= 1
        assert first.digest_changed is False
        assert first.digest_mismatch is False
        assert first.shutdown_seconds < 1.0
    assert saga._heartbeat_state.popen is not None
    assert saga._heartbeat_state.popen.poll() is None

    def second_invoke(_: bytes) -> bytes:
        _wp9_wait_for_renewals(saga, operation_id, 1)
        return canonical_json_bytes({"wp9": "second"})

    saga._invoke_with_heartbeat(
        phase=phase,
        operation_id=operation_id,
        owner_generation=generation,
        payload=payload,
        invoke=second_invoke,
    )
    second = boundary[-1]["outcome"]
    assert second is not None
    assert second.acked is True
    assert second.renewal_ok is True
    assert second.ticks >= 1
    assert second.escalation == "clean"
    assert second.child_pid == first.child_pid
    assert second.restarts == 0
    assert second.shutdown_seconds < 1.0

    _wp9_witness_no_renewal_after_the_session(saga, operation_id, interval=interval)

    pid = second.child_pid
    assert pid is not None
    outcome = saga.close()
    assert outcome.orphaned is False
    assert outcome.returncode is not None
    assert saga._heartbeat_state.popen is None
    assert not saga._orphaned_heartbeat_children
    assert multiprocessing.active_children() == []
    # Only a corroborating check, and only valid here: `os.kill(pid, 0)` says
    # nothing while a process is a zombie, and returns success on the orphan
    # path where the exit was never observed.  `returncode` above is the
    # evidence; this is the second opinion.
    with pytest.raises(ProcessLookupError):
        os.kill(pid, 0)
    assert _wp9_integrity_check(saga.path) == "ok"


def test_v2_saga_refuses_before_invoking_when_the_helper_cannot_confirm_a_session(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """T6: no acknowledgement, no invocation - and exactly one restart.

    A session frame that lands in a buffer nobody reads would leave the whole
    invocation running with no renewal behind it and nobody the wiser, which is
    the silent degradation this design forbids.  So the refusal happens before
    the external call, which makes the external call count zero, and the retry
    is one restart rather than a loop - a loop would be a new unbounded point.

    Starts from a live helper and kills it first, so both detectors are on the
    path: the pre-check that finds a dead process, and the acknowledgement
    timeout that finds a live one that will not answer.
    """

    monkeypatch.setattr(source_broker_v2_module, "_HEARTBEAT_HANDSHAKE_FLOOR_SECONDS", 0.12)
    saga, _ = _wp9_saga(tmp_path)
    boundary = _wp9_watch_boundary(saga, monkeypatch)
    starts = _wp9_count_helper_starts(saga, monkeypatch)
    phase, operation_id, generation, payload = _wp9_window(saga)
    calls: list[int] = []

    def invoke(_: bytes) -> bytes:
        calls.append(1)
        return canonical_json_bytes({"wp9": "never"})

    saga._invoke_with_heartbeat(
        phase=phase,
        operation_id=operation_id,
        owner_generation=generation,
        payload=payload,
        invoke=invoke,
    )
    assert calls == [1]
    live = saga._heartbeat_state.popen
    assert live is not None
    live.kill()
    live.wait(timeout=5)
    before = _wp9_heartbeat_columns(saga.path, operation_id)
    _wp9_use_fault_helper(saga, monkeypatch, "no-ack")
    started_before = len(starts)

    started = time.monotonic()
    with pytest.raises(SourceBrokerV2SagaUnavailableError, match="did not acknowledge"):
        saga._invoke_with_heartbeat(
            phase=phase,
            operation_id=operation_id,
            owner_generation=generation,
            payload=payload,
            invoke=invoke,
        )
    elapsed = time.monotonic() - started

    assert calls == [1]
    # Two starts: the replacement for the helper this case killed, and the one
    # restart the session confirmation is allowed.
    assert len(starts) - started_before == 2
    handshake = max(0.12, _WP9_BUSY_TIMEOUT_MS / 1_000)
    assert elapsed <= 2 * (handshake + handshake) + 0.25 + 5.0 + 0.05
    assert not saga._orphaned_heartbeat_children
    assert saga._heartbeat_state.popen is None
    assert boundary[-1]["outcome"] is None or boundary[-1]["outcome"].token
    assert _wp9_heartbeat_columns(saga.path, operation_id) == before
    # The failure lands after `_mark_invoke_started`, so the row already says
    # an invocation began even though none did.  That is the conservative
    # direction - the next pass reconciles instead of dispatching again - and
    # it is a consequence of refusing before the external call, so it is
    # asserted rather than left implicit.
    assert _wp9_outbox_column(saga.path, operation_id, "invoke_started") == 1
    saga._release_outbox_lease(
        phase=phase, operation_id=operation_id, owner_generation=generation
    )


@pytest.mark.parametrize(
    "shape",
    ["self-healing-tamper", "untouched-three-ticks", "zero-ticks"],
)
def test_v2_saga_detects_a_self_healing_tamper_inside_the_invoke_window(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    shape: str,
) -> None:
    """T10: a change that is put back still leaves a mark.

    The window between the two full validation points is sampled once per
    renewal, which is the resolution it has always had.  What is new is that
    the "it moved" flag is sticky, so a tamper that is undone before the next
    sample - the case that used to leave nothing at all - is still reported.

    Three shapes, because the rule has three answers.  With a tamper, the row
    ends up byte-identical to how it started and only the sticky flag knows.
    Untouched with samples taken, nothing is reported.  With no samples at all,
    there is nothing to compare and the comparison is skipped - which is not a
    relaxation but the same resolution as before any of this existed, and it is
    the common case in production, where the default lease makes the renewal
    interval as long as the whole request deadline.
    """

    lease = _WP9_QUIET_LEASE_SECONDS if shape == "zero-ticks" else _WP9_TAMPER_LEASE_SECONDS
    saga, _ = _wp9_saga(tmp_path, lease_seconds=lease)
    boundary = _wp9_watch_boundary(saga, monkeypatch)
    marker = tmp_path / f"{shape}.marker"
    if shape == "self-healing-tamper":
        _wp9_use_fault_helper(saga, monkeypatch, "tamper-self-healing", marker)
    phase, operation_id, generation, payload = _wp9_window(saga)

    def invoke(_: bytes) -> bytes:
        if shape == "self-healing-tamper":
            _wp9_open_window(marker)
            _wp9_wait_for(marker.exists, what="the tamper sequence to finish")
        elif shape == "untouched-three-ticks":
            _wp9_wait_for_renewals(saga, operation_id, 3)
        return canonical_json_bytes({"wp9": "result"})

    before = _wp9_outbox_bytes(saga.path, operation_id)
    if shape == "self-healing-tamper":
        with pytest.raises(SourceBrokerV2SagaUnavailableError) as caught:
            saga._invoke_with_heartbeat(
                phase=phase,
                operation_id=operation_id,
                owner_generation=generation,
                payload=payload,
                invoke=invoke,
            )
        assert type(caught.value) is SourceBrokerV2SagaUnavailableError
        assert type(caught.value.__cause__) is SourceBrokerV2SagaIntegrityError
    else:
        saga._invoke_with_heartbeat(
            phase=phase,
            operation_id=operation_id,
            owner_generation=generation,
            payload=payload,
            invoke=invoke,
        )

    outcome = boundary[-1]["outcome"]
    assert outcome is not None
    assert outcome.renewal_ok is True
    if shape == "self-healing-tamper":
        assert outcome.ticks >= 3
        assert outcome.digest_changed is True
        # Back where it started: only the sticky flag remembers, which is the
        # whole claim.
        assert outcome.last_digest == outcome.first_digest
        after = _wp9_outbox_bytes(saga.path, operation_id)
        assert after[:9] == before[:9]
    elif shape == "untouched-three-ticks":
        assert outcome.ticks >= 3
        assert outcome.digest_changed is False
        assert outcome.digest_mismatch is False
        assert outcome.last_digest == outcome.first_digest
        assert outcome.observed_digest == outcome.last_digest
    else:
        assert outcome.ticks == 0
        assert outcome.first_digest is None
        assert outcome.last_digest is None
        assert outcome.digest_changed is False
        assert outcome.digest_mismatch is False
        assert outcome.observed_digest is None
    assert _wp9_integrity_check(saga.path) == "ok"
    saga.close()


def _wp9_start_a_helper(saga: SourceBrokerV2Saga) -> Any:
    helper = saga._start_helper_once(operation_id="wp9-lifecycle")
    assert helper.poll() is None
    return helper


def test_wp9_saga_finalizer_releases_the_helper_with_no_strong_reference(
    tmp_path: Path,
) -> None:
    """P1-01: the backstop has to be able to fire while the process is up.

    `weakref.finalize(owner, owner.close)` cannot: a bound method holds the
    owner, so the owner is never collected and the finalizer only ever runs at
    interpreter shutdown.  Registering a module-level function against a state
    object that points at nobody is what makes collection - and therefore the
    backstop - actually happen, and that is what this measures.
    """

    saga, _ = _wp9_saga(tmp_path)
    helper = _wp9_start_a_helper(saga)
    popen = saga._heartbeat_state.popen
    assert popen is not None
    reference = weakref.ref(saga)
    finalizer = saga._heartbeat_finalizer
    assert finalizer.alive
    del helper, saga
    gc.collect()
    assert reference() is None
    assert not finalizer.alive
    popen.wait(timeout=10)
    assert popen.returncode is not None


def test_wp9_saga_close_is_idempotent_and_survives_a_second_call(tmp_path: Path) -> None:
    saga, _ = _wp9_saga(tmp_path)
    _wp9_start_a_helper(saga)
    first = saga.close()
    assert first.orphaned is False
    assert first.returncode is not None
    second = saga.close()
    assert second.orphaned is False
    assert saga._heartbeat_state.popen is None
    assert not saga._orphaned_heartbeat_children


def test_wp9_saga_close_retries_while_an_orphan_is_unconfirmed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An orphan is not a closed saga, and a closed saga is not retryable.

    Only an observed `returncode` counts as closed.  While a kill has been sent
    and its effect not seen, the state stays populated so a later `poll` can
    still resolve it - and the typed error is raised after the cleanup, from
    outside the `finally`, so nothing is left half released.
    """

    saga, _ = _wp9_saga(tmp_path)
    _wp9_start_a_helper(saga)
    popen = saga._heartbeat_state.popen
    assert popen is not None
    real_wait = popen.wait
    refuse = {"on": True}

    def stubborn_wait(timeout: float | None = None) -> int:
        if refuse["on"]:
            raise subprocess.TimeoutExpired(cmd="helper", timeout=timeout or 0.0)
        return real_wait(timeout=timeout)

    monkeypatch.setattr(popen, "wait", stubborn_wait)
    with pytest.raises(SourceBrokerV2SagaUnavailableError, match="SIGKILL"):
        saga.close()
    assert saga._orphaned_heartbeat_children
    assert saga._heartbeat_state.popen is popen
    assert saga._heartbeat_finalizer.alive
    # The kill really was sent; only its observation was blocked.
    refuse["on"] = False
    assert popen.wait(timeout=10) is not None
    outcome = saga.close()
    assert outcome.orphaned is False
    assert not saga._orphaned_heartbeat_children


def test_wp9_saga_context_manager_keeps_the_block_exception(tmp_path: Path) -> None:
    """A cleanup failure must never displace the reason the block ended.

    `__exit__` forwarding to `close` and returning its result would swallow the
    block's exception outright - any truthy return value does.  With an
    exception already in flight the close failure becomes a note; with none, it
    is allowed to surface.
    """

    saga, _ = _wp9_saga(tmp_path)
    _wp9_start_a_helper(saga)
    boom = ValueError("from inside the block")

    def failing_close(**_: Any) -> Any:
        raise SourceBrokerV2SagaUnavailableError("close failed")

    original_close = saga.close
    saga.close = failing_close  # type: ignore[method-assign]
    with pytest.raises(ValueError) as caught, saga:
        raise boom
    assert caught.value is boom
    assert type(caught.value) is ValueError
    assert caught.value.args == ("from inside the block",)
    notes = getattr(caught.value, "__notes__", [])
    assert len(notes) == 1
    assert "close" in notes[0]
    saga.close = original_close  # type: ignore[method-assign]
    saga.close()


def test_wp9_saga_context_manager_surfaces_a_close_failure_on_a_clean_block(
    tmp_path: Path,
) -> None:
    saga, _ = _wp9_saga(tmp_path)
    _wp9_start_a_helper(saga)

    def failing_close(**_: Any) -> Any:
        raise SourceBrokerV2SagaUnavailableError("close failed")

    original_close = saga.close
    saga.close = failing_close  # type: ignore[method-assign]
    with pytest.raises(SourceBrokerV2SagaUnavailableError, match="close failed"), saga:
        pass
    saga.close = original_close  # type: ignore[method-assign]
    saga.close()


def test_wp9_saga_close_refuses_to_pull_descriptors_from_a_live_session(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """P1-01: `close` and a running window share one gate.

    Closing the control and stop descriptors is how the helper is told to
    leave.  Doing that underneath a session in progress would end the renewals
    for an invocation that is still running, so the gate that admits one window
    at a time also admits `close`.
    """

    saga, _ = _wp9_saga(tmp_path)
    phase, operation_id, generation, payload = _wp9_window(saga)
    outcomes: list[BaseException | None] = []

    def invoke(_: bytes) -> bytes:
        try:
            saga.close()
        except BaseException as exc:
            outcomes.append(exc)
        else:
            outcomes.append(None)
        assert saga._heartbeat_state.popen is not None
        assert saga._heartbeat_state.ctrl_w is not None
        return canonical_json_bytes({"wp9": "result"})

    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(
            saga._invoke_with_heartbeat,
            phase=phase,
            operation_id=operation_id,
            owner_generation=generation,
            payload=payload,
            invoke=invoke,
        )
        future.result(timeout=30)
    assert len(outcomes) == 1
    assert type(outcomes[0]) is SourceBrokerV2SagaConflictError
    saga.close()


def test_wp9_saga_refuses_a_second_window_on_the_same_instance(
    tmp_path: Path,
) -> None:
    """The gate is a non-blocking lock, not a boolean anybody can race."""

    saga, _ = _wp9_saga(tmp_path)
    phase, operation_id, generation, payload = _wp9_window(saga)
    entered = Barrier(2)
    rival_done = Event()
    second: list[BaseException] = []

    def invoke(_: bytes) -> bytes:
        # The first window stays open until the second has tried, so the gate
        # is genuinely contended rather than merely visited twice.
        entered.wait(timeout=10)
        assert rival_done.wait(timeout=10)
        return canonical_json_bytes({"wp9": "result"})

    def rival() -> None:
        entered.wait(timeout=10)
        try:
            saga._invoke_with_heartbeat(
                phase=phase,
                operation_id=operation_id,
                owner_generation=generation,
                payload=payload,
                invoke=lambda _: canonical_json_bytes({"wp9": "rival"}),
            )
        except BaseException as exc:
            second.append(exc)
        finally:
            rival_done.set()

    with ThreadPoolExecutor(max_workers=2) as executor:
        rival_future = executor.submit(rival)
        saga._invoke_with_heartbeat(
            phase=phase,
            operation_id=operation_id,
            owner_generation=generation,
            payload=payload,
            invoke=invoke,
        )
        rival_future.result(timeout=30)
    assert len(second) == 1
    assert type(second[0]) is SourceBrokerV2SagaConflictError
    saga.close()


@pytest.mark.parametrize(
    "fault",
    [
        "popen-raises",
        "exit-immediately",
        "no-ready",
        "ready-wrong-pid",
        "ready-wrong-protocol",
        "ready-garbage",
    ],
)
def test_wp9_a_failed_helper_start_owns_nothing_afterwards(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    fault: str,
) -> None:
    """P1-02: every way a start can fail closes exactly what it opened.

    Six raw descriptors exist between the first `os.pipe` and a successful
    `Popen`, and each of these faults aborts at a different point among them:
    the spawn itself, an immediate exit, a silence, and three frames that are
    the wrong shape.  Counting descriptors before and after is what says none
    of them leaked one; the registry and the state say none of them left a
    process behind either.
    """

    monkeypatch.setattr(source_broker_v2_module, "_HEARTBEAT_HANDSHAKE_FLOOR_SECONDS", 0.12)
    saga, _ = _wp9_saga(tmp_path)
    spawned: list[Any] = []
    real_popen = source_broker_v2_module.subprocess.Popen

    def recording_popen(args: Any, **kwargs: Any) -> Any:
        popen = real_popen(args, **kwargs)
        spawned.append(popen)
        return popen

    if fault == "popen-raises":
        def refusing_popen(*_args: Any, **_kwargs: Any) -> Any:
            raise OSError("cannot spawn")

        monkeypatch.setattr(source_broker_v2_module.subprocess, "Popen", refusing_popen)
    else:
        _wp9_use_fault_helper(saga, monkeypatch, fault)
        monkeypatch.setattr(source_broker_v2_module.subprocess, "Popen", recording_popen)
    before = _wp9_open_descriptors()
    with pytest.raises(SourceBrokerV2SagaUnavailableError):
        saga._start_helper_once(operation_id="wp9-start")
    assert _wp9_open_descriptors() == before
    # Exactly one spawn attempt, and whatever it started is already reaped.
    assert len(spawned) == (0 if fault == "popen-raises" else 1)
    for popen in spawned:
        assert popen.returncode is not None
    assert saga._heartbeat_state.popen is None
    assert saga._heartbeat_state.ctrl_w is None
    assert saga._heartbeat_state.status_r is None
    assert saga._heartbeat_state.stop_w is None
    assert saga._heartbeat_child is None
    assert not saga._orphaned_heartbeat_children
    assert multiprocessing.active_children() == []


def test_wp9_a_second_helper_is_refused_while_one_is_running(tmp_path: Path) -> None:
    saga, _ = _wp9_saga(tmp_path)
    _wp9_start_a_helper(saga)
    before = _wp9_open_descriptors()
    with pytest.raises(SourceBrokerV2SagaUnavailableError, match="already running"):
        saga._start_helper_once(operation_id="wp9-second")
    assert _wp9_open_descriptors() == before
    saga.close()


def test_wp9_an_unconfirmed_orphan_blocks_the_next_window(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """R1 and R3: a kill nobody saw land is a reason to stop, not to retry.

    A process that survived SIGKILL is one this system cannot say anything
    about - it may still be inside a write.  So every later entry refuses
    before the external call rather than starting a second writer beside it.
    """

    saga, transport = _wp9_saga(tmp_path)
    phase, operation_id, generation, payload = _wp9_window(saga)
    helper = _wp9_start_a_helper(saga)
    popen = saga._heartbeat_state.popen
    assert popen is not None
    saga._orphaned_heartbeat_children.append(
        source_broker_v2_module._HeartbeatOrphan(
            popen=popen,
            pid=popen.pid,
            create_time=None,
            killed_at=time.time(),
            operation_id="earlier",
        )
    )
    starts = _wp9_count_helper_starts(saga, monkeypatch)
    calls: list[int] = []
    with pytest.raises(SourceBrokerV2SagaUnavailableError, match="has not reported its exit"):
        saga._invoke_with_heartbeat(
            phase=phase,
            operation_id=operation_id,
            owner_generation=generation,
            payload=payload,
            invoke=lambda _: calls.append(1) or canonical_json_bytes({}),
        )
    assert calls == []
    assert starts == []
    assert transport.dispatch_calls == 0
    del helper
    saga.close()


def test_wp9_entry_probes_the_write_lock_before_reusing_a_cleared_saga(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """P1-04: a dead pid is not the same fact as a released write lock.

    An exit that has been observed clears the orphan, but the file lock is held
    by the kernel, not by the registry, and a process wedged in uninterruptible
    I/O releases neither. So after clearing one, the production path takes the
    write lock and lets it go before it will start anything - and when that
    probe fails, no helper is started and no external call is made.
    """

    saga, transport = _wp9_saga(tmp_path)
    phase, operation_id, generation, payload = _wp9_window(saga)
    exited = subprocess.Popen([sys.executable, "-I", "-c", ""])
    exited.wait(timeout=10)
    probes: list[int] = []
    original_probe = saga._probe_saga_write_lock

    def probing() -> None:
        probes.append(1)
        original_probe()

    monkeypatch.setattr(saga, "_probe_saga_write_lock", probing)
    saga._orphaned_heartbeat_children.append(
        source_broker_v2_module._HeartbeatOrphan(
            popen=exited,
            pid=exited.pid,
            create_time=None,
            killed_at=time.time(),
            operation_id="earlier",
        )
    )
    saga._invoke_with_heartbeat(
        phase=phase,
        operation_id=operation_id,
        owner_generation=generation,
        payload=payload,
        invoke=lambda _: canonical_json_bytes({"wp9": "result"}),
    )
    assert probes == [1]
    assert not saga._orphaned_heartbeat_children
    saga.close()

    # And now the same entry with the probe refusing: nothing starts, nothing
    # is invoked.
    second_root = tmp_path / "second"
    second_root.mkdir()
    saga2, transport2 = _wp9_saga(second_root)
    phase2, operation2, generation2, payload2 = _wp9_window(saga2)
    exited2 = subprocess.Popen([sys.executable, "-I", "-c", ""])
    exited2.wait(timeout=10)
    saga2._orphaned_heartbeat_children.append(
        source_broker_v2_module._HeartbeatOrphan(
            popen=exited2,
            pid=exited2.pid,
            create_time=None,
            killed_at=time.time(),
            operation_id="earlier",
        )
    )

    def refusing() -> None:
        raise SourceBrokerV2SagaUnavailableError("saga write lock is still held")

    monkeypatch.setattr(saga2, "_probe_saga_write_lock", refusing)
    starts = _wp9_count_helper_starts(saga2, monkeypatch)
    calls: list[int] = []
    with pytest.raises(SourceBrokerV2SagaUnavailableError, match="write lock is still held"):
        saga2._invoke_with_heartbeat(
            phase=phase2,
            operation_id=operation2,
            owner_generation=generation2,
            payload=payload2,
            invoke=lambda _: calls.append(1) or canonical_json_bytes({}),
        )
    assert starts == []
    assert calls == []
    assert transport2.dispatch_calls == 0
    assert transport.dispatch_calls == 0


def test_wp9_a_fresh_lease_is_taken_between_the_handshake_and_the_invocation(
    tmp_path: Path,
) -> None:
    """P1-04: the window opens on a lease this executor demonstrably holds.

    A session start is allowed to be slow - a restart costs a terminate, a kill
    and a second handshake - and the lease that covers the invocation was last
    renewed before all of that.  One owner-and-generation guarded renewal after
    the final acknowledgement closes the gap; if ownership changed in between,
    it fails here, before the external call.
    """

    saga, _ = _wp9_saga(tmp_path, lease_seconds=_WP9_QUIET_LEASE_SECONDS)
    phase, operation_id, generation, payload = _wp9_window(saga)
    before = _wp9_heartbeat_columns(saga.path, operation_id)
    observed: list[tuple[Any, Any]] = []

    saga._invoke_with_heartbeat(
        phase=phase,
        operation_id=operation_id,
        owner_generation=generation,
        payload=payload,
        invoke=lambda _: observed.append(_wp9_heartbeat_columns(saga.path, operation_id))
        or canonical_json_bytes({"wp9": "result"}),
    )
    # No renewal is due inside this window - the lease is far longer than it -
    # so the movement can only be the synchronous one after the handshake.
    assert saga._last_heartbeat_session is not None
    assert saga._last_heartbeat_session.ticks == 0
    assert observed[0] != before
    saga.close()


def test_wp9_a_lost_lease_before_the_window_is_still_a_conflict(tmp_path: Path) -> None:
    """The synchronous renewal after the handshake keeps its classification."""

    saga, _ = _wp9_saga(tmp_path, lease_seconds=_WP9_QUIET_LEASE_SECONDS)
    phase, operation_id, generation, payload = _wp9_window(saga)
    calls: list[int] = []
    with pytest.raises(SourceBrokerV2SagaConflictError, match="lost ownership"):
        saga._invoke_with_heartbeat(
            phase=phase,
            operation_id=operation_id,
            owner_generation=generation + 1,
            payload=payload,
            invoke=lambda _: calls.append(1) or canonical_json_bytes({}),
        )
    assert calls == []
    saga.close()


def test_wp9_a_takeover_inside_the_window_outranks_the_digest(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A legitimate takeover keeps its classification, and beats the digest.

    Another executor taking the effect moves the owner columns, which moves the
    row, which moves the digest too - so both signals fire at once and the
    order they are reported in decides what the caller sees.  Ownership first:
    a takeover is a conflict, as it has always been, and calling it tampering
    would change what callers retry and what they escalate.
    """

    saga, _ = _wp9_saga(tmp_path, lease_seconds=_WP9_TAMPER_LEASE_SECONDS)
    interval = _WP9_TAMPER_LEASE_SECONDS / 3
    boundary = _wp9_watch_boundary(saga, monkeypatch)
    phase, operation_id, generation, payload = _wp9_window(saga)

    def invoke(_: bytes) -> bytes:
        # First make the digest move on its own, under an intact ownership, so
        # that when the takeover lands both signals are live at once and the
        # order they are reported in is what decides the answer.
        _wp9_wait_for_renewals(saga, operation_id, 1)
        _wp9_write(
            saga.path,
            "UPDATE source_broker_v2_outbox SET payload_hash = ? WHERE operation_id = ?",
            ("f" * 64, operation_id),
        )
        _wp9_wait_for_renewals(saga, operation_id, 1)
        # Renewals are flowing, so the window after the takeover is long enough
        # to contain several attempts rather than a guess about this host.
        connection = sqlite3.connect(saga.path, timeout=5.0, isolation_level=None)
        try:
            connection.execute("BEGIN IMMEDIATE")
            connection.execute(
                "UPDATE source_broker_v2_outbox SET executor_owner_token = ?, "
                "executor_generation = ? WHERE operation_id = ?",
                ("another-executor", generation + 1, operation_id),
            )
            connection.commit()
        finally:
            connection.close()
        time.sleep(interval * 4)
        return canonical_json_bytes({"wp9": "result"})

    with pytest.raises(SourceBrokerV2SagaUnavailableError) as caught:
        saga._invoke_with_heartbeat(
            phase=phase,
            operation_id=operation_id,
            owner_generation=generation,
            payload=payload,
            invoke=invoke,
        )
    assert type(caught.value.__cause__) is SourceBrokerV2SagaConflictError
    outcome = boundary[-1]["outcome"]
    assert outcome is not None
    assert outcome.acked is True
    assert outcome.renewal_ok is False
    assert outcome.escalation == "clean"
    assert outcome.child_alive is True
    # The digest moved too - the owner columns are part of the row - and it is
    # still not what got reported.
    assert outcome.digest_changed is True
    assert outcome.digest_mismatch is False
    saga.close()


def _wp9_watch_for_exit(pid: int) -> Callable[[float], bool]:
    """Register interest in a process's exit *before* anything kills it.

    Registration is what makes this immune to pid reuse: both `pidfd_open` and
    a kqueue `EVFILT_PROC` bind to the process that exists at this instant, not
    to the number.  Reading a creation timestamp afterwards and comparing would
    not do the same job - on macOS `ps -o lstart=` is only accurate to the
    second, so two processes started in the same second are indistinguishable.

    The returned callable waits for the exit and says whether it arrived.
    """

    if sys.platform == "linux":
        descriptor = os.pidfd_open(pid)

        def wait_pidfd(timeout: float) -> bool:
            ready, _, _ = select.select([descriptor], [], [], timeout)
            os.close(descriptor)
            return bool(ready)

        return wait_pidfd

    queue = select.kqueue()
    queue.control(
        [
            select.kevent(
                pid,
                filter=select.KQ_FILTER_PROC,
                flags=select.KQ_EV_ADD | select.KQ_EV_ENABLE,
                fflags=select.KQ_NOTE_EXIT,
            )
        ],
        0,
        0,
    )

    def wait_kqueue(timeout: float) -> bool:
        events = queue.control(None, 1, timeout)
        queue.close()
        return bool(events)

    return wait_kqueue


def _wp9_stop_pipe_descriptors(pid: int, inode: int) -> list[tuple[int, int]]:
    """Every descriptor `pid` holds on the stop pipe, with its access mode."""

    found: list[tuple[int, int]] = []
    for name in os.listdir(f"/proc/{pid}/fd"):
        try:
            if os.stat(f"/proc/{pid}/fd/{name}").st_ino != inode:
                continue
            with open(f"/proc/{pid}/fdinfo/{name}", encoding="utf-8") as handle:
                info = handle.read()
        except OSError:
            continue
        flags = 0
        for line in info.splitlines():
            if line.startswith("flags:"):
                flags = int(line.split()[1], 8)
        found.append((int(name), flags & os.O_ACCMODE))
    return found


def test_v2_saga_helper_dies_with_its_parent_and_releases_the_write_lock(
    tmp_path: Path,
) -> None:
    """T9: the helper is not an orphan when the process that started it dies.

    Three layers, because pytest cannot both be the helper's parent and survive
    to make the assertions: pytest supervises, a middle process runs the real
    launch path and is killed outright, and the production helper sits under
    it.  There is no `PR_SET_PDEATHSIG` here and none is wanted - it is
    Linux-only.  The portable mechanism is that the parent holds the only write
    end of a pipe the helper is reading, so the kernel closes it when the
    parent dies and the helper wakes at once on EOF.

    The exit is observed with a handle registered before the kill, so pid reuse
    cannot make this pass by accident.  On Linux the fd assertion also runs: it
    is what proves the helper never held the write end itself - if it did, the
    EOF would never come and this whole mechanism would be silently dead.
    macOS has no `/proc` and `lsof` will not attribute an inode, so that one
    assertion is skipped there and said so; every other assertion runs on both.
    """

    root = tmp_path / "t9"
    root.mkdir()
    public_key = root / "authority.pub.pem"
    public_key.write_bytes(_source_authority_security().public_key)
    report = root / "report.json"
    database = root / "saga.sqlite3"
    lease_seconds = _WP9_TICKING_LEASE_SECONDS
    interval = lease_seconds / 3
    operation_id = _operation_id("saga-t9", "wp9-parent-death")
    layer = subprocess.Popen(
        (
            sys.executable,
            "-I",
            str(_WP9_PARENT_DEATH_LAYER),
            "--db",
            str(database),
            "--root",
            str(root),
            "--public-key",
            str(public_key),
            "--saga-id",
            "saga-t9",
            "--operation-id",
            operation_id,
            "--report",
            str(report),
            "--lease-seconds",
            str(lease_seconds),
            "--busy-timeout-ms",
            str(_WP9_BUSY_TIMEOUT_MS),
        ),
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    try:
        def reported() -> bool:
            if layer.poll() is not None:
                raise AssertionError(
                    f"parent-death layer exited early: {layer.stderr.read().decode()}"
                    if layer.stderr is not None
                    else "parent-death layer exited early"
                )
            return report.exists()

        _wp9_wait_for(reported, what="the parent-death layer to open its window")
        record = json.loads(report.read_text(encoding="utf-8"))
        helper_pid = int(record["helper_pid"])
        stop_inode = int(record["stop_inode"])
        assert helper_pid != layer.pid

        if sys.platform == "linux":
            descriptors = _wp9_stop_pipe_descriptors(helper_pid, stop_inode)
            assert len(descriptors) == 1
            assert descriptors[0][1] == os.O_RDONLY
        # macOS: `/proc` does not exist and `lsof` cannot attribute a pipe
        # inode to an end, so this one assertion has no equivalent here.  Every
        # other assertion in this case runs on both platforms.

        # A watch on a pid that is gone has to fail loudly.  Were it silently
        # accepted, the wait below would report an exit for a process that was
        # never being watched, and this case would pass without evidence.
        departed = subprocess.Popen([sys.executable, "-I", "-c", ""])
        departed.wait(timeout=10)
        with pytest.raises(ProcessLookupError):
            _wp9_watch_for_exit(departed.pid)
        wait_for_helper_exit = _wp9_watch_for_exit(helper_pid)

        layer.kill()
        layer.wait(timeout=10)
        assert wait_for_helper_exit(10.0)
    finally:
        if layer.poll() is None:
            layer.kill()
            layer.wait(timeout=10)
        if layer.stdout is not None:
            layer.stdout.close()
        if layer.stderr is not None:
            layer.stderr.close()

    taken, code, elapsed = _wp9_probe_write_lock(database, timeout_ms=_WP9_BUSY_TIMEOUT_MS)
    assert taken, code
    assert elapsed < _WP9_BUSY_TIMEOUT_MS / 1_000
    settled = _wp9_heartbeat_columns(database, operation_id)
    time.sleep(interval * 2)
    assert _wp9_heartbeat_columns(database, operation_id) == settled
    assert _wp9_integrity_check(database) == "ok"


def test_wp9_helper_is_launched_with_three_descriptors_and_no_secrets(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The whole shape of the launch, asserted where it is decided.

    Three inherited descriptors and no more, everything else closed, its own
    session so signals go to it and nothing else, an environment built from
    nothing, and a command line carrying only numbers.  The owner token and the
    database path are the two secrets here and both travel in the config frame:
    argv is world-readable through `ps`.
    """

    saga, _ = _wp9_saga(tmp_path)
    recorded: list[dict[str, Any]] = []
    real_popen = source_broker_v2_module.subprocess.Popen

    def recording_popen(args: Any, **kwargs: Any) -> Any:
        recorded.append({"args": list(args), **kwargs})
        return real_popen(args, **kwargs)

    monkeypatch.setattr(source_broker_v2_module.subprocess, "Popen", recording_popen)
    saga._start_helper_once(operation_id="wp9-launch")
    try:
        assert len(recorded) == 1
        launch = recorded[0]
        assert launch["args"][:4] == [
            sys.executable,
            "-I",
            "-m",
            "rquant.source_broker_v2_heartbeat",
        ]
        assert len(launch["pass_fds"]) == 3
        assert launch["close_fds"] is True
        assert launch["start_new_session"] is True
        assert set(launch["env"]) <= {"PATH", "LC_ALL", "LANG", "TMPDIR", "SQLITE_TMPDIR"}
        assert launch["stdin"] is subprocess.DEVNULL
        assert launch["stdout"] is subprocess.DEVNULL
        assert launch["stderr"] is subprocess.DEVNULL
        argv = " ".join(launch["args"])
        assert saga._executor_owner_token not in argv
        assert str(saga.path) not in argv
        assert saga.saga_id not in argv
        assert saga._executor_owner_token not in " ".join(launch["env"].values())
        # And the child really only holds those three.
        if sys.platform == "linux":
            popen = saga._heartbeat_state.popen
            assert popen is not None
            held = sorted(os.listdir(f"/proc/{popen.pid}/fd"))
            assert len(held) == 6  # 0, 1, 2 on /dev/null plus the three pipes
    finally:
        saga.close()


def test_wp9_shutdown_bound_is_closed_form_at_production_defaults(tmp_path: Path) -> None:
    """The number a caller can rely on, written out in full.

    `T1` is two write-lock windows and nothing else - the helper sleeps in a
    `select` this process can wake, so there is no term for the interval
    between renewals any more.  On production defaults, busy_timeout 5s, that
    is 10 + 0.25 + 5 + 0.05 = 15.30s, whatever the disk is doing.
    """

    saga, _ = _wp9_saga(tmp_path, busy_timeout_ms=5_000, lease_seconds=30.0)
    assert saga._heartbeat_shutdown_budget() == 10.0
    assert saga._heartbeat_handshake_budget() == 5.0
    assert saga._heartbeat_idle_exit_seconds() == 60.0
    bound = (
        saga._heartbeat_shutdown_budget()
        + source_broker_v2_module._HEARTBEAT_TERMINATE_SECONDS
        + source_broker_v2_module._HEARTBEAT_FINAL_REAP_SECONDS
        + 0.05
    )
    assert bound == pytest.approx(15.30)
    session_start = (
        2 * (saga._heartbeat_handshake_budget() + saga._heartbeat_handshake_budget())
        + source_broker_v2_module._HEARTBEAT_TERMINATE_SECONDS
        + source_broker_v2_module._HEARTBEAT_FINAL_REAP_SECONDS
        + 0.05
    )
    assert session_start == pytest.approx(25.30)
    close_bound = (
        source_broker_v2_module._HEARTBEAT_EOF_SECONDS
        + source_broker_v2_module._HEARTBEAT_TERMINATE_SECONDS
        + source_broker_v2_module._HEARTBEAT_FINAL_REAP_SECONDS
        + 0.05
    )
    assert close_bound == pytest.approx(5.55)


def test_wp9_finalizer_is_registered_against_a_plain_function_and_state(
    tmp_path: Path,
) -> None:
    """The registration itself, checked rather than inferred.

    `weakref.finalize(saga, saga.close)` looks equivalent and is not: a bound
    method holds the instance, the instance is then never collected, and the
    finalizer only ever runs at interpreter exit.  What has to be registered is
    a module-level function and a state object that points at nobody.
    """

    saga, _ = _wp9_saga(tmp_path)
    finalizer = saga._heartbeat_finalizer
    assert finalizer.alive
    _, function, args, _ = finalizer.peek()
    assert function is source_broker_v2_module._close_resources
    assert args == (saga._heartbeat_state,)
    state = saga._heartbeat_state
    assert not any(value is saga for value in vars(state).values())
    assert state.orphans is saga._orphaned_heartbeat_children
    saga.close()


def test_wp9_close_resources_reports_rather_than_raises(tmp_path: Path) -> None:
    """The finalizer's callable has nowhere to raise to, so it never does."""

    saga, _ = _wp9_saga(tmp_path)
    state = saga._heartbeat_state
    assert source_broker_v2_module._close_resources(state).closed is True
    _wp9_start_a_helper(saga)
    first = source_broker_v2_module._close_resources(state)
    assert first.orphaned is False
    assert first.returncode is not None
    second = source_broker_v2_module._close_resources(state)
    assert second.orphaned is False
    assert state.popen is None
    assert state.ctrl_w is None
    assert state.stop_w is None
    assert state.status_r is None


def test_wp9_a3_stops_rather_than_starting_a_helper_beside_an_orphan(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A replacement is not started next to a kill nobody saw land.

    The restart the session confirmation is allowed exists for a helper that is
    known to be gone.  When the one being replaced cannot be confirmed dead,
    starting a second writer beside it is the one thing that must not happen -
    so A3 ends there instead, before the external call.
    """

    saga, transport = _wp9_saga(tmp_path)
    phase, operation_id, generation, payload = _wp9_window(saga)
    _wp9_start_a_helper(saga)
    popen = saga._heartbeat_state.popen
    assert popen is not None
    real_wait = popen.wait

    def never_observed(timeout: float | None = None) -> int:
        raise subprocess.TimeoutExpired(cmd="helper", timeout=timeout or 0.0)

    # A control descriptor whose reader is gone, swapped in for the real one:
    # the session write hits EPIPE while the helper itself stays alive, so the
    # pre-check does not get to notice anything first.
    real_ctrl_w = saga._heartbeat_state.ctrl_w
    assert real_ctrl_w is not None
    dead_r, dead_w = os.pipe()
    os.close(dead_r)
    saga._heartbeat_state.ctrl_w = dead_w
    monkeypatch.setattr(popen, "wait", never_observed)
    starts = _wp9_count_helper_starts(saga, monkeypatch)
    calls: list[int] = []

    with pytest.raises(SourceBrokerV2SagaUnavailableError, match="has not reported its exit"):
        saga._invoke_with_heartbeat(
            phase=phase,
            operation_id=operation_id,
            owner_generation=generation,
            payload=payload,
            invoke=lambda _: calls.append(1) or canonical_json_bytes({}),
        )
    assert starts == []
    assert calls == []
    assert transport.dispatch_calls == 0
    assert len(saga._orphaned_heartbeat_children) == 1
    # The kill was really sent; only its observation was blocked.
    monkeypatch.setattr(popen, "wait", real_wait)
    os.close(real_ctrl_w)
    assert popen.wait(timeout=10) is not None
    saga._sweep_orphaned_heartbeat_children()
    assert not saga._orphaned_heartbeat_children


def test_wp9_a_missing_row_still_fails_the_synchronous_renewal_as_integrity(
    tmp_path: Path,
) -> None:
    """The synchronous validation points keep the classification they had.

    The shared write reports a missing row in its own vocabulary; in this
    process that has always been an integrity failure, and callers match on it.
    """

    saga, _ = _wp9_saga(tmp_path)
    phase, operation_id, _, _ = _wp9_window(saga)
    _wp9_write(
        saga.path,
        "DELETE FROM source_broker_v2_outbox WHERE operation_id = ?",
        (operation_id,),
    )
    with pytest.raises(SourceBrokerV2SagaIntegrityError, match="required outbox effect is missing"):
        saga._heartbeat_outbox(phase=phase, operation_id=operation_id, owner_generation=0)
