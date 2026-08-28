from __future__ import annotations

import json
import multiprocessing
import os
import shutil
import socket
import sqlite3
import struct
import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from pathlib import Path
from threading import Barrier, Event, Lock, Thread
from types import ModuleType
from uuid import UUID

import pytest

from rquant.external_monotonic_root import (
    UnixSocketExternalMonotonicRootClient,
    UnixSocketExternalMonotonicRootManifest,
)
from rquant.lab_shard_protocol import LabShardClaimV2, LabShardDefinition, StrategyShardPayloadV2
from rquant.runtime_contracts import canonical_sha256
from rquant.source_operation_contracts import (
    CurrentClaimPlanIssueV2,
    SourceAttemptBindingV2,
    SourceIntentV2,
    SourceResourceRequestV2,
    SourceUsePlanV2,
)
from rquant.strict_json import canonical_model_json_bytes, strict_model_validate_canonical_json
from tests.unit.test_adapter_manifest import (
    NOW,
    Authorities,
    create_test_authorities,
    signed_manifest,
)

_DEFAULT_ATTEMPT_ID = UUID("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")


class _RootSigner:
    issuer = "current-claim-root-test-issuer"
    key_id = "current-claim-root-test-key"
    key_purpose = "current-claim-monotonic-root"
    signature_algorithm = "ed25519"
    public_key_fingerprint = "e" * 64

    def sign(self, *, namespace: str, payload: bytes) -> str:
        return canonical_sha256(
            {"key": self.key_id, "namespace": namespace, "payload": payload.hex()}
        )

    def verify(self, *, namespace: str, payload: bytes, signature: str) -> bool:
        return signature == self.sign(namespace=namespace, payload=payload)


def _root_json(root: object) -> str:
    return canonical_model_json_bytes(root).decode("utf-8")


def _external_receipt_json(
    api: ModuleType,
    *,
    request: object,
    state_json: str | None,
    signer: _RootSigner,
) -> str | None:
    if state_json is None:
        return None
    state = strict_model_validate_canonical_json(
        api.CurrentClaimAntiRollbackRoot,
        state_json,
    )
    unsigned = api.CurrentClaimExternalRootReceipt(
        schema_version=1,
        contract="rquant-current-claim-external-root-receipt/v1",
        role=state.role,
        root_authority_id=state.root_authority_id,
        root_store_id=state.root_store_id,
        current_claim_authority_id=state.current_claim_authority_id,
        request_kind=request.kind,
        request_hash=request.request_hash,
        challenge_nonce=request.challenge_nonce,
        operation_id=state.operation_id,
        previous_checkpoint_hash=state.previous_checkpoint_hash,
        checkpoint=state.checkpoint,
        issuer=signer.issuer,
        key_id=signer.key_id,
        key_purpose=signer.key_purpose,
        namespace="rquant-current-claim-anti-rollback-root/v1",
        signature_algorithm=signer.signature_algorithm,
        public_key_fingerprint=signer.public_key_fingerprint,
        signature="pending",
    )
    receipt = unsigned.model_copy(
        update={
            "signature": signer.sign(
                namespace="rquant-current-claim-anti-rollback-root/v1",
                payload=unsigned.signing_bytes(),
            )
        }
    )
    return _root_json(receipt)


def _external_request(api: ModuleType, request_json: str) -> object:
    return strict_model_validate_canonical_json(api.ExternalMonotonicRootRequest, request_json)


def _request_checkpoint(api: ModuleType, request: object) -> object:
    assert request.checkpoint_json is not None
    return strict_model_validate_canonical_json(
        api.CurrentClaimCheckpoint,
        request.checkpoint_json,
    )


def _api() -> ModuleType:
    import rquant.current_claim_authority as api

    return api


class _MemoryRoot:
    """Independent test double; its state is intentionally outside authority SQLite."""

    def __init__(self, path: Path, api: ModuleType) -> None:
        self._path = path.resolve()
        self._api = api
        self._lock = Lock()
        self._roots: dict[str, object] = {}
        self._operations: dict[str, object] = {}
        self._signer = _RootSigner()
        self.fail = False
        self.lose_response_once = False

    @property
    def authority_id(self) -> str:
        return "test-independent-current-root"

    @property
    def role(self) -> str:
        return "current_claim_monotonic_root"

    @property
    def store_id(self) -> str:
        return "test-independent-current-root-store"

    @property
    def key_id(self) -> str:
        return self._signer.key_id

    @property
    def public_key_fingerprint(self) -> str:
        return self._signer.public_key_fingerprint

    @property
    def transport(self) -> str:
        return "nonproduction-inprocess-v1"

    @property
    def manifest_hash(self) -> str:
        return canonical_sha256({"test_client": "memory-current-root-v1"})

    @property
    def rollback_domain_id(self) -> str:
        return "remote-current-claim-witness-test-domain"

    @property
    def storage_path(self) -> Path:
        return self._path

    def current(self, *, current_claim_authority_id: str) -> str | None:
        if self.fail:
            raise ConnectionError("root unavailable")
        with self._lock:
            root = self._roots.get(current_claim_authority_id)
            return None if root is None else _root_json(root)

    def pin(
        self,
        *,
        operation_id: str,
        current_claim_authority_id: str,
        checkpoint: object,
    ) -> str:
        return self._write(
            operation_id=operation_id,
            current_claim_authority_id=current_claim_authority_id,
            previous_checkpoint_hash="0" * 64,
            checkpoint=checkpoint,
        )

    def compare_and_advance(
        self,
        *,
        operation_id: str,
        current_claim_authority_id: str,
        previous_checkpoint_hash: str,
        checkpoint: object,
    ) -> str:
        root_json = self._write(
            operation_id=operation_id,
            current_claim_authority_id=current_claim_authority_id,
            previous_checkpoint_hash=previous_checkpoint_hash,
            checkpoint=checkpoint,
        )
        if self.lose_response_once:
            self.lose_response_once = False
            raise ConnectionError("root commit response lost")
        return root_json

    def invoke(self, *, request_json: str) -> str | None:
        request = _external_request(self._api, request_json)
        if request.kind == "current":
            state_json = self.current(current_claim_authority_id=request.subject_authority_id)
        else:
            checkpoint = _request_checkpoint(self._api, request)
            assert request.operation_id is not None
            if request.kind == "pin":
                state_json = self.pin(
                    operation_id=request.operation_id,
                    current_claim_authority_id=request.subject_authority_id,
                    checkpoint=checkpoint,
                )
            else:
                assert request.previous_checkpoint_hash is not None
                state_json = self.compare_and_advance(
                    operation_id=request.operation_id,
                    current_claim_authority_id=request.subject_authority_id,
                    previous_checkpoint_hash=request.previous_checkpoint_hash,
                    checkpoint=checkpoint,
                )
        return _external_receipt_json(
            self._api,
            request=request,
            state_json=state_json,
            signer=self._signer,
        )

    def _write(
        self,
        *,
        operation_id: str,
        current_claim_authority_id: str,
        previous_checkpoint_hash: str,
        checkpoint: object,
    ) -> str:
        if self.fail:
            raise ConnectionError("root unavailable")
        with self._lock:
            request_hash = canonical_sha256(
                {
                    "authority": current_claim_authority_id,
                    "previous": previous_checkpoint_hash,
                    "checkpoint": checkpoint.model_dump(mode="python"),
                }
            )
            prior = self._operations.get(operation_id)
            if prior is not None:
                if prior[0] != request_hash:
                    raise ValueError("root operation was rebound")
                return _root_json(prior[1])
            current = self._roots.get(current_claim_authority_id)
            if current is None:
                if previous_checkpoint_hash != "0" * 64:
                    raise ValueError("root pin predecessor is invalid")
            elif current.checkpoint.checkpoint_hash != previous_checkpoint_hash:
                raise ValueError("root compare and swap failed")
            unsigned = self._api.CurrentClaimAntiRollbackRoot(
                schema_version=1,
                contract="rquant-current-claim-anti-rollback-root/v1",
                role="current_claim_monotonic_root",
                root_authority_id=self.authority_id,
                root_store_id=self.store_id,
                current_claim_authority_id=current_claim_authority_id,
                operation_id=operation_id,
                previous_checkpoint_hash=previous_checkpoint_hash,
                checkpoint=checkpoint,
                issuer=self._signer.issuer,
                key_id=self._signer.key_id,
                key_purpose=self._signer.key_purpose,
                namespace="rquant-current-claim-anti-rollback-root/v1",
                signature_algorithm=self._signer.signature_algorithm,
                public_key_fingerprint=self._signer.public_key_fingerprint,
                signature="pending",
            )
            root = unsigned.model_copy(
                update={
                    "signature": self._signer.sign(
                        namespace="rquant-current-claim-anti-rollback-root/v1",
                        payload=unsigned.signing_bytes(),
                    )
                }
            )
            self._roots[current_claim_authority_id] = root
            self._operations[operation_id] = (request_hash, root)
            return _root_json(root)


class _FailingSigner:
    def __init__(self, delegate: object) -> None:
        self._delegate = delegate
        self.fail = False

    @property
    def issuer(self) -> str:
        return self._delegate.issuer

    @property
    def key_id(self) -> str:
        return self._delegate.key_id

    def sign(self, *, namespace: str, payload: bytes) -> str:
        if self.fail:
            raise RuntimeError("signer unavailable")
        return self._delegate.sign(namespace=namespace, payload=payload)


class _LoseConcreteRootResponseOnce:
    def __init__(self, inner: object) -> None:
        self._inner = inner
        self.lose_once = False

    @property
    def authority_id(self) -> str:
        return self._inner.authority_id

    @property
    def role(self) -> str:
        return self._inner.role

    @property
    def store_id(self) -> str:
        return self._inner.store_id

    @property
    def storage_path(self) -> Path:
        return self._inner.storage_path

    @property
    def key_id(self) -> str:
        return self._inner.key_id

    @property
    def public_key_fingerprint(self) -> str:
        return self._inner.public_key_fingerprint

    @property
    def transport(self) -> str:
        return "nonproduction-inprocess-v1"

    @property
    def manifest_hash(self) -> str:
        return canonical_sha256({"test_client": "lossy-current-root-v1"})

    @property
    def rollback_domain_id(self) -> str:
        return "remote-current-claim-witness-test-domain"

    def current(self, *, current_claim_authority_id: str) -> str | None:
        root = self._inner.current(current_claim_authority_id=current_claim_authority_id)
        return None if root is None else _root_json(root)

    def pin(self, **kwargs: object) -> str:
        return _root_json(self._inner.pin(**kwargs))

    def compare_and_advance(self, **kwargs: object) -> str:
        result = self._inner.compare_and_advance(**kwargs)
        if self.lose_once:
            self.lose_once = False
            raise ConnectionError("concrete root commit response lost")
        return _root_json(result)

    def invoke(self, *, request_json: str) -> str | None:
        api = _api()
        request = _external_request(api, request_json)
        if request.kind == "current":
            state_json = self.current(current_claim_authority_id=request.subject_authority_id)
        else:
            checkpoint = _request_checkpoint(api, request)
            assert request.operation_id is not None
            if request.kind == "pin":
                state_json = self.pin(
                    operation_id=request.operation_id,
                    current_claim_authority_id=request.subject_authority_id,
                    checkpoint=checkpoint,
                )
            else:
                assert request.previous_checkpoint_hash is not None
                state_json = self.compare_and_advance(
                    operation_id=request.operation_id,
                    current_claim_authority_id=request.subject_authority_id,
                    previous_checkpoint_hash=request.previous_checkpoint_hash,
                    checkpoint=checkpoint,
                )
        return _external_receipt_json(
            api,
            request=request,
            state_json=state_json,
            signer=_RootSigner(),
        )


class _ExternalWitnessServiceClient:
    """Test service client; production code only sees the closed external adapter."""

    def __init__(self, api: ModuleType, path: Path, *, signer: object | None = None) -> None:
        self._api = api
        self._signer = signer or _RootSigner()
        self._inner = api.SQLiteCurrentClaimMonotonicRoot(
            path,
            authority_id="current-claim-root",
            store_id="current-claim-root-store-a",
            signer=self._signer,
        )

    @property
    def authority_id(self) -> str:
        return "current-claim-root"

    @property
    def role(self) -> str:
        return "current_claim_monotonic_root"

    @property
    def store_id(self) -> str:
        return "current-claim-root-store-a"

    @property
    def transport(self) -> str:
        return "nonproduction-inprocess-v1"

    @property
    def manifest_hash(self) -> str:
        return canonical_sha256({"test_client": "sqlite-current-root-service-v1"})

    @property
    def rollback_domain_id(self) -> str:
        return "remote-current-claim-witness-test-domain"

    def current(self, **kwargs: object) -> str | None:
        root = self._inner.current(**kwargs)
        return None if root is None else _root_json(root)

    def pin(self, **kwargs: object) -> str:
        return _root_json(self._inner.pin(**kwargs))

    def compare_and_advance(self, **kwargs: object) -> str:
        return _root_json(self._inner.compare_and_advance(**kwargs))

    def invoke(self, *, request_json: str) -> str | None:
        request = _external_request(self._api, request_json)
        if request.kind == "current":
            state_json = self.current(current_claim_authority_id=request.subject_authority_id)
        else:
            checkpoint = _request_checkpoint(self._api, request)
            assert request.operation_id is not None
            if request.kind == "pin":
                state_json = self.pin(
                    operation_id=request.operation_id,
                    current_claim_authority_id=request.subject_authority_id,
                    checkpoint=checkpoint,
                )
            else:
                assert request.previous_checkpoint_hash is not None
                state_json = self.compare_and_advance(
                    operation_id=request.operation_id,
                    current_claim_authority_id=request.subject_authority_id,
                    previous_checkpoint_hash=request.previous_checkpoint_hash,
                    checkpoint=checkpoint,
                )
        return _external_receipt_json(
            self._api,
            request=request,
            state_json=state_json,
            signer=self._signer,
        )


class _TransformingExternalClient:
    def __init__(self, inner: object, transform: Callable[[str], str]) -> None:
        self._inner = inner
        self._transform = transform

    @property
    def authority_id(self) -> str:
        return self._inner.authority_id

    @property
    def role(self) -> str:
        return self._inner.role

    @property
    def store_id(self) -> str:
        return self._inner.store_id

    @property
    def transport(self) -> str:
        return self._inner.transport

    @property
    def manifest_hash(self) -> str:
        return self._inner.manifest_hash

    @property
    def rollback_domain_id(self) -> str:
        return self._inner.rollback_domain_id

    def current(self, **kwargs: object) -> str | None:
        response = self._inner.current(**kwargs)
        return None if response is None else self._transform(response)

    def pin(self, **kwargs: object) -> str:
        return self._transform(self._inner.pin(**kwargs))

    def compare_and_advance(self, **kwargs: object) -> str:
        return self._transform(self._inner.compare_and_advance(**kwargs))

    def invoke(self, *, request_json: str) -> str | None:
        response = self._inner.invoke(request_json=request_json)
        return None if response is None else self._transform(response)


class _ReplayExternalClient:
    def __init__(self, inner: object, api: ModuleType) -> None:
        self._inner = inner
        self._api = api
        self.replay_kind: str | None = None
        self._responses: dict[str, str] = {}

    @property
    def authority_id(self) -> str:
        return self._inner.authority_id

    @property
    def role(self) -> str:
        return self._inner.role

    @property
    def store_id(self) -> str:
        return self._inner.store_id

    @property
    def transport(self) -> str:
        return self._inner.transport

    @property
    def manifest_hash(self) -> str:
        return canonical_sha256(
            {"test_client": "replay-current-root-v1", "inner": self._inner.manifest_hash}
        )

    @property
    def rollback_domain_id(self) -> str:
        return self._inner.rollback_domain_id

    def invoke(self, *, request_json: str) -> str | None:
        request = _external_request(self._api, request_json)
        if self.replay_kind == request.kind and request.kind in self._responses:
            return self._responses[request.kind]
        response = self._inner.invoke(request_json=request_json)
        if response is not None:
            self._responses[request.kind] = response
        return response


class _UnixRootServer:
    def __init__(self, path: Path, handler: object) -> None:
        self.path = path
        self._handler = handler
        self._stop = Event()
        self._ready = Event()
        self._thread = Thread(target=self._serve, daemon=True)
        self.lose_next_advance_response = False
        self.response_fragment_delay_seconds = 0.0
        self.requests: list[object] = []

    def __enter__(self) -> _UnixRootServer:
        self._thread.start()
        assert self._ready.wait(timeout=5)
        return self

    def __exit__(self, *_args: object) -> None:
        self._stop.set()
        try:
            with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as wake:
                wake.connect(os.fspath(self.path))
        except OSError:
            pass
        self._thread.join(timeout=5)
        self.path.unlink(missing_ok=True)

    def _serve(self) -> None:
        self.path.unlink(missing_ok=True)
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as listener:
            listener.bind(os.fspath(self.path))
            os.chmod(self.path, 0o600)
            listener.listen(16)
            listener.settimeout(0.1)
            self._ready.set()
            while not self._stop.is_set():
                try:
                    connection, _ = listener.accept()
                except TimeoutError:
                    continue
                with connection:
                    try:
                        size = struct.unpack("!Q", self._receive_exact(connection, 8))[0]
                        request_json = self._receive_exact(connection, size).decode("utf-8")
                        request = _external_request(_api(), request_json)
                        self.requests.append(request)
                        response = self._handler.invoke(request_json=request_json)
                        if self.lose_next_advance_response and request.kind == "advance":
                            self.lose_next_advance_response = False
                            continue
                        response_bytes = b"" if response is None else response.encode("utf-8")
                        framed = struct.pack("!Q", len(response_bytes)) + response_bytes
                        if self.response_fragment_delay_seconds:
                            for byte in framed:
                                connection.sendall(bytes((byte,)))
                                time.sleep(self.response_fragment_delay_seconds)
                        else:
                            connection.sendall(framed)
                    except (ConnectionError, OSError, UnicodeError, struct.error):
                        if not self._stop.is_set():
                            continue

    @staticmethod
    def _receive_exact(connection: socket.socket, size: int) -> bytes:
        value = bytearray()
        while len(value) < size:
            chunk = connection.recv(size - len(value))
            if not chunk:
                raise ConnectionError("test root request was truncated")
            value.extend(chunk)
        return bytes(value)


def _external_config(api: ModuleType, client: object) -> object:
    verifier = _RootSigner()
    return api.ExternalCurrentClaimRootConfig(
        adapter_id="rquant-external-monotonic-root-cas-v1",
        transport=client.transport,
        transport_manifest_hash=client.manifest_hash,
        root_authority_id=client.authority_id,
        root_store_id=client.store_id,
        root_issuer=verifier.issuer,
        root_key_id=verifier.key_id,
        root_public_key_fingerprint=verifier.public_key_fingerprint,
        witness_rollback_domain_id=client.rollback_domain_id,
        local_rollback_domain_id="local-current-claim-test-domain",
    )


def _external_adapter(
    api: ModuleType,
    *,
    path: Path,
    client: object | None = None,
) -> object:
    selected = client or _ExternalWitnessServiceClient(api, path)
    verifier = _RootSigner()
    return api.ExternalCurrentClaimMonotonicRootAdapter.for_nonproduction_test(
        config=_external_config(api, selected),
        client=selected,
        root_verifiers=(verifier,),
    )


def _production_external_adapter(
    api: ModuleType,
    *,
    socket_path: Path,
    timeout_ms: int = 2_000,
) -> object:
    socket_metadata = socket_path.lstat()
    manifest = UnixSocketExternalMonotonicRootManifest(
        role="current_claim_monotonic_root",
        authority_id="current-claim-root",
        store_id="current-claim-root-store-a",
        rollback_domain_id="remote-current-claim-witness-test-domain",
        socket_path=socket_path,
        socket_uid=socket_metadata.st_uid,
        socket_gid=socket_metadata.st_gid,
        peer_uid=os.getuid(),
        peer_gid=os.getgid(),
        connect_timeout_ms=timeout_ms,
        max_response_bytes=2_000_000,
    )
    client = UnixSocketExternalMonotonicRootClient(manifest)
    return api.ExternalCurrentClaimMonotonicRootAdapter(
        config=_external_config(api, client),
        client=client,
        root_verifiers=(_RootSigner(),),
    )


def _multiprocess_issue(
    start: multiprocessing.synchronize.Event,
    output: multiprocessing.queues.Queue,
    *,
    authority_path: Path,
    root_path: Path,
    authorities: Authorities,
    issue: CurrentClaimPlanIssueV2,
) -> None:
    api = _api()
    try:
        root = _external_adapter(api, path=root_path)
        authority = api.PersistentCurrentClaimAuthority(
            authority_path,
            authority_id="global-source-use",
            signer=authorities.plan_v2,
            keyring=authorities.authorization_keyring,
            monotonic_root=root,
            mode="test-external",
        )
        start.wait(timeout=10)
        receipt = authority.issue_plan_once(issue=issue, now=NOW)
        output.put(("ok", receipt.model_dump_json(round_trip=True)))
    except BaseException as exc:
        output.put(("error", f"{type(exc).__name__}:{exc}"))


def _intent(authorities: Authorities) -> SourceIntentV2:
    manifest = signed_manifest(authorities)
    request = SourceResourceRequestV2.from_manifest(manifest, requested_calls=1)
    return SourceIntentV2.from_manifest(manifest, resource_request=request)


def _claim(
    authorities: Authorities,
    *,
    attempt_id: UUID = _DEFAULT_ATTEMPT_ID,
    generation: int = 3,
    fence: int = 9,
) -> LabShardClaimV2:
    intent = _intent(authorities)
    payload = StrategyShardPayloadV2.from_source_intent(
        adapter_id=intent.manifest.adapter_id,
        adapter_version=intent.manifest.adapter_version,
        payload_json='{"partition":"2026-08-05"}',
        source_intent=intent,
    )
    definition = LabShardDefinition.from_payload(
        shard_index=2,
        adapter_id=payload.adapter_id,
        adapter_version=payload.adapter_version,
        plan_hash="b" * 64,
        payload_json=payload.model_dump_json(round_trip=True),
    )
    binding = SourceAttemptBindingV2(
        job_id=UUID("11111111-2222-3333-4444-555555555555"),
        spec_hash="a" * 64,
        shard_id=definition.shard_id,
        attempt_id=attempt_id,
        claim_generation=generation,
        scheduler_fencing_token=fence,
        worker_id="lab-worker-a",
    )
    return LabShardClaimV2.from_current_attempt(
        definition=definition,
        attempt_binding=binding,
        claimed_at=NOW,
        lease_expires_at=NOW + timedelta(minutes=5),
    )


def _issue(
    authority: object,
    claim: LabShardClaimV2,
    *,
    operation_id: str = "1" * 64,
    nonce: str = "nonce-1",
) -> CurrentClaimPlanIssueV2:
    payload = claim.strategy_payload
    identity = authority.plan_signer_identity
    plan = SourceUsePlanV2.from_source_intent(
        payload.source_intent,
        issuer=identity.issuer,
        key_id=identity.key_id,
        attempt_binding=claim.attempt_binding,
        adapter_id=claim.definition.adapter_id,
        adapter_version=claim.definition.adapter_version,
        adapter_code_hash=claim.adapter_code_hash,
        payload_hash=claim.definition.payload_hash,
        payload_source_contract_hash=claim.payload_source_contract_hash,
        operation_id=operation_id,
        audience="test-broker",
        not_before=claim.claimed_at,
        expires_at=NOW + timedelta(minutes=3),
        lease_expires_at=claim.lease_expires_at,
        nonce=nonce,
        single_use_authority_id=authority.authority_id,
    )
    return CurrentClaimPlanIssueV2.from_unsigned_plan(plan)


def _authority(
    tmp_path: Path,
    *,
    root: object | None = None,
    signer: object | None = None,
    mode: str = "test-external",
) -> tuple[ModuleType, object, Authorities, Path, object | None]:
    api = _api()
    authorities = create_test_authorities(tmp_path / "keys")
    path = tmp_path / "current-claim.sqlite3"
    selected_root = root
    if mode == "test-external" and selected_root is None:
        selected_root = _external_adapter(api, path=tmp_path / "external-witness.sqlite3")
    elif mode == "test-external" and not isinstance(
        selected_root, api.ExternalCurrentClaimMonotonicRootAdapter
    ):
        selected_root = _external_adapter(
            api,
            path=tmp_path / "external-witness.sqlite3",
            client=selected_root,
        )
    authority = api.PersistentCurrentClaimAuthority(
        path,
        authority_id="global-source-use",
        signer=signer or authorities.plan_v2,
        keyring=authorities.authorization_keyring,
        monotonic_root=selected_root,
        mode=mode,
    )
    return api, authority, authorities, path, selected_root


def test_production_requires_external_root_and_standalone_is_explicit(tmp_path: Path) -> None:
    api = _api()
    authorities = create_test_authorities(tmp_path / "keys")

    with pytest.raises(api.CurrentClaimAuthoritySecurityError, match="requires.*root"):
        api.PersistentCurrentClaimAuthority(
            tmp_path / "authority.sqlite3",
            authority_id="global-source-use",
            signer=authorities.plan_v2,
            keyring=authorities.authorization_keyring,
        )

    _, authority, _, _, _ = _authority(tmp_path / "standalone", mode="test-standalone")
    assert authority.preflight().non_production is True


def test_production_rejects_local_sqlite_materializer_as_antirollback_root(
    tmp_path: Path,
) -> None:
    api = _api()
    authorities = create_test_authorities(tmp_path / "keys")
    local = api.SQLiteCurrentClaimMonotonicRoot(
        tmp_path / "local-root.sqlite3",
        authority_id="current-claim-root",
        store_id="current-claim-root-store-a",
        signer=_RootSigner(),
    )

    with pytest.raises(
        api.CurrentClaimAuthoritySecurityError,
        match="external.*witness|non-production",
    ):
        api.PersistentCurrentClaimAuthority(
            tmp_path / "current-claim.sqlite3",
            authority_id="global-source-use",
            signer=authorities.plan_v2,
            keyring=authorities.authorization_keyring,
            monotonic_root=local,
        )


def test_production_rejects_adapter_subclass_override_bypass(tmp_path: Path) -> None:
    api = _api()
    authorities = create_test_authorities(tmp_path / "keys")
    client = _ExternalWitnessServiceClient(api, tmp_path / "external.sqlite3")
    verifier = _RootSigner()

    class BypassAdapter(api.ExternalCurrentClaimMonotonicRootAdapter):
        pass

    bypass = BypassAdapter.for_nonproduction_test(
        config=api.ExternalCurrentClaimRootConfig(
            adapter_id="rquant-external-monotonic-root-cas-v1",
            transport=client.transport,
            transport_manifest_hash=client.manifest_hash,
            root_authority_id=client.authority_id,
            root_store_id=client.store_id,
            root_issuer=verifier.issuer,
            root_key_id=verifier.key_id,
            root_public_key_fingerprint=verifier.public_key_fingerprint,
            witness_rollback_domain_id=client.rollback_domain_id,
            local_rollback_domain_id="local-current-claim-test-domain",
        ),
        client=client,
        root_verifiers=(verifier,),
    )
    with pytest.raises(api.CurrentClaimAuthoritySecurityError, match="external witness adapter"):
        api.PersistentCurrentClaimAuthority(
            tmp_path / "authority.sqlite3",
            authority_id="global-source-use",
            signer=authorities.plan_v2,
            keyring=authorities.authorization_keyring,
            monotonic_root=bypass,
        )


def test_production_composition_requires_closed_external_witness_adapter(
    tmp_path: Path,
) -> None:
    api = _api()
    authorities = create_test_authorities(tmp_path / "keys")
    external_root = _external_adapter(
        api,
        path=tmp_path / "external-witness-service.sqlite3",
    )
    with pytest.raises(api.CurrentClaimAuthoritySecurityError, match="validated external"):
        api.compose_production_current_claim_authority(
            tmp_path / "current-claim.sqlite3",
            authority_id="global-source-use",
            signer=authorities.plan_v2,
            keyring=authorities.authorization_keyring,
            external_root=external_root,
        )

    materializer = api.SQLiteCurrentClaimMonotonicRoot(
        tmp_path / "local-materializer.sqlite3",
        authority_id="current-claim-root",
        store_id="current-claim-root-store-a",
        signer=_RootSigner(),
    )
    assert materializer.preflight().non_production is True
    assert materializer.preflight().mode == "test-only-materializer"


def test_production_composes_only_through_pinned_unix_peer_transport(tmp_path: Path) -> None:
    api = _api()
    authorities = create_test_authorities(tmp_path / "keys")
    service = _ExternalWitnessServiceClient(api, tmp_path / "root.sqlite3")
    socket_name = f"rq-claim-{os.getpid()}-{canonical_sha256(str(tmp_path))[:8]}.sock"
    socket_path = Path("/tmp") / socket_name

    with _UnixRootServer(socket_path, service):
        root = _production_external_adapter(api, socket_path=socket_path)
        authority = api.compose_production_current_claim_authority(
            tmp_path / "authority.sqlite3",
            authority_id="global-source-use",
            signer=authorities.plan_v2,
            keyring=authorities.authorization_keyring,
            external_root=root,
        )
        claim = _claim(authorities)
        authority.replace_current(claim)
        receipt = authority.issue_plan_once(issue=_issue(authority, claim), now=NOW)
        preflight = authority.preflight()

    assert receipt.signed_plan.signature
    assert preflight.non_production is False
    assert root.production_ready is True


def test_production_adapter_rejects_same_identity_structural_local_client(
    tmp_path: Path,
) -> None:
    api = _api()
    client = _ExternalWitnessServiceClient(api, tmp_path / "local-wrapper.sqlite3")
    with pytest.raises(api.CurrentClaimAuthoritySecurityError, match="closed Unix peer client"):
        api.ExternalCurrentClaimMonotonicRootAdapter(
            config=_external_config(api, client),
            client=client,
            root_verifiers=(_RootSigner(),),
        )


def test_external_adapter_rejects_wrong_config_or_verifier_identity(tmp_path: Path) -> None:
    api = _api()
    client = _ExternalWitnessServiceClient(api, tmp_path / "external.sqlite3")
    verifier = _RootSigner()
    with pytest.raises(api.CurrentClaimAuthoritySecurityError, match="config is invalid"):
        api.ExternalCurrentClaimMonotonicRootAdapter.for_nonproduction_test(
            config=api.ExternalCurrentClaimRootConfig(
                adapter_id="rquant-external-monotonic-root-cas-v1",
                transport=client.transport,
                transport_manifest_hash=client.manifest_hash,
                root_authority_id=client.authority_id,
                root_store_id=client.store_id,
                root_issuer=verifier.issuer,
                root_key_id=verifier.key_id,
                root_public_key_fingerprint="f" * 64,
                witness_rollback_domain_id=client.rollback_domain_id,
                local_rollback_domain_id="local-current-claim-test-domain",
            ),
            client=client,
            root_verifiers=(verifier,),
        )

    with pytest.raises(ValueError, match="independent rollback domain"):
        api.ExternalCurrentClaimRootConfig(
            adapter_id="rquant-external-monotonic-root-cas-v1",
            transport=client.transport,
            transport_manifest_hash=client.manifest_hash,
            root_authority_id=client.authority_id,
            root_store_id=client.store_id,
            root_issuer=verifier.issuer,
            root_key_id=verifier.key_id,
            root_public_key_fingerprint=verifier.public_key_fingerprint,
            witness_rollback_domain_id="same-domain",
            local_rollback_domain_id="same-domain",
        )


def test_authority_persists_external_root_binding_across_restart(tmp_path: Path) -> None:
    api, authority, authorities, path, _ = _authority(tmp_path)
    authority.replace_current(_claim(authorities))
    other_client = _MemoryRoot(tmp_path / "other-external-root", api)
    other_root = _external_adapter(
        api,
        path=tmp_path / "other-external-root",
        client=other_client,
    )

    with pytest.raises(api.CurrentClaimAuthoritySecurityError, match="metadata was tampered"):
        api.PersistentCurrentClaimAuthority(
            path,
            authority_id="global-source-use",
            signer=authorities.plan_v2,
            keyring=authorities.authorization_keyring,
            monotonic_root=other_root,
            mode="test-external",
        )


@pytest.mark.parametrize(
    "mutation",
    ["trailing", "duplicate", "nan", "signature", "identity"],
)
def test_external_adapter_rejects_noncanonical_or_untrusted_receipts(
    tmp_path: Path,
    mutation: str,
) -> None:
    api = _api()
    inner = _ExternalWitnessServiceClient(api, tmp_path / f"external-{mutation}.sqlite3")

    def transform(response: str) -> str:
        if mutation == "trailing":
            return response + "\n"
        if mutation == "duplicate":
            return response[:-1] + ',"role":"current_claim_monotonic_root"}'
        value = json.loads(response)
        if mutation == "nan":
            value["checkpoint"]["operation_count"] = float("nan")
        elif mutation == "signature":
            value["signature"] = "0" * 64
        else:
            value["root_store_id"] = "donor-store"
        return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)

    client = _TransformingExternalClient(inner, transform)
    external = _external_adapter(
        api,
        path=tmp_path / f"external-{mutation}.sqlite3",
        client=client,
    )
    authorities = create_test_authorities(tmp_path / f"keys-{mutation}")
    with pytest.raises(
        api.CurrentClaimAuthoritySecurityError,
        match="malformed|verification failed|identity.*trust binding",
    ):
        api.PersistentCurrentClaimAuthority(
            tmp_path / f"authority-{mutation}.sqlite3",
            authority_id="global-source-use",
            signer=authorities.plan_v2,
            keyring=authorities.authorization_keyring,
            monotonic_root=external,
            mode="test-external",
        )


def test_fresh_challenge_rejects_old_current_receipt_replay(tmp_path: Path) -> None:
    api = _api()
    inner = _ExternalWitnessServiceClient(api, tmp_path / "external.sqlite3")
    replay = _ReplayExternalClient(inner, api)
    external = _external_adapter(api, path=tmp_path / "external.sqlite3", client=replay)
    checkpoint = api.CurrentClaimCheckpoint(
        schema_version=1,
        contract="rquant-current-claim-checkpoint/v1",
        authority_id="global-source-use",
        operation_count=0,
        journal_root="0" * 64,
        state_root="1" * 64,
    )
    external.pin(
        operation_id="a" * 64,
        current_claim_authority_id="global-source-use",
        checkpoint=checkpoint,
    )
    first = external.current(current_claim_authority_id="global-source-use")
    assert first is not None
    replay.replay_kind = "current"

    with pytest.raises(api.CurrentClaimAuthoritySecurityError, match="fresh request challenge"):
        external.current(current_claim_authority_id="global-source-use")


def test_fresh_challenge_rejects_old_idempotent_mutation_receipt(tmp_path: Path) -> None:
    api = _api()
    inner = _ExternalWitnessServiceClient(api, tmp_path / "external.sqlite3")
    replay = _ReplayExternalClient(inner, api)
    external = _external_adapter(api, path=tmp_path / "external.sqlite3", client=replay)
    checkpoint = api.CurrentClaimCheckpoint(
        schema_version=1,
        contract="rquant-current-claim-checkpoint/v1",
        authority_id="global-source-use",
        operation_count=0,
        journal_root="0" * 64,
        state_root="1" * 64,
    )
    external.pin(
        operation_id="a" * 64,
        current_claim_authority_id="global-source-use",
        checkpoint=checkpoint,
    )
    replay.replay_kind = "pin"

    with pytest.raises(api.CurrentClaimAuthoritySecurityError, match="fresh request challenge"):
        external.pin(
            operation_id="a" * 64,
            current_claim_authority_id="global-source-use",
            checkpoint=checkpoint,
        )


def test_real_unix_witness_response_loss_retries_operation_with_fresh_challenge(
    tmp_path: Path,
) -> None:
    api = _api()
    authorities = create_test_authorities(tmp_path / "keys")
    service = _ExternalWitnessServiceClient(api, tmp_path / "root.sqlite3")
    socket_path = Path("/tmp") / f"rq-loss-{os.getpid()}-{canonical_sha256(str(tmp_path))[:8]}.sock"

    with _UnixRootServer(socket_path, service) as server:
        root = _production_external_adapter(api, socket_path=socket_path)
        authority_path = tmp_path / "authority.sqlite3"
        authority = api.compose_production_current_claim_authority(
            authority_path,
            authority_id="global-source-use",
            signer=authorities.plan_v2,
            keyring=authorities.authorization_keyring,
            external_root=root,
        )
        claim = _claim(authorities)
        authority.replace_current(claim)
        issue = _issue(authority, claim)
        server.lose_next_advance_response = True
        with pytest.raises(ConnectionError, match="transport failed closed"):
            authority.issue_plan_once(issue=issue, now=NOW)
        lost_request = server.requests[-1]
        assert lost_request.kind == "advance"

        reopened = api.compose_production_current_claim_authority(
            authority_path,
            authority_id="global-source-use",
            signer=authorities.plan_v2,
            keyring=authorities.authorization_keyring,
            external_root=_production_external_adapter(api, socket_path=socket_path),
        )
        recovered = reopened.issue_plan_once(issue=issue, now=NOW)
        repeated = reopened.issue_plan_once(issue=issue, now=NOW)
        checkpoint0 = api.CurrentClaimCheckpoint(
            schema_version=1,
            contract="rquant-current-claim-checkpoint/v1",
            authority_id="direct-response-loss-subject",
            operation_count=0,
            journal_root="0" * 64,
            state_root="3" * 64,
        )
        pinned = root.pin(
            operation_id="c" * 64,
            current_claim_authority_id="direct-response-loss-subject",
            checkpoint=checkpoint0,
        )
        checkpoint1 = checkpoint0.model_copy(
            update={"operation_count": 1, "journal_root": "4" * 64, "state_root": "5" * 64}
        )
        server.lose_next_advance_response = True
        with pytest.raises(ConnectionError, match="transport failed closed"):
            root.compare_and_advance(
                operation_id="d" * 64,
                current_claim_authority_id="direct-response-loss-subject",
                previous_checkpoint_hash=pinned.checkpoint.checkpoint_hash,
                checkpoint=checkpoint1,
            )
        retried_root = root.compare_and_advance(
            operation_id="d" * 64,
            current_claim_authority_id="direct-response-loss-subject",
            previous_checkpoint_hash=pinned.checkpoint.checkpoint_hash,
            checkpoint=checkpoint1,
        )
        repeated_root = root.compare_and_advance(
            operation_id="d" * 64,
            current_claim_authority_id="direct-response-loss-subject",
            previous_checkpoint_hash=pinned.checkpoint.checkpoint_hash,
            checkpoint=checkpoint1,
        )
        matching = [
            request
            for request in server.requests
            if request.kind == "advance" and request.operation_id == "d" * 64
        ]

    assert recovered == repeated
    assert retried_root.checkpoint == repeated_root.checkpoint
    assert retried_root.operation_id == repeated_root.operation_id
    assert retried_root.signature != repeated_root.signature
    assert len(matching) == 3
    assert len({request.challenge_nonce for request in matching}) == 3
    assert len({request.request_hash for request in matching}) == 3


def test_unix_transport_identity_substitution_fails_closed(tmp_path: Path) -> None:
    api = _api()
    service = _ExternalWitnessServiceClient(api, tmp_path / "root.sqlite3")
    socket_path = Path("/tmp") / f"rq-peer-{os.getpid()}-{canonical_sha256(str(tmp_path))[:8]}.sock"

    with _UnixRootServer(socket_path, service):
        socket_metadata = socket_path.lstat()
        bad_manifest = UnixSocketExternalMonotonicRootManifest(
            role="current_claim_monotonic_root",
            authority_id="current-claim-root",
            store_id="current-claim-root-store-a",
            rollback_domain_id="remote-current-claim-witness-test-domain",
            socket_path=socket_path,
            socket_uid=socket_metadata.st_uid,
            socket_gid=socket_metadata.st_gid,
            peer_uid=os.getuid() + 1,
            peer_gid=os.getgid(),
            connect_timeout_ms=2_000,
            max_response_bytes=2_000_000,
        )
        with pytest.raises(Exception, match="owner|identity|untrusted"):
            UnixSocketExternalMonotonicRootClient(bad_manifest).invoke(
                request_json=_root_json(
                    api.ExternalMonotonicRootRequest.close(
                        kind="current",
                        role="current_claim_monotonic_root",
                        root_authority_id="current-claim-root",
                        root_store_id="current-claim-root-store-a",
                        subject_authority_id="global-source-use",
                        challenge_nonce="9" * 64,
                    )
                )
            )

        assert _production_external_adapter(api, socket_path=socket_path).production_ready


def test_unix_transport_uses_one_total_deadline_for_slow_fragments(tmp_path: Path) -> None:
    api = _api()
    service = _ExternalWitnessServiceClient(api, tmp_path / "root.sqlite3")
    socket_name = f"rq-slow-{os.getpid()}-{canonical_sha256(str(tmp_path))[:8]}.sock"
    socket_path = Path("/tmp") / socket_name

    with _UnixRootServer(socket_path, service) as server:
        root = _production_external_adapter(api, socket_path=socket_path, timeout_ms=70)
        server.response_fragment_delay_seconds = 0.03
        started = time.monotonic()
        with pytest.raises(ConnectionError, match="transport failed closed"):
            root.current(current_claim_authority_id="slow-fragment-test")
        elapsed = time.monotonic() - started

    assert elapsed < 0.25


def test_concrete_root_persists_idempotent_operations_and_recovers_lost_response(
    tmp_path: Path,
) -> None:
    api = _api()
    path = tmp_path / "root.sqlite3"
    root = api.SQLiteCurrentClaimMonotonicRoot(
        path,
        authority_id="current-claim-root",
        store_id="current-claim-root-store-a",
        signer=_RootSigner(),
    )
    checkpoint0 = api.CurrentClaimCheckpoint(
        schema_version=1,
        contract="rquant-current-claim-checkpoint/v1",
        authority_id="global-source-use",
        operation_count=0,
        journal_root="0" * 64,
        state_root="1" * 64,
    )
    pinned = root.pin(
        operation_id="a" * 64,
        current_claim_authority_id="global-source-use",
        checkpoint=checkpoint0,
    )
    checkpoint1 = checkpoint0.model_copy(
        update={"operation_count": 1, "journal_root": "2" * 64, "state_root": "3" * 64}
    )
    committed_but_response_lost = root.compare_and_advance(
        operation_id="b" * 64,
        current_claim_authority_id="global-source-use",
        previous_checkpoint_hash=pinned.checkpoint.checkpoint_hash,
        checkpoint=checkpoint1,
    )

    reopened = api.SQLiteCurrentClaimMonotonicRoot(
        path,
        authority_id="current-claim-root",
        store_id="current-claim-root-store-a",
        signer=_RootSigner(),
    )
    recovered = reopened.compare_and_advance(
        operation_id="b" * 64,
        current_claim_authority_id="global-source-use",
        previous_checkpoint_hash=pinned.checkpoint.checkpoint_hash,
        checkpoint=checkpoint1,
    )
    assert recovered == committed_but_response_lost
    assert reopened.current(current_claim_authority_id="global-source-use") == recovered
    assert reopened.audit_summary().operation_count == 2


def test_claim_authority_recovers_concrete_root_commit_response_loss(
    tmp_path: Path,
) -> None:
    api = _api()
    authorities = create_test_authorities(tmp_path / "keys")
    root_path = tmp_path / "root.sqlite3"
    concrete = api.SQLiteCurrentClaimMonotonicRoot(
        root_path,
        authority_id="current-claim-root",
        store_id="current-claim-root-store-a",
        signer=_RootSigner(),
    )
    lossy = _LoseConcreteRootResponseOnce(concrete)
    external_root = _external_adapter(api, path=root_path, client=lossy)
    path = tmp_path / "current-claim.sqlite3"
    authority = api.PersistentCurrentClaimAuthority(
        path,
        authority_id="global-source-use",
        signer=authorities.plan_v2,
        keyring=authorities.authorization_keyring,
        monotonic_root=external_root,
        mode="test-external",
    )
    claim = _claim(authorities)
    authority.replace_current(claim)
    issue = _issue(authority, claim)
    lossy.lose_once = True
    with pytest.raises(ConnectionError, match="concrete root commit response lost"):
        authority.issue_plan_once(issue=issue, now=NOW)

    reopened_root = api.SQLiteCurrentClaimMonotonicRoot(
        root_path,
        authority_id="current-claim-root",
        store_id="current-claim-root-store-a",
        signer=_RootSigner(),
    )
    reopened_external_root = _external_adapter(
        api,
        path=root_path,
        client=_LoseConcreteRootResponseOnce(reopened_root),
    )
    reopened = api.PersistentCurrentClaimAuthority(
        path,
        authority_id="global-source-use",
        signer=authorities.plan_v2,
        keyring=authorities.authorization_keyring,
        monotonic_root=reopened_external_root,
        mode="test-external",
    )
    recovered = reopened.issue_plan_once(issue=issue, now=NOW)
    assert recovered == reopened.verify_current(binding=issue.binding, now=NOW)
    assert reopened_root.audit_summary().operation_count == 3


def test_concrete_root_rejects_rebind_donor_and_trailing_json(tmp_path: Path) -> None:
    api = _api()
    signer = _RootSigner()
    path = tmp_path / "root.sqlite3"
    root = api.SQLiteCurrentClaimMonotonicRoot(
        path,
        authority_id="current-claim-root",
        store_id="current-claim-root-store-a",
        signer=signer,
    )
    checkpoint = api.CurrentClaimCheckpoint(
        schema_version=1,
        contract="rquant-current-claim-checkpoint/v1",
        authority_id="global-source-use",
        operation_count=0,
        journal_root="0" * 64,
        state_root="1" * 64,
    )
    root.pin(
        operation_id="a" * 64,
        current_claim_authority_id="global-source-use",
        checkpoint=checkpoint,
    )
    with pytest.raises(api.CurrentClaimAuthoritySecurityError, match="rebound"):
        root.pin(
            operation_id="a" * 64,
            current_claim_authority_id="other-authority",
            checkpoint=checkpoint.model_copy(update={"authority_id": "other-authority"}),
        )

    donor = tmp_path / "donor.sqlite3"
    shutil.copy2(path, donor)
    with pytest.raises(api.CurrentClaimAuthoritySecurityError, match="store identity"):
        api.SQLiteCurrentClaimMonotonicRoot(
            donor,
            authority_id="current-claim-root",
            store_id="different-root-store",
            signer=signer,
        )

    with sqlite3.connect(path) as connection:
        connection.execute("UPDATE current_claim_root_state SET root_json = root_json || char(10)")
        connection.commit()
    with pytest.raises(api.CurrentClaimAuthoritySecurityError, match="canonical|malformed"):
        root.audit_summary()


def test_independent_authority_processes_serialize_same_issue(tmp_path: Path) -> None:
    api = _api()
    authorities = create_test_authorities(tmp_path / "keys")
    root_path = tmp_path / "current-claim-root.sqlite3"
    authority_path = tmp_path / "current-claim.sqlite3"
    authority = api.PersistentCurrentClaimAuthority(
        authority_path,
        authority_id="global-source-use",
        signer=authorities.plan_v2,
        keyring=authorities.authorization_keyring,
        monotonic_root=_external_adapter(api, path=root_path),
        mode="test-external",
    )
    claim = _claim(authorities)
    authority.replace_current(claim)
    issue = _issue(authority, claim)
    context = multiprocessing.get_context("fork")
    all_results: list[tuple[str, str]] = []
    for _round in range(3):
        start = context.Event()
        output = context.Queue()
        processes = [
            context.Process(
                target=_multiprocess_issue,
                kwargs={
                    "start": start,
                    "output": output,
                    "authority_path": authority_path,
                    "root_path": root_path,
                    "authorities": authorities,
                    "issue": issue,
                },
            )
            for _ in range(6)
        ]
        for process in processes:
            process.start()
        start.set()
        all_results.extend(output.get(timeout=20) for _ in processes)
        for process in processes:
            process.join(timeout=20)
            assert process.exitcode == 0
    assert {status for status, _payload in all_results} == {"ok"}
    assert len({payload for _status, payload in all_results}) == 1

    reopened = api.PersistentCurrentClaimAuthority(
        authority_path,
        authority_id="global-source-use",
        signer=authorities.plan_v2,
        keyring=authorities.authorization_keyring,
        monotonic_root=_external_adapter(api, path=root_path),
        mode="test-external",
    )
    assert reopened.audit_summary().issue_count == 1


def test_independent_processes_fence_different_operations_for_same_attempt(
    tmp_path: Path,
) -> None:
    api = _api()
    authorities = create_test_authorities(tmp_path / "keys")
    root_path = tmp_path / "current-claim-root.sqlite3"
    authority_path = tmp_path / "current-claim.sqlite3"
    authority = api.PersistentCurrentClaimAuthority(
        authority_path,
        authority_id="global-source-use",
        signer=authorities.plan_v2,
        keyring=authorities.authorization_keyring,
        monotonic_root=_external_adapter(api, path=root_path),
        mode="test-external",
    )
    claim = _claim(authorities)
    authority.replace_current(claim)
    issues = (
        _issue(authority, claim, operation_id="1" * 64, nonce="process-one"),
        _issue(authority, claim, operation_id="2" * 64, nonce="process-two"),
    )
    context = multiprocessing.get_context("fork")
    start = context.Event()
    output = context.Queue()
    processes = [
        context.Process(
            target=_multiprocess_issue,
            kwargs={
                "start": start,
                "output": output,
                "authority_path": authority_path,
                "root_path": root_path,
                "authorities": authorities,
                "issue": issue,
            },
        )
        for issue in issues
    ]
    for process in processes:
        process.start()
    start.set()
    results = [output.get(timeout=20) for _ in processes]
    for process in processes:
        process.join(timeout=20)
        assert process.exitcode == 0
    assert sorted(status for status, _payload in results) == ["error", "ok"]
    assert any("already consumed" in payload for status, payload in results if status == "error")

    reopened = api.PersistentCurrentClaimAuthority(
        authority_path,
        authority_id="global-source-use",
        signer=authorities.plan_v2,
        keyring=authorities.authorization_keyring,
        monotonic_root=_external_adapter(api, path=root_path),
        mode="test-external",
    )
    assert reopened.audit_summary().issue_count == 1


def test_claim_store_rejects_trailing_json(tmp_path: Path) -> None:
    _, authority, authorities, path, _ = _authority(tmp_path)
    authority.replace_current(_claim(authorities))
    with sqlite3.connect(path) as connection:
        connection.execute("UPDATE current_claim_current SET claim_json = claim_json || char(10)")
        connection.commit()
    with pytest.raises(Exception, match="canonical|malformed"):
        authority.audit_summary()


def test_persistent_issue_restarts_and_idempotently_recovers_exact_receipt(tmp_path: Path) -> None:
    api = _api()
    memory_root = _MemoryRoot(tmp_path / "external-root.sqlite3", api)
    api, authority, authorities, path, root = _authority(tmp_path, root=memory_root)
    assert root is not None
    claim = _claim(authorities)
    authority.replace_current(claim)
    issue = _issue(authority, claim)

    memory_root.lose_response_once = True
    with pytest.raises(ConnectionError, match="response lost"):
        authority.issue_plan_once(issue=issue, now=NOW)

    reopened = api.PersistentCurrentClaimAuthority(
        path,
        authority_id="global-source-use",
        signer=authorities.plan_v2,
        keyring=authorities.authorization_keyring,
        monotonic_root=root,
        mode="test-external",
    )
    receipt = reopened.issue_plan_once(issue=issue, now=NOW)
    assert receipt == reopened.issue_plan_once(issue=issue, now=NOW)
    assert receipt.signed_plan.signature
    assert reopened.verify_current(binding=issue.binding, now=NOW) == receipt


def test_attempt_is_consumed_once_and_reclaim_serializes_current_claim(tmp_path: Path) -> None:
    _, authority, authorities, _, _ = _authority(tmp_path)
    claim = _claim(authorities)
    authority.replace_current(claim)
    first = _issue(authority, claim, operation_id="1" * 64)
    authority.issue_plan_once(issue=first, now=NOW)
    other_operation = _issue(authority, claim, operation_id="2" * 64, nonce="nonce-2")
    with pytest.raises(Exception, match="already consumed"):
        authority.issue_plan_once(issue=other_operation, now=NOW)

    stale = _claim(
        authorities,
        attempt_id=UUID("bbbbbbbb-cccc-dddd-eeee-ffffffffffff"),
        generation=2,
        fence=8,
    )
    with pytest.raises(Exception, match="high-water|stale"):
        authority.replace_current(stale)
    replacement = _claim(
        authorities,
        attempt_id=UUID("bbbbbbbb-cccc-dddd-eeee-ffffffffffff"),
        generation=4,
        fence=10,
    )
    authority.replace_current(replacement)
    with pytest.raises(Exception, match="current|high-water"):
        authority.verify_current(binding=first.binding, now=NOW)


def test_concurrent_same_operation_signs_once_and_different_operations_are_fenced(
    tmp_path: Path,
) -> None:
    _, authority, authorities, _, _ = _authority(tmp_path)
    claim = _claim(authorities)
    authority.replace_current(claim)
    issue = _issue(authority, claim)
    receipts: list[object] = []
    for _round in range(3):
        barrier = Barrier(12)

        def same(current_barrier: Barrier = barrier) -> object:
            current_barrier.wait()
            return authority.issue_plan_once(issue=issue, now=NOW)

        with ThreadPoolExecutor(max_workers=12) as pool:
            receipts.extend(pool.map(lambda _: same(), range(12)))
    assert all(receipt == receipts[0] for receipt in receipts)
    assert authority.audit_summary().issue_count == 1

    replacement = _claim(
        authorities,
        attempt_id=UUID("bbbbbbbb-cccc-dddd-eeee-ffffffffffff"),
        generation=4,
        fence=10,
    )
    race_issue = _issue(authority, replacement, operation_id="2" * 64, nonce="race")
    barrier = Barrier(2)

    def replace() -> object:
        barrier.wait()
        return authority.replace_current(replacement)

    def issue_replacement() -> object:
        barrier.wait()
        return authority.issue_plan_once(issue=race_issue, now=NOW)

    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(replace), pool.submit(issue_replacement)]
        outcomes = [
            future.result() if not future.exception() else future.exception() for future in futures
        ]
    assert any(not isinstance(value, Exception) for value in outcomes)
    if isinstance(outcomes[1], Exception):
        assert "current" in str(outcomes[1]) or "high-water" in str(outcomes[1])


def test_signer_failure_leaves_no_receipt_or_high_water_advance(tmp_path: Path) -> None:
    api = _api()
    authorities = create_test_authorities(tmp_path / "keys")
    signer = _FailingSigner(authorities.plan_v2)
    root_client = _MemoryRoot(tmp_path / "external-root.sqlite3", api)
    root = _external_adapter(
        api,
        path=tmp_path / "external-root.sqlite3",
        client=root_client,
    )
    authority = api.PersistentCurrentClaimAuthority(
        tmp_path / "current-claim.sqlite3",
        authority_id="global-source-use",
        signer=signer,
        keyring=authorities.authorization_keyring,
        monotonic_root=root,
        mode="test-external",
    )
    claim = _claim(authorities)
    authority.replace_current(claim)
    before = authority.audit_summary()
    signer.fail = True
    with pytest.raises(RuntimeError, match="signer unavailable"):
        authority.issue_plan_once(issue=_issue(authority, claim), now=NOW)
    after = authority.audit_summary()
    assert after.issue_count == 0
    assert after.operation_count == before.operation_count


def test_external_root_failure_fails_closed_and_recovers_pending_without_resigning(
    tmp_path: Path,
) -> None:
    api = _api()
    memory_root = _MemoryRoot(tmp_path / "external-root.sqlite3", api)
    _, authority, authorities, _, root = _authority(tmp_path, root=memory_root)
    assert root is not None
    claim = _claim(authorities)
    memory_root.fail = True
    with pytest.raises(ConnectionError, match="root unavailable"):
        authority.replace_current(claim)
    memory_root.fail = False
    assert authority.audit_summary().claim_count == 0
    assert authority.replace_current(claim) == claim
    assert authority.audit_summary().claim_count == 1


def test_external_root_unavailable_on_production_open_fails_closed(tmp_path: Path) -> None:
    api = _api()
    authorities = create_test_authorities(tmp_path / "keys")
    client = _MemoryRoot(tmp_path / "remote", api)
    client.fail = True
    external = _external_adapter(api, path=tmp_path / "unused.sqlite3", client=client)
    with pytest.raises(ConnectionError, match="root unavailable"):
        api.PersistentCurrentClaimAuthority(
            tmp_path / "authority.sqlite3",
            authority_id="global-source-use",
            signer=authorities.plan_v2,
            keyring=authorities.authorization_keyring,
            monotonic_root=external,
            mode="test-external",
        )


def test_external_root_behind_local_authority_fails_closed_on_restart(tmp_path: Path) -> None:
    api = _api()
    authorities = create_test_authorities(tmp_path / "keys")
    witness_path = tmp_path / "external-witness.sqlite3"
    authority_path = tmp_path / "authority.sqlite3"
    authority = api.PersistentCurrentClaimAuthority(
        authority_path,
        authority_id="global-source-use",
        signer=authorities.plan_v2,
        keyring=authorities.authorization_keyring,
        monotonic_root=_external_adapter(api, path=witness_path),
        mode="test-external",
    )
    old_witness = tmp_path / "old-witness.sqlite3"
    shutil.copy2(witness_path, old_witness)
    authority.replace_current(_claim(authorities))
    shutil.copy2(old_witness, witness_path)

    with pytest.raises(api.CurrentClaimAuthorityRepairRequiredError) as raised:
        api.PersistentCurrentClaimAuthority(
            authority_path,
            authority_id="global-source-use",
            signer=authorities.plan_v2,
            keyring=authorities.authorization_keyring,
            monotonic_root=_external_adapter(api, path=witness_path),
            mode="test-external",
        )
    assert raised.value.state.reason == "local_state_ahead_of_external_root"


def test_joint_local_rollback_is_rejected_while_external_witness_stays_ahead(
    tmp_path: Path,
) -> None:
    api, authority, authorities, path, root = _authority(tmp_path)
    assert root is not None
    materializer_path = tmp_path / "nonproduction-materializer.sqlite3"
    materializer = api.SQLiteCurrentClaimMonotonicRoot(
        materializer_path,
        authority_id="local-materializer",
        store_id="local-materializer-store",
        signer=_RootSigner(),
    )
    materializer_checkpoint = api.CurrentClaimCheckpoint(
        schema_version=1,
        contract="rquant-current-claim-checkpoint/v1",
        authority_id="local-only",
        operation_count=0,
        journal_root="0" * 64,
        state_root="a" * 64,
    )
    materializer_root = materializer.pin(
        operation_id="a" * 64,
        current_claim_authority_id="local-only",
        checkpoint=materializer_checkpoint,
    )
    first = _claim(authorities)
    authority.replace_current(first)
    authority.issue_plan_once(issue=_issue(authority, first), now=NOW)
    snapshot = tmp_path / "old.sqlite3"
    materializer_snapshot = tmp_path / "old-materializer.sqlite3"
    shutil.copy2(path, snapshot)
    shutil.copy2(materializer_path, materializer_snapshot)
    next_claim = _claim(
        authorities,
        attempt_id=UUID("bbbbbbbb-cccc-dddd-eeee-ffffffffffff"),
        generation=4,
        fence=10,
    )
    authority.replace_current(next_claim)
    authority.issue_plan_once(
        issue=_issue(authority, next_claim, operation_id="2" * 64, nonce="next"), now=NOW
    )
    materializer.compare_and_advance(
        operation_id="b" * 64,
        current_claim_authority_id="local-only",
        previous_checkpoint_hash=materializer_root.checkpoint.checkpoint_hash,
        checkpoint=materializer_checkpoint.model_copy(
            update={
                "operation_count": 1,
                "journal_root": "b" * 64,
                "state_root": "c" * 64,
            }
        ),
    )
    shutil.copy2(snapshot, path)
    shutil.copy2(materializer_snapshot, materializer_path)
    with pytest.raises(api.CurrentClaimAuthorityRepairRequiredError) as raised:
        api.PersistentCurrentClaimAuthority(
            path,
            authority_id="global-source-use",
            signer=authorities.plan_v2,
            keyring=authorities.authorization_keyring,
            monotonic_root=root,
            mode="test-external",
        )
    assert raised.value.state.reason == "external_root_ahead_without_pending_proof"


def test_canonical_receipt_tampering_is_rejected(tmp_path: Path) -> None:
    api = _api()

    _, authority, authorities, path, _ = _authority(tmp_path / "tamper")
    claim = _claim(authorities)
    authority.replace_current(claim)
    authority.issue_plan_once(issue=_issue(authority, claim), now=NOW)
    with sqlite3.connect(path) as connection:
        row = connection.execute(
            "SELECT receipt_json FROM current_claim_issue WHERE operation_id = ?", ("1" * 64,)
        ).fetchone()
        assert row is not None
        data = json.loads(row[0])
        data["signed_plan"]["nonce"] = "tampered"
        connection.execute(
            "UPDATE current_claim_issue SET receipt_json = ? WHERE operation_id = ?",
            (json.dumps(data, ensure_ascii=False, separators=(",", ":"), sort_keys=True), "1" * 64),
        )
        connection.commit()
    with pytest.raises(
        api.CurrentClaimAuthoritySecurityError,
        match="malformed|signature|tampered",
    ):
        authority.audit_summary()


def test_same_identity_donor_database_is_rejected_at_equal_external_high_water(
    tmp_path: Path,
) -> None:
    api, authority, authorities, path, root = _authority(tmp_path)
    assert root is not None
    current = _claim(authorities)
    authority.replace_current(current)
    authority.issue_plan_once(issue=_issue(authority, current), now=NOW)

    donor_path = tmp_path / "donor.sqlite3"
    donor = api.PersistentCurrentClaimAuthority(
        donor_path,
        authority_id="global-source-use",
        signer=authorities.plan_v2,
        keyring=authorities.authorization_keyring,
        monotonic_root=_external_adapter(api, path=tmp_path / "donor-root.sqlite3"),
        mode="test-external",
    )
    donor_claim = _claim(
        authorities,
        attempt_id=UUID("bbbbbbbb-cccc-dddd-eeee-ffffffffffff"),
        generation=4,
        fence=10,
    )
    donor.replace_current(donor_claim)
    donor.issue_plan_once(
        issue=_issue(donor, donor_claim, operation_id="2" * 64, nonce="donor"), now=NOW
    )
    shutil.copy2(donor_path, path)

    with pytest.raises(api.CurrentClaimAuthorityRepairRequiredError) as raised:
        api.PersistentCurrentClaimAuthority(
            path,
            authority_id="global-source-use",
            signer=authorities.plan_v2,
            keyring=authorities.authorization_keyring,
            monotonic_root=root,
            mode="test-external",
        )
    assert raised.value.state.reason == "external_root_diverges_at_same_high_water"


def test_strict_unknown_duplicate_and_wrong_current_generation_are_rejected(tmp_path: Path) -> None:
    _, authority, authorities, path, _ = _authority(tmp_path)
    claim = _claim(authorities)
    authority.replace_current(claim)
    issue = _issue(authority, claim)
    authority.issue_plan_once(issue=issue, now=NOW)
    next_claim = _claim(
        authorities,
        attempt_id=UUID("bbbbbbbb-cccc-dddd-eeee-ffffffffffff"),
        generation=4,
        fence=10,
    )
    authority.replace_current(next_claim)
    with pytest.raises(Exception, match="current|high-water"):
        authority.issue_plan_once(issue=_issue(authority, claim, operation_id="2" * 64), now=NOW)

    with sqlite3.connect(path) as connection:
        row = connection.execute("SELECT claim_json FROM current_claim_current LIMIT 1").fetchone()
        assert row is not None
        connection.execute(
            "UPDATE current_claim_current SET claim_json = ?", (row[0][:-1] + ',"unknown":1}',)
        )
        connection.commit()
    with pytest.raises(Exception, match="canonical|malformed|tampered"):
        authority.audit_summary()


def test_duplicate_persistent_json_keys_are_rejected(tmp_path: Path) -> None:
    _, authority, authorities, path, _ = _authority(tmp_path)
    claim = _claim(authorities)
    authority.replace_current(claim)
    with sqlite3.connect(path) as connection:
        row = connection.execute("SELECT claim_json FROM current_claim_current LIMIT 1").fetchone()
        assert row is not None
        duplicate = row[0][:-1] + ',"claim_generation":3}'
        connection.execute("UPDATE current_claim_current SET claim_json = ?", (duplicate,))
        connection.commit()
    with pytest.raises(Exception, match="malformed|canonical|duplicate"):
        authority.audit_summary()


def test_sqlite_connections_close_after_success_exception_and_concurrency(
    tmp_path: Path,
) -> None:
    fd_root = Path("/dev/fd")
    if not fd_root.exists():
        pytest.skip("file descriptor inspection is unavailable")
    _, authority, authorities, _, _ = _authority(tmp_path)
    current = _claim(authorities)
    authority.replace_current(current)
    stale = _claim(
        authorities,
        attempt_id=UUID("bbbbbbbb-cccc-dddd-eeee-ffffffffffff"),
        generation=2,
        fence=8,
    )
    baseline = len(tuple(fd_root.iterdir()))
    retained: list[BaseException] = []

    for _ in range(20):
        authority.audit_summary()
        try:
            authority.replace_current(stale)
        except BaseException as exc:
            retained.append(exc)

    def retain_concurrent_failure() -> BaseException:
        try:
            authority.replace_current(stale)
        except BaseException as exc:
            return exc
        raise AssertionError("stale replacement unexpectedly succeeded")

    with ThreadPoolExecutor(max_workers=8) as pool:
        retained.extend(pool.map(lambda _index: retain_concurrent_failure(), range(32)))

    assert len(retained) == 52
    assert len(tuple(fd_root.iterdir())) <= baseline + 6


def test_hot_path_uses_incremental_checks_and_full_audit_stays_explicit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    api = _api()
    authorities = create_test_authorities(tmp_path / "keys")
    service = _ExternalWitnessServiceClient(api, tmp_path / "root.sqlite3")
    external = _external_adapter(api, path=tmp_path / "root.sqlite3", client=service)
    authority = api.PersistentCurrentClaimAuthority(
        tmp_path / "authority.sqlite3",
        authority_id="global-source-use",
        signer=authorities.plan_v2,
        keyring=authorities.authorization_keyring,
        monotonic_root=external,
        mode="test-external",
    )
    authority_audits = 0
    root_audits = 0
    original_authority_audit = authority._audit_state
    original_root_audit = service._inner._audit

    def counted_authority_audit(connection: sqlite3.Connection) -> object:
        nonlocal authority_audits
        authority_audits += 1
        return original_authority_audit(connection)

    def counted_root_audit(connection: sqlite3.Connection) -> object:
        nonlocal root_audits
        root_audits += 1
        return original_root_audit(connection)

    monkeypatch.setattr(authority, "_audit_state", counted_authority_audit)
    monkeypatch.setattr(service._inner, "_audit", counted_root_audit)
    started = time.monotonic()
    for index in range(1, 41):
        claim = _claim(
            authorities,
            attempt_id=UUID(int=index),
            generation=index + 2,
            fence=index + 8,
        )
        authority.replace_current(claim)
        authority.issue_plan_once(
            issue=_issue(
                authority,
                claim,
                operation_id=f"{index:064x}",
                nonce=f"scale-{index}",
            ),
            now=NOW,
        )
    hot_elapsed = time.monotonic() - started

    assert authority_audits == 0
    assert root_audits == 0
    # The subject of this case is the two audit counters above: the hot path
    # must not audit. This bound only has to catch an algorithmic regression -
    # a quadratic hot path would be orders of magnitude slower, not 20% - and a
    # five-second wall clock on a shared runner reports load as a defect (6.1s
    # observed on x64 CI while the audit counters were still correct).
    assert hot_elapsed < 20.0

    authority.preflight()
    assert authority_audits == 1
    authority.audit_summary()
    assert authority_audits == 2
    service._inner.preflight()
    assert root_audits == 1
