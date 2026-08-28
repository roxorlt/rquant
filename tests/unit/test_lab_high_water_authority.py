"""Contract tests for the independent monotonic Lab high-water authority.

Every test runs the real server (real socket, real dirfd store) — no fakes.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import os
import socket
import threading
import uuid
from collections.abc import Iterator
from pathlib import Path

import pytest

from rquant.lab_high_water_authority import (
    HIGH_WATER_GENESIS_HASH,
    LabHighWaterAuthorityClient,
    LabHighWaterAuthorityClientConfig,
    LabHighWaterAuthorityError,
    LabHighWaterAuthorityServer,
    LabHighWaterAuthorityServerConfig,
    LabHighWaterIdentityError,
    LabHighWaterKey,
    LabHighWaterRollbackError,
)
from rquant.strict_json import canonical_json_bytes

CLIENT_KEY = LabHighWaterKey(key_id="lab-client-1", secret=b"c" * 32)
CLIENT_KEY_2 = LabHighWaterKey(key_id="lab-client-2", secret=b"d" * 32)
AUTHORITY_KEY = LabHighWaterKey(key_id="authority-1", secret=b"a" * 32)
STABLE_IDENTITY = "sqlite:lab-jobs:test"
CODE_IDENTITY = "1" * 40
PROFILE_IDENTITY = "2" * 64
HEAD_HASH_A = "3" * 64
HEAD_HASH_B = "4" * 64
RECEIPT_HASH_A = "5" * 64
RECEIPT_HASH_B = "6" * 64


def _server_config(root: Path, **overrides: object) -> LabHighWaterAuthorityServerConfig:
    values: dict[str, object] = {
        "root": root / "authority-state",
        "socket_path": root / "authority.sock",
        "database_stable_identity": STABLE_IDENTITY,
        "signing_key_provider": lambda: AUTHORITY_KEY,
        "trusted_client_key_provider": lambda key_id: {
            CLIENT_KEY.key_id: CLIENT_KEY,
            CLIENT_KEY_2.key_id: CLIENT_KEY_2,
        }.get(key_id),
    }
    values.update(overrides)
    return LabHighWaterAuthorityServerConfig(**values)  # type: ignore[arg-type]


def _client_config(root: Path, **overrides: object) -> LabHighWaterAuthorityClientConfig:
    values: dict[str, object] = {
        "socket_path": root / "authority.sock",
        "database_stable_identity": STABLE_IDENTITY,
        "code_identity": CODE_IDENTITY,
        "profile_identity": PROFILE_IDENTITY,
        "signing_key_provider": lambda: CLIENT_KEY,
        "trusted_authority_key_provider": lambda key_id: (
            AUTHORITY_KEY if key_id == AUTHORITY_KEY.key_id else None
        ),
        "timeout_seconds": 5.0,
    }
    values.update(overrides)
    return LabHighWaterAuthorityClientConfig(**values)  # type: ignore[arg-type]


@pytest.fixture
def authority_root(tmp_path: Path) -> Path:
    return tmp_path


@pytest.fixture
def running_server(authority_root: Path) -> Iterator[LabHighWaterAuthorityServer]:
    server = LabHighWaterAuthorityServer(_server_config(authority_root))
    server.bind()
    stop = threading.Event()
    thread = threading.Thread(target=server.serve_forever, kwargs={"stop": stop}, daemon=True)
    thread.start()
    try:
        yield server
    finally:
        stop.set()
        server.close()
        thread.join(timeout=10)
        assert not thread.is_alive()


def _observe(
    client: LabHighWaterAuthorityClient,
    *,
    mutation_epoch: int,
    chain_generation: int,
    chain_head_hash: str = HEAD_HASH_A,
    graph_receipt_hash: str = RECEIPT_HASH_A,
    database_generation: tuple[int, int] = (7, 11),
    schema_generation: int = 5,
    graph_receipt_kind: str = "incremental",
) -> None:
    client.observe(
        database_generation=database_generation,
        schema_generation=schema_generation,
        mutation_epoch=mutation_epoch,
        chain_generation=chain_generation,
        chain_head_hash=chain_head_hash,
        graph_receipt_kind=graph_receipt_kind,  # type: ignore[arg-type]
        graph_receipt_hash=graph_receipt_hash,
    )


class TestAdvanceAndStatus:
    def test_genesis_advance_persists_head(
        self, running_server: LabHighWaterAuthorityServer, authority_root: Path
    ) -> None:
        client = LabHighWaterAuthorityClient(_client_config(authority_root))
        _observe(client, mutation_epoch=3, chain_generation=1)
        head = client.status()
        assert head is not None
        assert head.sequence == 0
        assert head.mutation_epoch == 3
        assert head.chain_generation == 1
        assert head.chain_head_hash == HEAD_HASH_A
        assert head.graph_receipt_hash == RECEIPT_HASH_A
        assert head.previous_record_hash == HIGH_WATER_GENESIS_HASH
        assert head.client_key_id == CLIENT_KEY.key_id
        assert head.authority_key_id == AUTHORITY_KEY.key_id

    def test_repeat_observation_is_idempotent(
        self, running_server: LabHighWaterAuthorityServer, authority_root: Path
    ) -> None:
        client = LabHighWaterAuthorityClient(_client_config(authority_root))
        _observe(client, mutation_epoch=3, chain_generation=1)
        _observe(client, mutation_epoch=3, chain_generation=1)
        head = client.status()
        assert head is not None
        assert head.sequence == 0

    def test_advance_grows_sequence_monotonically(
        self, running_server: LabHighWaterAuthorityServer, authority_root: Path
    ) -> None:
        client = LabHighWaterAuthorityClient(_client_config(authority_root))
        _observe(client, mutation_epoch=3, chain_generation=1)
        _observe(
            client,
            mutation_epoch=4,
            chain_generation=2,
            chain_head_hash=HEAD_HASH_B,
            graph_receipt_hash=RECEIPT_HASH_B,
        )
        head = client.status()
        assert head is not None
        assert head.sequence == 1
        assert head.chain_generation == 2
        assert head.chain_head_hash == HEAD_HASH_B

    def test_status_on_empty_authority_is_none(
        self, running_server: LabHighWaterAuthorityServer, authority_root: Path
    ) -> None:
        client = LabHighWaterAuthorityClient(_client_config(authority_root))
        assert client.status() is None


class TestMonotonicity:
    def test_chain_generation_rollback_refused(
        self, running_server: LabHighWaterAuthorityServer, authority_root: Path
    ) -> None:
        client = LabHighWaterAuthorityClient(_client_config(authority_root))
        _observe(client, mutation_epoch=5, chain_generation=3)
        with pytest.raises(LabHighWaterRollbackError):
            _observe(client, mutation_epoch=4, chain_generation=2)
        head = client.status()
        assert head is not None
        assert head.chain_generation == 3

    def test_mutation_epoch_rollback_refused(
        self, running_server: LabHighWaterAuthorityServer, authority_root: Path
    ) -> None:
        client = LabHighWaterAuthorityClient(_client_config(authority_root))
        _observe(client, mutation_epoch=5, chain_generation=3)
        with pytest.raises(LabHighWaterRollbackError):
            _observe(client, mutation_epoch=4, chain_generation=4, chain_head_hash=HEAD_HASH_B)

    def test_in_place_chain_rewrite_refused(
        self, running_server: LabHighWaterAuthorityServer, authority_root: Path
    ) -> None:
        client = LabHighWaterAuthorityClient(_client_config(authority_root))
        _observe(client, mutation_epoch=5, chain_generation=3)
        with pytest.raises(LabHighWaterAuthorityError):
            _observe(client, mutation_epoch=5, chain_generation=3, chain_head_hash=HEAD_HASH_B)

    def test_fresh_process_cannot_reduce_high_water(
        self, running_server: LabHighWaterAuthorityServer, authority_root: Path
    ) -> None:
        """A restarted Lab process with a rolled-back local DB fails closed."""

        client = LabHighWaterAuthorityClient(_client_config(authority_root))
        _observe(client, mutation_epoch=10, chain_generation=8)
        rolled_back = LabHighWaterAuthorityClient(_client_config(authority_root))
        with pytest.raises(LabHighWaterRollbackError):
            _observe(rolled_back, mutation_epoch=5, chain_generation=4)


class TestIdentityBinding:
    def test_database_generation_change_refused(
        self, running_server: LabHighWaterAuthorityServer, authority_root: Path
    ) -> None:
        client = LabHighWaterAuthorityClient(_client_config(authority_root))
        _observe(client, mutation_epoch=3, chain_generation=1, database_generation=(7, 11))
        with pytest.raises(LabHighWaterIdentityError):
            _observe(client, mutation_epoch=4, chain_generation=2, database_generation=(8, 11))

    def test_stable_identity_mismatch_refused(
        self, running_server: LabHighWaterAuthorityServer, authority_root: Path
    ) -> None:
        client = LabHighWaterAuthorityClient(
            _client_config(authority_root, database_stable_identity="sqlite:other")
        )
        with pytest.raises(LabHighWaterAuthorityError):
            _observe(client, mutation_epoch=3, chain_generation=1)

    def test_code_identity_rotation_refused_by_default(
        self, running_server: LabHighWaterAuthorityServer, authority_root: Path
    ) -> None:
        client = LabHighWaterAuthorityClient(_client_config(authority_root))
        _observe(client, mutation_epoch=3, chain_generation=1)
        rotated = LabHighWaterAuthorityClient(
            _client_config(authority_root, code_identity="f" * 40)
        )
        with pytest.raises(LabHighWaterAuthorityError):
            _observe(rotated, mutation_epoch=4, chain_generation=2, chain_head_hash=HEAD_HASH_B)

    def test_identity_rotation_allowed_when_configured(self, tmp_path: Path) -> None:
        server = LabHighWaterAuthorityServer(_server_config(tmp_path, allow_identity_rotation=True))
        server.bind()
        stop = threading.Event()
        thread = threading.Thread(target=server.serve_forever, kwargs={"stop": stop}, daemon=True)
        thread.start()
        try:
            client = LabHighWaterAuthorityClient(_client_config(tmp_path))
            _observe(client, mutation_epoch=3, chain_generation=1)
            rotated = LabHighWaterAuthorityClient(_client_config(tmp_path, code_identity="f" * 40))
            _observe(
                rotated,
                mutation_epoch=4,
                chain_generation=2,
                chain_head_hash=HEAD_HASH_B,
                graph_receipt_hash=RECEIPT_HASH_B,
            )
            head = rotated.status()
            assert head is not None
            assert head.code_identity == "f" * 40
        finally:
            stop.set()
            server.close()
            thread.join(timeout=10)


class TestKeyAuthority:
    def test_untrusted_client_key_refused(
        self, running_server: LabHighWaterAuthorityServer, authority_root: Path
    ) -> None:
        rogue = LabHighWaterKey(key_id="rogue-1", secret=b"r" * 32)
        client = LabHighWaterAuthorityClient(
            _client_config(authority_root, signing_key_provider=lambda: rogue)
        )
        with pytest.raises(LabHighWaterAuthorityError):
            _observe(client, mutation_epoch=3, chain_generation=1)

    def test_client_key_rotation_accepted(
        self, running_server: LabHighWaterAuthorityServer, authority_root: Path
    ) -> None:
        first = LabHighWaterAuthorityClient(_client_config(authority_root))
        _observe(first, mutation_epoch=3, chain_generation=1)
        rotated = LabHighWaterAuthorityClient(
            _client_config(authority_root, signing_key_provider=lambda: CLIENT_KEY_2)
        )
        _observe(
            rotated,
            mutation_epoch=4,
            chain_generation=2,
            chain_head_hash=HEAD_HASH_B,
            graph_receipt_hash=RECEIPT_HASH_B,
        )
        head = rotated.status()
        assert head is not None
        assert head.client_key_id == CLIENT_KEY_2.key_id

    def test_client_rejects_untrusted_authority_signature(self, authority_root: Path) -> None:
        other_authority = LabHighWaterKey(key_id="authority-2", secret=b"z" * 32)
        server = LabHighWaterAuthorityServer(
            _server_config(authority_root, signing_key_provider=lambda: other_authority)
        )
        server.bind()
        stop = threading.Event()
        thread = threading.Thread(target=server.serve_forever, kwargs={"stop": stop}, daemon=True)
        thread.start()
        try:
            client = LabHighWaterAuthorityClient(_client_config(authority_root))
            with pytest.raises(LabHighWaterAuthorityError):
                _observe(client, mutation_epoch=3, chain_generation=1)
        finally:
            stop.set()
            server.close()
            thread.join(timeout=10)


def _signed_request(payload: dict[str, object], key: LabHighWaterKey) -> bytes:
    body = dict(payload)
    body["key_id"] = key.key_id
    request_hash = hashlib.sha256(canonical_json_bytes(body)).hexdigest()
    signature = hmac.new(key.secret, request_hash.encode("ascii"), hashlib.sha256).hexdigest()
    body["request_hash"] = request_hash
    body["signature"] = signature
    return canonical_json_bytes(body) + b"\n"


def _raw_exchange(socket_path: Path, request_line: bytes) -> dict[str, object]:
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as sock:
        sock.settimeout(5.0)
        sock.connect(str(socket_path))
        sock.sendall(request_line)
        sock.shutdown(socket.SHUT_WR)
        chunks: list[bytes] = []
        while True:
            block = sock.recv(65_536)
            if not block:
                break
            chunks.append(block)
    return json.loads(b"".join(chunks))


def _advance_payload(
    *,
    request_id: str,
    mutation_epoch: int,
    chain_generation: int,
    expected_sequence: int,
    expected_record_hash: str,
    chain_head_hash: str = HEAD_HASH_A,
) -> dict[str, object]:
    return {
        "schema_version": 1,
        "kind": "advance",
        "request_id": request_id,
        "database_stable_identity": STABLE_IDENTITY,
        "database_generation": [7, 11],
        "schema_generation": 5,
        "mutation_epoch": mutation_epoch,
        "chain_generation": chain_generation,
        "chain_head_hash": chain_head_hash,
        "graph_receipt_kind": "incremental",
        "graph_receipt_hash": RECEIPT_HASH_A,
        "code_identity": CODE_IDENTITY,
        "profile_identity": PROFILE_IDENTITY,
        "expected_sequence": expected_sequence,
        "expected_record_hash": expected_record_hash,
    }


class TestCompareAndAdvance:
    def test_stale_expected_head_is_refused(
        self, running_server: LabHighWaterAuthorityServer, authority_root: Path
    ) -> None:
        client = LabHighWaterAuthorityClient(_client_config(authority_root))
        _observe(client, mutation_epoch=3, chain_generation=1)
        stale = _advance_payload(
            request_id=uuid.uuid4().hex,
            mutation_epoch=9,
            chain_generation=9,
            expected_sequence=-1,
            expected_record_hash=HIGH_WATER_GENESIS_HASH,
            chain_head_hash=HEAD_HASH_B,
        )
        response = _raw_exchange(
            running_server.config.socket_path, _signed_request(stale, CLIENT_KEY)
        )
        assert response["outcome"] == "refused"
        head = client.status()
        assert head is not None
        assert head.chain_generation == 1

    def test_duplicate_request_is_idempotent(
        self, running_server: LabHighWaterAuthorityServer, authority_root: Path
    ) -> None:
        request_id = uuid.uuid4().hex
        payload = _advance_payload(
            request_id=request_id,
            mutation_epoch=3,
            chain_generation=1,
            expected_sequence=-1,
            expected_record_hash=HIGH_WATER_GENESIS_HASH,
        )
        line = _signed_request(payload, CLIENT_KEY)
        first = _raw_exchange(running_server.config.socket_path, line)
        second = _raw_exchange(running_server.config.socket_path, line)
        assert first["outcome"] == "advanced"
        assert second["outcome"] in {"advanced", "unchanged"}
        client = LabHighWaterAuthorityClient(_client_config(authority_root))
        head = client.status()
        assert head is not None
        assert head.sequence == 0

    def test_request_id_reuse_with_different_content_refused(
        self, running_server: LabHighWaterAuthorityServer, authority_root: Path
    ) -> None:
        request_id = uuid.uuid4().hex
        first_payload = _advance_payload(
            request_id=request_id,
            mutation_epoch=3,
            chain_generation=1,
            expected_sequence=-1,
            expected_record_hash=HIGH_WATER_GENESIS_HASH,
        )
        first = _raw_exchange(
            running_server.config.socket_path, _signed_request(first_payload, CLIENT_KEY)
        )
        assert first["outcome"] == "advanced"
        head_hash = first["state"]["record_hash"]  # type: ignore[index]
        replayed = _advance_payload(
            request_id=request_id,
            mutation_epoch=4,
            chain_generation=2,
            expected_sequence=0,
            expected_record_hash=head_hash,
            chain_head_hash=HEAD_HASH_B,
        )
        response = _raw_exchange(
            running_server.config.socket_path, _signed_request(replayed, CLIENT_KEY)
        )
        assert response["outcome"] == "refused"
        client = LabHighWaterAuthorityClient(_client_config(authority_root))
        head = client.status()
        assert head is not None
        assert head.sequence == 0

    def test_tampered_signature_refused(
        self, running_server: LabHighWaterAuthorityServer, authority_root: Path
    ) -> None:
        payload = _advance_payload(
            request_id=uuid.uuid4().hex,
            mutation_epoch=3,
            chain_generation=1,
            expected_sequence=-1,
            expected_record_hash=HIGH_WATER_GENESIS_HASH,
        )
        line = _signed_request(payload, CLIENT_KEY)
        document = json.loads(line)
        document["signature"] = "0" * 64
        response = _raw_exchange(
            running_server.config.socket_path, canonical_json_bytes(document) + b"\n"
        )
        assert response["outcome"] == "refused"
        client = LabHighWaterAuthorityClient(_client_config(authority_root))
        assert client.status() is None


class TestDurability:
    def test_state_survives_restart_and_still_blocks_rollback(self, tmp_path: Path) -> None:
        config = _server_config(tmp_path)
        server = LabHighWaterAuthorityServer(config)
        server.bind()
        stop = threading.Event()
        thread = threading.Thread(target=server.serve_forever, kwargs={"stop": stop}, daemon=True)
        thread.start()
        client = LabHighWaterAuthorityClient(_client_config(tmp_path))
        try:
            _observe(client, mutation_epoch=10, chain_generation=8)
        finally:
            stop.set()
            server.close()
            thread.join(timeout=10)
        restarted = LabHighWaterAuthorityServer(_server_config(tmp_path))
        restarted.bind()
        stop2 = threading.Event()
        thread2 = threading.Thread(
            target=restarted.serve_forever, kwargs={"stop": stop2}, daemon=True
        )
        thread2.start()
        try:
            head = client.status()
            assert head is not None
            assert head.mutation_epoch == 10
            with pytest.raises(LabHighWaterRollbackError):
                _observe(client, mutation_epoch=5, chain_generation=4)
        finally:
            stop2.set()
            restarted.close()
            thread2.join(timeout=10)

    def test_unreachable_authority_fails_closed(self, tmp_path: Path) -> None:
        client = LabHighWaterAuthorityClient(_client_config(tmp_path, timeout_seconds=0.5))
        with pytest.raises(LabHighWaterAuthorityError):
            _observe(client, mutation_epoch=1, chain_generation=1)
        with pytest.raises(LabHighWaterAuthorityError):
            client.status()


class TestStorePathHardening:
    def test_symlinked_state_root_refused(self, tmp_path: Path) -> None:
        real_root = tmp_path / "real-root"
        real_root.mkdir(mode=0o700)
        linked = tmp_path / "authority-state"
        linked.symlink_to(real_root)
        config = _server_config(tmp_path)
        server = LabHighWaterAuthorityServer(config)
        with pytest.raises(LabHighWaterAuthorityError):
            server.bind()

    def test_symlinked_chain_log_refused(self, tmp_path: Path) -> None:
        config = _server_config(tmp_path)
        server = LabHighWaterAuthorityServer(config)
        server.bind()
        stop = threading.Event()
        thread = threading.Thread(target=server.serve_forever, kwargs={"stop": stop}, daemon=True)
        thread.start()
        client = LabHighWaterAuthorityClient(_client_config(tmp_path))
        try:
            _observe(client, mutation_epoch=3, chain_generation=1)
        finally:
            stop.set()
            server.close()
            thread.join(timeout=10)
        state_root = tmp_path / "authority-state"
        chain = state_root / "chain.jsonl"
        moved = tmp_path / "chain-moved.jsonl"
        os.rename(chain, moved)
        chain.symlink_to(moved)
        restarted = LabHighWaterAuthorityServer(_server_config(tmp_path))
        with pytest.raises(LabHighWaterAuthorityError):
            restarted.bind()

    def test_world_writable_state_root_refused(self, tmp_path: Path) -> None:
        config = _server_config(tmp_path)
        state_root = tmp_path / "authority-state"
        state_root.mkdir(mode=0o700)
        state_root.chmod(0o770)
        server = LabHighWaterAuthorityServer(config)
        with pytest.raises(LabHighWaterAuthorityError):
            server.bind()
