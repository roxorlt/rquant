from __future__ import annotations

import ast
import hashlib
import json
import multiprocessing
import os
import shutil
import sqlite3
import subprocess
from pathlib import Path
from typing import Any

import pytest

import rquant.replay_lineage_authority as replay_lineage_api
from rquant.adapter_manifest import (
    REPLAY_CLAIM_NAMESPACE,
    Ed25519ContractSigner,
    Ed25519PublicKeyRecord,
    VerifyOnlyEd25519Keyring,
)
from rquant.runtime_contracts import canonical_sha256
from rquant.source_broker import (
    ReplayLineageAuthorityProtocol,
    ReplayLineageCheckpointReceipt,
)
from tests.unit.test_adapter_manifest import OpenSslSigningClient

# Watchdogs for four spawned competitors, not subjects: what the case they
# guard asserts is that exactly one of them commits the operation. Each is a
# fresh CPython importing the rquant surface, measured at 1.5s on an *idle*
# ubuntu-24.04 x64 CI runner against 0.5s on the development machine (CI
# diagnostic job on 017d808), and four of them start at once on four vCPUs
# while the parent is still running. Ten and fifteen seconds were a fast
# machine's numbers and the runner missed them.
_COMPETITOR_WATCHDOG_SECONDS = 120


class _SQLiteMonotonicCheckpointStore:
    def __init__(
        self,
        path: Path,
        *,
        authority_id: str = "independent-anti-rollback-root",
        lose_compare_response_once: bool = False,
    ) -> None:
        self._path = path.resolve()
        self._authority_id = authority_id
        self._lose_compare_response_once = lose_compare_response_once
        with sqlite3.connect(self._path) as connection:
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS root_state (
                    lineage_authority_id TEXT PRIMARY KEY,
                    root_json TEXT NOT NULL
                )
                """
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS root_operation (
                    operation_id TEXT PRIMARY KEY,
                    request_hash TEXT NOT NULL,
                    root_json TEXT NOT NULL
                )
                """
            )

    @property
    def authority_id(self) -> str:
        return self._authority_id

    @property
    def storage_path(self) -> Path:
        return self._path

    @property
    def verifier_fingerprints(self) -> frozenset[str]:
        return frozenset({_digest(f"root-fingerprint:{self._authority_id}")})

    def pin(
        self,
        *,
        operation_id: str,
        lineage_authority_id: str,
        checkpoint: replay_lineage_api.SignedReplayLineageCheckpoint,
    ) -> replay_lineage_api.AntiRollbackRoot:
        return self._write(
            operation_id=operation_id,
            lineage_authority_id=lineage_authority_id,
            previous_checkpoint_hash="0" * 64,
            checkpoint=checkpoint,
            pin=True,
        )

    def current(self, *, lineage_authority_id: str) -> replay_lineage_api.AntiRollbackRoot | None:
        with sqlite3.connect(self._path) as connection:
            row = connection.execute(
                "SELECT root_json FROM root_state WHERE lineage_authority_id = ?",
                (lineage_authority_id,),
            ).fetchone()
        if row is None:
            return None
        return replay_lineage_api.AntiRollbackRoot.model_validate_json(row[0])

    def compare_and_advance(
        self,
        *,
        operation_id: str,
        lineage_authority_id: str,
        previous_checkpoint_hash: str,
        checkpoint: replay_lineage_api.SignedReplayLineageCheckpoint,
    ) -> replay_lineage_api.AntiRollbackRoot:
        root = self._write(
            operation_id=operation_id,
            lineage_authority_id=lineage_authority_id,
            previous_checkpoint_hash=previous_checkpoint_hash,
            checkpoint=checkpoint,
            pin=False,
        )
        if self._lose_compare_response_once:
            self._lose_compare_response_once = False
            raise ConnectionError("simulated root commit-response loss")
        return root

    def _write(
        self,
        *,
        operation_id: str,
        lineage_authority_id: str,
        previous_checkpoint_hash: str,
        checkpoint: replay_lineage_api.SignedReplayLineageCheckpoint,
        pin: bool,
    ) -> replay_lineage_api.AntiRollbackRoot:
        request_hash = canonical_sha256(
            {
                "operation_id": operation_id,
                "lineage_authority_id": lineage_authority_id,
                "previous_checkpoint_hash": previous_checkpoint_hash,
                "checkpoint": checkpoint,
                "pin": pin,
            }
        )
        with sqlite3.connect(self._path, isolation_level=None) as connection:
            connection.execute("BEGIN IMMEDIATE")
            existing = connection.execute(
                "SELECT request_hash, root_json FROM root_operation WHERE operation_id = ?",
                (operation_id,),
            ).fetchone()
            if existing is not None:
                if existing[0] != request_hash:
                    raise RuntimeError("root operation_id payload rebind")
                connection.commit()
                return replay_lineage_api.AntiRollbackRoot.model_validate_json(existing[1])
            current_row = connection.execute(
                "SELECT root_json FROM root_state WHERE lineage_authority_id = ?",
                (lineage_authority_id,),
            ).fetchone()
            if pin:
                if current_row is not None:
                    raise RuntimeError("root lineage already pinned")
            else:
                if current_row is None:
                    raise RuntimeError("root lineage is not pinned")
                current = replay_lineage_api.AntiRollbackRoot.model_validate_json(current_row[0])
                if current.checkpoint.checkpoint_hash != previous_checkpoint_hash:
                    raise RuntimeError("root compare-and-advance mismatch")
                if checkpoint.operation_count != current.checkpoint.operation_count + 1:
                    raise RuntimeError("root checkpoint sequence must advance by one")
            root = replay_lineage_api.AntiRollbackRoot(
                schema_version=1,
                contract="rquant-replay-lineage-anti-rollback-root/v1",
                root_authority_id=self._authority_id,
                lineage_authority_id=lineage_authority_id,
                operation_id=operation_id,
                previous_checkpoint_hash=previous_checkpoint_hash,
                checkpoint=checkpoint,
            )
            root_json = root.model_dump_json()
            connection.execute(
                "INSERT INTO root_operation(operation_id, request_hash, root_json) "
                "VALUES (?, ?, ?)",
                (operation_id, request_hash, root_json),
            )
            connection.execute(
                "INSERT INTO root_state(lineage_authority_id, root_json) VALUES (?, ?) "
                "ON CONFLICT(lineage_authority_id) DO UPDATE SET root_json = excluded.root_json",
                (lineage_authority_id, root_json),
            )
            connection.commit()
        return root


def test_replay_lineage_authority_exposes_a_specific_security_error() -> None:
    assert issubclass(
        replay_lineage_api.ReplayLineageAuthoritySecurityError,
        RuntimeError,
    )


def _create_sqlite(path: Path) -> None:
    with sqlite3.connect(path) as connection:
        connection.execute("CREATE TABLE marker (value TEXT NOT NULL)")


def _trust_root(
    root: Path,
    *,
    authority_id: str = "persistent-lineage-root",
) -> tuple[Ed25519ContractSigner, VerifyOnlyEd25519Keyring]:
    openssl = shutil.which("openssl")
    assert openssl is not None
    root.mkdir()
    private_key = root / "lineage.private.pem"
    public_key = root / "lineage.public.pem"
    subprocess.run(
        (openssl, "genpkey", "-algorithm", "ED25519", "-out", str(private_key)),
        check=True,
        capture_output=True,
    )
    subprocess.run(
        (openssl, "pkey", "-in", str(private_key), "-pubout", "-out", str(public_key)),
        check=True,
        capture_output=True,
    )
    private_key.chmod(0o600)
    record = Ed25519PublicKeyRecord(
        key_id="lineage-v1",
        issuer=authority_id,
        key_purpose="replay_claim",
        rotation="active",
        public_key_pem=public_key.read_bytes(),
    )
    signer = Ed25519ContractSigner(
        key_id=record.key_id,
        issuer=authority_id,
        key_purpose="replay_claim",
        client=OpenSslSigningClient(
            private_key,
            key_purpose="replay_claim",
            allowed_namespaces=frozenset({REPLAY_CLAIM_NAMESPACE}),
            public_key_fingerprint=record.public_key_fingerprint,
        ),
    )
    keyring = VerifyOnlyEd25519Keyring(
        records=(record,),
        issuer_allowlist={"replay_claim": frozenset({authority_id})},
        rotation_allowlist={(authority_id, "replay_claim"): frozenset({record.key_id})},
    )
    return signer, keyring


def _open_trust_root(
    root: Path,
    *,
    authority_id: str = "persistent-lineage-root",
) -> tuple[Ed25519ContractSigner, VerifyOnlyEd25519Keyring]:
    record = Ed25519PublicKeyRecord(
        key_id="lineage-v1",
        issuer=authority_id,
        key_purpose="replay_claim",
        rotation="active",
        public_key_pem=(root / "lineage.public.pem").read_bytes(),
    )
    signer = Ed25519ContractSigner(
        key_id=record.key_id,
        issuer=authority_id,
        key_purpose="replay_claim",
        client=OpenSslSigningClient(
            root / "lineage.private.pem",
            key_purpose="replay_claim",
            allowed_namespaces=frozenset({REPLAY_CLAIM_NAMESPACE}),
            public_key_fingerprint=record.public_key_fingerprint,
        ),
    )
    keyring = VerifyOnlyEd25519Keyring(
        records=(record,),
        issuer_allowlist={"replay_claim": frozenset({authority_id})},
        rotation_allowlist={(authority_id, "replay_claim"): frozenset({record.key_id})},
    )
    return signer, keyring


def _competing_advance_process(
    authority_path: str,
    broker_path: str,
    replay_path: str,
    key_path: str,
    ready: Any,
    gate: Any,
    output: Any,
) -> None:
    try:
        signer, keyring = _open_trust_root(Path(key_path))
        authority = replay_lineage_api.PersistentReplayLineageAuthority(
            Path(authority_path),
            authority_id="persistent-lineage-root",
            signer=signer,
            keyring=keyring,
            broker_db_path=Path(broker_path),
            replay_db_path=Path(replay_path),
            busy_timeout_ms=10_000,
            monotonic_checkpoint_store=_SQLiteMonotonicCheckpointStore(
                Path(authority_path).parent / "anti-rollback.sqlite3"
            ),
            lineage_key_paths=(
                Path(key_path) / "lineage.private.pem",
                Path(key_path) / "lineage.public.pem",
            ),
        )
        ready.put("ready")
        if not gate.wait(timeout=_COMPETITOR_WATCHDOG_SECONDS):
            raise RuntimeError("multiprocess start gate timed out")
        output.put(("ok", _advance(authority).model_dump_json()))
    except BaseException as exc:
        output.put(("error", repr(exc)))


def _database_paths(tmp_path: Path) -> tuple[Path, Path, Path]:
    authority = tmp_path / "authority.sqlite3"
    broker = tmp_path / "broker.sqlite3"
    replay = tmp_path / "replay.sqlite3"
    _create_sqlite(broker)
    _create_sqlite(replay)
    return authority, broker, replay


def _production_root_kwargs(tmp_path: Path) -> dict[str, object]:
    return {
        "monotonic_checkpoint_store": _SQLiteMonotonicCheckpointStore(
            tmp_path / "anti-rollback.sqlite3"
        ),
        "lineage_key_paths": (
            tmp_path / "keys" / "lineage.private.pem",
            tmp_path / "keys" / "lineage.public.pem",
        ),
    }


def _authority(
    tmp_path: Path,
) -> tuple[
    object,
    Ed25519ContractSigner,
    VerifyOnlyEd25519Keyring,
    Path,
    Path,
    Path,
]:
    signer, keyring = _trust_root(tmp_path / "keys")
    authority_path, broker_path, replay_path = _database_paths(tmp_path)
    authority = replay_lineage_api.PersistentReplayLineageAuthority(
        authority_path,
        authority_id="persistent-lineage-root",
        signer=signer,
        keyring=keyring,
        broker_db_path=broker_path,
        replay_db_path=replay_path,
        **_production_root_kwargs(tmp_path),
    )
    return authority, signer, keyring, authority_path, broker_path, replay_path


def _as_protocol(
    authority: ReplayLineageAuthorityProtocol,
) -> ReplayLineageAuthorityProtocol:
    return authority


def _digest(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


def _advance(
    authority: ReplayLineageAuthorityProtocol,
    *,
    operation: str = "operation-1",
    replay_authority_id: str = "source-replay-root",
    lineage_id: str = "source-replay-lineage-1",
    previous: str = "head-0",
    next_head: str = "head-1",
    sequence: int = 1,
    binding: str = "binding-1",
) -> ReplayLineageCheckpointReceipt:
    return authority.compare_and_advance(
        operation_id=_digest(operation),
        replay_authority_id=replay_authority_id,
        lineage_id=lineage_id,
        previous_head_hash=_digest(previous),
        next_head_hash=_digest(next_head),
        sequence=sequence,
        claim_binding_hash=_digest(binding),
    )


def test_path_separation_rejects_missing_inputs_without_creating_them(tmp_path: Path) -> None:
    broker = tmp_path / "missing-broker.sqlite3"
    replay = tmp_path / "missing-replay.sqlite3"

    with pytest.raises(
        replay_lineage_api.ReplayLineageAuthoritySecurityError,
        match="must already exist",
    ):
        replay_lineage_api.assert_production_path_separation(
            authority_db_path=tmp_path / "authority.sqlite3",
            broker_db_path=broker,
            replay_db_path=replay,
        )

    assert not broker.exists()
    assert not replay.exists()


def test_path_separation_uses_normalized_paths_and_inode_identity(tmp_path: Path) -> None:
    broker = tmp_path / "broker.sqlite3"
    replay = tmp_path / "replay.sqlite3"
    _create_sqlite(broker)
    _create_sqlite(replay)
    separated = replay_lineage_api.assert_production_path_separation(
        authority_db_path=tmp_path / "authority.sqlite3",
        broker_db_path=broker,
        replay_db_path=replay,
    )
    assert separated.authority_db_path.is_absolute()

    replay_alias = tmp_path / "replay-alias.sqlite3"
    replay_alias.symlink_to(broker)
    with pytest.raises(
        replay_lineage_api.ReplayLineageAuthoritySecurityError,
        match="independent",
    ):
        replay_lineage_api.assert_production_path_separation(
            authority_db_path=tmp_path / "authority.sqlite3",
            broker_db_path=broker,
            replay_db_path=replay_alias,
        )

    authority_hardlink = tmp_path / "authority-hardlink.sqlite3"
    os.link(broker, authority_hardlink)
    with pytest.raises(
        replay_lineage_api.ReplayLineageAuthoritySecurityError,
        match="independent",
    ):
        replay_lineage_api.assert_production_path_separation(
            authority_db_path=authority_hardlink,
            broker_db_path=broker,
            replay_db_path=replay,
        )


def test_first_open_creates_closed_schema_and_pins_authority_identity(tmp_path: Path) -> None:
    signer, keyring = _trust_root(tmp_path / "keys")
    authority_path, broker_path, replay_path = _database_paths(tmp_path)
    authority = replay_lineage_api.PersistentReplayLineageAuthority(
        authority_path,
        authority_id="persistent-lineage-root",
        signer=signer,
        keyring=keyring,
        broker_db_path=broker_path,
        replay_db_path=replay_path,
        **_production_root_kwargs(tmp_path),
    )
    compatible = _as_protocol(authority)

    assert compatible.authority_id == "persistent-lineage-root"
    assert compatible.verifier_fingerprints == frozenset({signer.public_key_fingerprint})
    assert authority.audit_summary().operation_count == 0
    with sqlite3.connect(authority_path) as connection:
        objects = {
            (row[0], row[1])
            for row in connection.execute(
                "SELECT type, name FROM sqlite_master WHERE name NOT LIKE 'sqlite_%'"
            )
        }
    assert objects == {
        ("table", "replay_lineage_authority_meta"),
        ("table", "replay_lineage_head"),
        ("table", "replay_lineage_operation"),
        ("table", "replay_lineage_pending_advance"),
    }

    reopened = replay_lineage_api.PersistentReplayLineageAuthority(
        authority_path,
        authority_id="persistent-lineage-root",
        signer=signer,
        keyring=keyring,
        broker_db_path=broker_path,
        replay_db_path=replay_path,
        **_production_root_kwargs(tmp_path),
    )
    assert reopened.audit_summary() == authority.audit_summary()
    with pytest.raises(
        replay_lineage_api.ReplayLineageAuthoritySecurityError,
        match="authority_id.*pinned",
    ):
        replay_lineage_api.PersistentReplayLineageAuthority(
            authority_path,
            authority_id="different-lineage-root",
            signer=signer,
            keyring=keyring,
            broker_db_path=broker_path,
            replay_db_path=replay_path,
            **_production_root_kwargs(tmp_path),
        )


def test_committed_advance_is_signed_verified_and_idempotent_after_reopen(
    tmp_path: Path,
) -> None:
    signer, keyring = _trust_root(tmp_path / "keys")
    authority_path, broker_path, replay_path = _database_paths(tmp_path)
    authority = replay_lineage_api.PersistentReplayLineageAuthority(
        authority_path,
        authority_id="persistent-lineage-root",
        signer=signer,
        keyring=keyring,
        broker_db_path=broker_path,
        replay_db_path=replay_path,
        **_production_root_kwargs(tmp_path),
    )

    committed_receipt = _advance(authority)
    assert isinstance(committed_receipt, ReplayLineageCheckpointReceipt)
    assert keyring.verify(
        issuer=committed_receipt.authority_id,
        key_id=committed_receipt.key_id,
        key_purpose="replay_claim",
        namespace=REPLAY_CLAIM_NAMESPACE,
        payload=committed_receipt.signing_bytes(),
        signature=committed_receipt.signature,
    )
    authority.verify_current(
        replay_authority_id="source-replay-root",
        lineage_id="source-replay-lineage-1",
        head_hash=_digest("head-1"),
        sequence=1,
        receipt=committed_receipt,
    )

    reopened = replay_lineage_api.PersistentReplayLineageAuthority(
        authority_path,
        authority_id="persistent-lineage-root",
        signer=signer,
        keyring=keyring,
        broker_db_path=broker_path,
        replay_db_path=replay_path,
        **_production_root_kwargs(tmp_path),
    )
    recovered_receipt = _advance(reopened)
    assert recovered_receipt == committed_receipt
    assert recovered_receipt.signature == committed_receipt.signature
    assert reopened.audit_summary().operation_count == 1


def test_committed_transaction_recovers_after_the_response_is_lost(tmp_path: Path) -> None:
    authority, signer, keyring, authority_path, broker_path, replay_path = _authority(tmp_path)

    with pytest.raises(ConnectionError, match="response was lost"):
        _advance(authority)
        raise ConnectionError("response was lost after the SQLite commit")

    reopened = replay_lineage_api.PersistentReplayLineageAuthority(
        authority_path,
        authority_id="persistent-lineage-root",
        signer=signer,
        keyring=keyring,
        broker_db_path=broker_path,
        replay_db_path=replay_path,
        **_production_root_kwargs(tmp_path),
    )
    recovered = _advance(reopened)
    assert recovered.operation_id == _digest("operation-1")
    assert reopened.audit_summary().operation_count == 1


def test_operation_id_rebinding_is_rejected_even_across_replay_authorities(
    tmp_path: Path,
) -> None:
    authority, *_ = _authority(tmp_path)
    _advance(authority)

    with pytest.raises(
        replay_lineage_api.ReplayLineageAuthoritySecurityError,
        match="operation_id.*rebound",
    ):
        _advance(authority, next_head="different-head")
    with pytest.raises(
        replay_lineage_api.ReplayLineageAuthoritySecurityError,
        match="operation_id.*rebound",
    ):
        _advance(authority, replay_authority_id="different-replay-root")

    assert authority.audit_summary().operation_count == 1


def test_lineage_pins_genesis_and_rejects_fork_and_rollback(tmp_path: Path) -> None:
    authority, *_ = _authority(tmp_path)
    with pytest.raises(
        replay_lineage_api.ReplayLineageAuthoritySecurityError,
        match="genesis sequence",
    ):
        _advance(
            authority,
            operation="invalid-genesis",
            next_head="invalid-genesis-head",
            sequence=2,
        )
    _advance(authority)

    rejected = (
        {"operation": "alternate", "lineage_id": "alternate", "sequence": 1},
        {"operation": "fork", "previous": "not-head-1", "sequence": 2},
        {
            "operation": "rollback",
            "previous": "head-1",
            "next_head": "rollback-head",
            "sequence": 1,
        },
        {
            "operation": "skip",
            "previous": "head-1",
            "next_head": "skip-head",
            "sequence": 3,
        },
    )
    for request in rejected:
        with pytest.raises(
            replay_lineage_api.ReplayLineageAuthoritySecurityError,
            match="genesis|fork|rollback",
        ):
            _advance(authority, **request)

    second = _advance(
        authority,
        operation="operation-2",
        previous="head-1",
        next_head="head-2",
        sequence=2,
        binding="binding-2",
    )
    independent = _advance(
        authority,
        operation="other-operation-1",
        replay_authority_id="other-replay-root",
        lineage_id="other-lineage",
        previous="other-head-0",
        next_head="other-head-1",
    )
    assert second.sequence == 2
    assert independent.sequence == 1
    assert authority.audit_summary().lineage_count == 2


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        ({"lineage_id": ""}, "malformed"),
        ({"previous": "same", "next_head": "same"}, "retain"),
        ({"sequence": 0}, "malformed"),
    ],
)
def test_malformed_advances_fail_closed(
    tmp_path: Path,
    overrides: dict[str, object],
    message: str,
) -> None:
    authority, *_ = _authority(tmp_path)
    with pytest.raises(
        replay_lineage_api.ReplayLineageAuthoritySecurityError,
        match=message,
    ):
        _advance(authority, **overrides)


def test_malformed_hash_input_fails_closed(tmp_path: Path) -> None:
    authority, *_ = _authority(tmp_path)
    with pytest.raises(
        replay_lineage_api.ReplayLineageAuthoritySecurityError,
        match="malformed",
    ):
        authority.compare_and_advance(
            operation_id="not-a-hash",
            replay_authority_id="source-replay-root",
            lineage_id="source-replay-lineage-1",
            previous_head_hash=_digest("head-0"),
            next_head_hash=_digest("head-1"),
            sequence=1,
            claim_binding_hash=_digest("binding-1"),
        )


@pytest.mark.parametrize(
    "tamper_kind",
    [
        "receipt",
        "receipt_delete",
        "result",
        "result_delete",
        "operation",
        "operation_delete",
        "head",
        "head_delete",
        "checkpoint",
        "meta_delete",
    ],
)
def test_persisted_receipt_result_journal_and_head_tampering_fails_closed(
    tmp_path: Path,
    tamper_kind: str,
) -> None:
    authority, signer, keyring, authority_path, broker_path, replay_path = _authority(tmp_path)
    _advance(authority)
    _advance(
        authority,
        operation="operation-2",
        previous="head-1",
        next_head="head-2",
        sequence=2,
        binding="binding-2",
    )

    with sqlite3.connect(authority_path) as connection:
        if tamper_kind in {"receipt", "result", "head", "checkpoint"}:
            table, column, where = {
                "receipt": (
                    "replay_lineage_operation",
                    "receipt_json",
                    "operation_id = ?",
                ),
                "result": (
                    "replay_lineage_operation",
                    "result_json",
                    "operation_id = ?",
                ),
                "head": (
                    "replay_lineage_head",
                    "head_json",
                    "replay_authority_id = ?",
                ),
                "checkpoint": (
                    "replay_lineage_authority_meta",
                    "checkpoint_json",
                    "singleton = ?",
                ),
            }[tamper_kind]
            identifier: object = {
                "receipt": _digest("operation-1"),
                "result": _digest("operation-1"),
                "head": "source-replay-root",
                "checkpoint": 1,
            }[tamper_kind]
            stored = connection.execute(
                f"SELECT {column} FROM {table} WHERE {where}",
                (identifier,),
            ).fetchone()[0]
            payload = json.loads(stored)
            field = {
                "receipt": "signature",
                "result": "receipt_hash",
                "head": "head_hash",
                "checkpoint": "journal_root",
            }[tamper_kind]
            payload[field] = "f" * 64
            connection.execute(
                f"UPDATE {table} SET {column} = ? WHERE {where}",
                (
                    json.dumps(payload, sort_keys=True, separators=(",", ":")),
                    identifier,
                ),
            )
        elif tamper_kind == "receipt_delete":
            connection.execute(
                "UPDATE replay_lineage_operation SET receipt_json = '' WHERE journal_index = 1"
            )
        elif tamper_kind == "result_delete":
            connection.execute(
                "UPDATE replay_lineage_operation SET result_json = '' WHERE journal_index = 1"
            )
        elif tamper_kind == "operation":
            connection.execute(
                "UPDATE replay_lineage_operation SET journal_hash = ? WHERE journal_index = 1",
                ("f" * 64,),
            )
        elif tamper_kind == "operation_delete":
            connection.execute("DELETE FROM replay_lineage_operation WHERE journal_index = 1")
        elif tamper_kind == "head_delete":
            connection.execute(
                "DELETE FROM replay_lineage_head WHERE replay_authority_id = ?",
                ("source-replay-root",),
            )
        elif tamper_kind == "meta_delete":
            connection.execute("DELETE FROM replay_lineage_authority_meta")
        connection.commit()

    with pytest.raises(
        replay_lineage_api.ReplayLineageAuthoritySecurityError,
        match="tamper|malformed|missing|divergent|signature|commitment|invalid|checkpoint",
    ):
        replay_lineage_api.PersistentReplayLineageAuthority(
            authority_path,
            authority_id="persistent-lineage-root",
            signer=signer,
            keyring=keyring,
            broker_db_path=broker_path,
            replay_db_path=replay_path,
            **_production_root_kwargs(tmp_path),
        )


@pytest.mark.parametrize("schema_attack", ["unknown", "missing"])
def test_unknown_or_missing_schema_fails_closed(
    tmp_path: Path,
    schema_attack: str,
) -> None:
    _authority_instance, signer, keyring, authority_path, broker_path, replay_path = _authority(
        tmp_path
    )
    with sqlite3.connect(authority_path) as connection:
        if schema_attack == "unknown":
            connection.execute("CREATE TABLE attacker_state (value TEXT)")
        else:
            connection.execute("DROP TABLE replay_lineage_head")
        connection.commit()

    with pytest.raises(
        replay_lineage_api.ReplayLineageAuthoritySecurityError,
        match="schema.*unknown|schema.*missing",
    ):
        replay_lineage_api.PersistentReplayLineageAuthority(
            authority_path,
            authority_id="persistent-lineage-root",
            signer=signer,
            keyring=keyring,
            broker_db_path=broker_path,
            replay_db_path=replay_path,
            **_production_root_kwargs(tmp_path),
        )


def test_signed_checkpoint_export_import_and_backup_round_trip(tmp_path: Path) -> None:
    authority, signer, keyring, _authority_path, broker_path, replay_path = _authority(tmp_path)
    receipt = _advance(authority)

    checkpoint = authority.export_checkpoint()
    assert checkpoint.operation_count == 1
    assert keyring.verify(
        issuer=checkpoint.authority_id,
        key_id=checkpoint.key_id,
        key_purpose="replay_claim",
        namespace=REPLAY_CLAIM_NAMESPACE,
        payload=checkpoint.signing_bytes(),
        signature=checkpoint.signature,
    )
    assert authority.preflight_checkpoint(checkpoint).operation_count == 1
    assert authority.import_checkpoint(checkpoint.model_dump_json()).operation_count == 1

    backup_path = tmp_path / "authority-backup.sqlite3"
    exported = authority.export_backup(backup_path)
    assert exported.database_path == backup_path.resolve()
    assert exported.checkpoint == checkpoint
    backup = replay_lineage_api.PersistentReplayLineageAuthority(
        backup_path,
        authority_id="persistent-lineage-root",
        signer=signer,
        keyring=keyring,
        broker_db_path=broker_path,
        replay_db_path=replay_path,
        **_production_root_kwargs(tmp_path),
    )
    backup.verify_current(
        replay_authority_id="source-replay-root",
        lineage_id="source-replay-lineage-1",
        head_hash=_digest("head-1"),
        sequence=1,
        receipt=receipt,
    )


def test_external_checkpoint_rejects_database_rollback_and_divergence(
    tmp_path: Path,
) -> None:
    authority, signer, keyring, _authority_path, broker_path, replay_path = _authority(tmp_path)
    _advance(authority)
    old_checkpoint = authority.export_checkpoint()
    rollback_path = tmp_path / "rollback.sqlite3"
    authority.export_backup(rollback_path)
    _advance(
        authority,
        operation="operation-2",
        previous="head-1",
        next_head="head-2",
        sequence=2,
        binding="binding-2",
    )
    current_checkpoint = authority.export_checkpoint()

    with pytest.raises(
        replay_lineage_api.ReplayLineageAuthoritySecurityError,
        match="checkpoint rollback",
    ):
        authority.preflight_checkpoint(old_checkpoint)
    with pytest.raises(replay_lineage_api.ReplayLineageAuthorityRepairRequiredError):
        replay_lineage_api.PersistentReplayLineageAuthority(
            rollback_path,
            authority_id="persistent-lineage-root",
            signer=signer,
            keyring=keyring,
            broker_db_path=broker_path,
            replay_db_path=replay_path,
            **_production_root_kwargs(tmp_path),
        )

    divergent_path = tmp_path / "divergent.sqlite3"
    divergent = replay_lineage_api.PersistentReplayLineageAuthority(
        divergent_path,
        authority_id="persistent-lineage-root",
        signer=signer,
        keyring=keyring,
        broker_db_path=broker_path,
        replay_db_path=replay_path,
        mode="test-standalone",
    )
    _advance(divergent, next_head="divergent-head")
    with pytest.raises(
        replay_lineage_api.ReplayLineageAuthoritySecurityError,
        match="divergence",
    ):
        divergent.preflight_checkpoint(old_checkpoint)

    tampered = current_checkpoint.model_copy(update={"journal_root": "f" * 64})
    with pytest.raises(
        replay_lineage_api.ReplayLineageAuthoritySecurityError,
        match="signature",
    ):
        authority.preflight_checkpoint(tampered)


def test_competing_processes_commit_one_operation_exactly_once(tmp_path: Path) -> None:
    authority, _signer, _keyring, authority_path, broker_path, replay_path = _authority(tmp_path)
    context = multiprocessing.get_context("spawn")
    ready = context.Queue()
    gate = context.Event()
    output = context.Queue()
    processes = [
        context.Process(
            target=_competing_advance_process,
            args=(
                str(authority_path),
                str(broker_path),
                str(replay_path),
                str(tmp_path / "keys"),
                ready,
                gate,
                output,
            ),
        )
        for _ in range(4)
    ]
    for process in processes:
        process.start()
    for _ in processes:
        assert ready.get(timeout=_COMPETITOR_WATCHDOG_SECONDS) == "ready"
    gate.set()
    results = [output.get(timeout=_COMPETITOR_WATCHDOG_SECONDS) for _ in processes]
    for process in processes:
        process.join(timeout=_COMPETITOR_WATCHDOG_SECONDS)
        assert process.exitcode == 0

    assert {status for status, _payload in results} == {"ok"}
    assert len({payload for _status, payload in results}) == 1
    assert authority.audit_summary().operation_count == 1
    with sqlite3.connect(authority_path) as connection:
        assert connection.execute("SELECT COUNT(*) FROM replay_lineage_operation").fetchone() == (
            1,
        )


def test_expected_and_forbidden_verifier_fingerprints_are_enforced(
    tmp_path: Path,
) -> None:
    _authority_instance, signer, keyring, authority_path, broker_path, replay_path = _authority(
        tmp_path
    )
    expected = frozenset({signer.public_key_fingerprint})
    reopened = replay_lineage_api.PersistentReplayLineageAuthority(
        authority_path,
        authority_id="persistent-lineage-root",
        signer=signer,
        keyring=keyring,
        broker_db_path=broker_path,
        replay_db_path=replay_path,
        expected_verifier_fingerprints=expected,
        **_production_root_kwargs(tmp_path),
    )
    assert reopened.verifier_fingerprints == expected

    with pytest.raises(
        replay_lineage_api.ReplayLineageAuthoritySecurityError,
        match="expected trust set",
    ):
        replay_lineage_api.PersistentReplayLineageAuthority(
            authority_path,
            authority_id="persistent-lineage-root",
            signer=signer,
            keyring=keyring,
            broker_db_path=broker_path,
            replay_db_path=replay_path,
            expected_verifier_fingerprints=frozenset({"f" * 64}),
            **_production_root_kwargs(tmp_path),
        )
    with pytest.raises(
        replay_lineage_api.ReplayLineageAuthoritySecurityError,
        match="invalid fingerprint",
    ):
        replay_lineage_api.PersistentReplayLineageAuthority(
            authority_path,
            authority_id="persistent-lineage-root",
            signer=signer,
            keyring=keyring,
            broker_db_path=broker_path,
            replay_db_path=replay_path,
            expected_verifier_fingerprints=frozenset({"not-a-fingerprint"}),
            **_production_root_kwargs(tmp_path),
        )
    with pytest.raises(
        replay_lineage_api.ReplayLineageAuthoritySecurityError,
        match="forbidden",
    ):
        replay_lineage_api.PersistentReplayLineageAuthority(
            authority_path,
            authority_id="persistent-lineage-root",
            signer=signer,
            keyring=keyring,
            broker_db_path=broker_path,
            replay_db_path=replay_path,
            forbidden_verifier_fingerprints=expected,
            **_production_root_kwargs(tmp_path),
        )


def test_verify_current_checks_identity_fields_signature_and_persisted_binding(
    tmp_path: Path,
) -> None:
    authority, *_ = _authority(tmp_path)
    authority.verify_current(
        replay_authority_id="unseen-replay-root",
        lineage_id="unseen-lineage",
        head_hash=_digest("empty-head"),
        sequence=0,
        receipt=None,
    )
    receipt = _advance(authority)
    invalid_assertions = (
        {
            "replay_authority_id": "source-replay-root",
            "lineage_id": "wrong-lineage",
            "head_hash": _digest("head-1"),
            "sequence": 1,
            "receipt": receipt,
        },
        {
            "replay_authority_id": "source-replay-root",
            "lineage_id": "source-replay-lineage-1",
            "head_hash": _digest("wrong-head"),
            "sequence": 1,
            "receipt": receipt,
        },
        {
            "replay_authority_id": "source-replay-root",
            "lineage_id": "source-replay-lineage-1",
            "head_hash": _digest("head-1"),
            "sequence": 2,
            "receipt": receipt,
        },
        {
            "replay_authority_id": "source-replay-root",
            "lineage_id": "source-replay-lineage-1",
            "head_hash": _digest("head-1"),
            "sequence": 1,
            "receipt": receipt.model_copy(update={"next_head_hash": "f" * 64}),
        },
        {
            "replay_authority_id": "source-replay-root",
            "lineage_id": "source-replay-lineage-1",
            "head_hash": _digest("head-1"),
            "sequence": 1,
            "receipt": None,
        },
    )
    for assertion in invalid_assertions:
        with pytest.raises(replay_lineage_api.ReplayLineageAuthoritySecurityError):
            authority.verify_current(**assertion)


def test_authority_uses_no_os_clock_and_keeps_compatibility_domain_explicit() -> None:
    source_path = Path(replay_lineage_api.__file__)
    source = source_path.read_text()
    tree = ast.parse(source)
    imported_roots = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported_roots.update(alias.name.split(".", maxsplit=1)[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module is not None:
            imported_roots.add(node.module.split(".", maxsplit=1)[0])

    assert "time" not in imported_roots
    assert "datetime" not in imported_roots
    assert "importlib" not in imported_roots
    assert "__import__" not in source
    assert "rquant-replay-lineage-authority-checkpoint/v1" in source
    assert "external_anti_rollback_root" in source
    assert "rquant-replay-lineage-authority-journal-entry/v1" in source


def test_production_construction_requires_monotonic_checkpoint_store(tmp_path: Path) -> None:
    signer, keyring = _trust_root(tmp_path / "keys")
    authority_path, broker_path, replay_path = _database_paths(tmp_path)

    with pytest.raises(
        replay_lineage_api.ReplayLineageAuthoritySecurityError,
        match="production.*monotonic checkpoint store",
    ):
        replay_lineage_api.PersistentReplayLineageAuthority(
            authority_path,
            authority_id="persistent-lineage-root",
            signer=signer,
            keyring=keyring,
            broker_db_path=broker_path,
            replay_db_path=replay_path,
        )


def test_explicit_test_standalone_mode_is_marked_non_production(tmp_path: Path) -> None:
    signer, keyring = _trust_root(tmp_path / "keys")
    authority_path, broker_path, replay_path = _database_paths(tmp_path)
    authority = replay_lineage_api.PersistentReplayLineageAuthority(
        authority_path,
        authority_id="persistent-lineage-root",
        signer=signer,
        keyring=keyring,
        broker_db_path=broker_path,
        replay_db_path=replay_path,
        mode="test-standalone",
    )

    preflight = authority.preflight()
    assert preflight.mode == "test-standalone"
    assert preflight.non_production is True
    assert preflight.lineage_db_path == authority_path.resolve()
    assert preflight.root_store_path is None
    assert preflight.lineage_key_paths == ()
    assert preflight.root_verifier_fingerprints == frozenset()
    assert authority.audit_summary().non_production is True
    with pytest.raises(
        replay_lineage_api.ReplayLineageAuthoritySecurityError,
        match="production composition.*non-production",
    ):
        replay_lineage_api.compose_production_replay_lineage_authority(
            path=authority_path,
            authority_id="persistent-lineage-root",
            signer=signer,
            keyring=keyring,
            broker_db_path=broker_path,
            replay_db_path=replay_path,
            monotonic_checkpoint_store=None,
            lineage_key_paths=(),
            mode="test-standalone",
        )


def test_production_composition_rejects_shared_root_authority(tmp_path: Path) -> None:
    signer, keyring = _trust_root(tmp_path / "keys")
    authority_path, broker_path, replay_path = _database_paths(tmp_path)
    store = _SQLiteMonotonicCheckpointStore(
        tmp_path / "anti-rollback.sqlite3",
        authority_id="persistent-lineage-root",
    )

    with pytest.raises(
        replay_lineage_api.ReplayLineageAuthoritySecurityError,
        match="root authority.*independent",
    ):
        replay_lineage_api.compose_production_replay_lineage_authority(
            path=authority_path,
            authority_id="persistent-lineage-root",
            signer=signer,
            keyring=keyring,
            broker_db_path=broker_path,
            replay_db_path=replay_path,
            monotonic_checkpoint_store=store,
            lineage_key_paths=(
                tmp_path / "keys" / "lineage.private.pem",
                tmp_path / "keys" / "lineage.public.pem",
            ),
        )


def test_production_composition_rejects_root_storage_key_inode_alias(tmp_path: Path) -> None:
    signer, keyring = _trust_root(tmp_path / "keys")
    authority_path, broker_path, replay_path = _database_paths(tmp_path)
    root_path = tmp_path / "anti-rollback.sqlite3"
    store = _SQLiteMonotonicCheckpointStore(root_path)
    root_key_alias = tmp_path / "root-key-alias.pem"
    os.link(root_path, root_key_alias)

    with pytest.raises(
        replay_lineage_api.ReplayLineageAuthoritySecurityError,
        match="root storage.*key files.*independent",
    ):
        replay_lineage_api.compose_production_replay_lineage_authority(
            path=authority_path,
            authority_id="persistent-lineage-root",
            signer=signer,
            keyring=keyring,
            broker_db_path=broker_path,
            replay_db_path=replay_path,
            monotonic_checkpoint_store=store,
            lineage_key_paths=(
                tmp_path / "keys" / "lineage.private.pem",
                tmp_path / "keys" / "lineage.public.pem",
                root_key_alias,
            ),
        )


def test_production_open_pins_independent_root_at_current_checkpoint(tmp_path: Path) -> None:
    authority, *_ = _authority(tmp_path)
    store = _SQLiteMonotonicCheckpointStore(tmp_path / "anti-rollback.sqlite3")

    current = store.current(lineage_authority_id=authority.authority_id)
    assert current is not None
    assert current.root_authority_id == "independent-anti-rollback-root"
    assert current.checkpoint == authority.export_checkpoint()
    assert current.checkpoint.operation_count == 0
    preflight = authority.preflight()
    assert preflight.non_production is False
    assert preflight.root_store_path == (tmp_path / "anti-rollback.sqlite3").resolve()
    assert preflight.lineage_key_paths == (
        (tmp_path / "keys" / "lineage.private.pem").resolve(),
        (tmp_path / "keys" / "lineage.public.pem").resolve(),
    )
    assert preflight.root_verifier_fingerprints == store.verifier_fingerprints


def test_root_commit_response_loss_recovers_signed_pending_advance_on_reopen(
    tmp_path: Path,
) -> None:
    authority, signer, keyring, authority_path, broker_path, replay_path = _authority(tmp_path)
    lossy_store = _SQLiteMonotonicCheckpointStore(
        tmp_path / "anti-rollback.sqlite3",
        lose_compare_response_once=True,
    )
    lossy_authority = replay_lineage_api.PersistentReplayLineageAuthority(
        authority_path,
        authority_id="persistent-lineage-root",
        signer=signer,
        keyring=keyring,
        broker_db_path=broker_path,
        replay_db_path=replay_path,
        monotonic_checkpoint_store=lossy_store,
        lineage_key_paths=(
            tmp_path / "keys" / "lineage.private.pem",
            tmp_path / "keys" / "lineage.public.pem",
        ),
    )

    with pytest.raises(ConnectionError, match="commit-response loss"):
        _advance(lossy_authority)
    root_after_loss = lossy_store.current(lineage_authority_id="persistent-lineage-root")
    assert root_after_loss is not None
    assert root_after_loss.checkpoint.operation_count == 1

    reopened = replay_lineage_api.PersistentReplayLineageAuthority(
        authority_path,
        authority_id="persistent-lineage-root",
        signer=signer,
        keyring=keyring,
        broker_db_path=broker_path,
        replay_db_path=replay_path,
        **_production_root_kwargs(tmp_path),
    )
    recovered = _advance(reopened)
    assert recovered.operation_id == _digest("operation-1")
    assert reopened.audit_summary().operation_count == 1
    assert reopened.export_checkpoint() == root_after_loss.checkpoint


def test_same_key_legal_old_snapshot_replacement_requires_explicit_repair(
    tmp_path: Path,
) -> None:
    authority, signer, keyring, authority_path, broker_path, replay_path = _authority(tmp_path)
    _advance(authority)
    old_snapshot_path = tmp_path / "old-authority.sqlite3"
    authority.export_backup(old_snapshot_path)
    _advance(
        authority,
        operation="operation-2",
        previous="head-1",
        next_head="head-2",
        sequence=2,
        binding="binding-2",
    )
    shutil.copy2(old_snapshot_path, authority_path)

    with pytest.raises(replay_lineage_api.ReplayLineageAuthorityRepairRequiredError) as raised:
        replay_lineage_api.PersistentReplayLineageAuthority(
            authority_path,
            authority_id="persistent-lineage-root",
            signer=signer,
            keyring=keyring,
            broker_db_path=broker_path,
            replay_db_path=replay_path,
            **_production_root_kwargs(tmp_path),
        )
    assert raised.value.state.reason == "external_root_ahead_without_pending_proof"
    assert raised.value.state.non_production is False
    assert raised.value.state.local_operation_count == 1
    assert raised.value.state.root_operation_count == 2


@pytest.mark.parametrize("entrypoint", ["open", "compare_and_advance", "verify_current"])
def test_same_key_donor_database_is_rejected_against_external_high_water(
    tmp_path: Path,
    entrypoint: str,
) -> None:
    authority, signer, keyring, authority_path, broker_path, replay_path = _authority(tmp_path)
    original_receipt = _advance(authority)
    donor_path = tmp_path / "donor-authority.sqlite3"
    donor = replay_lineage_api.PersistentReplayLineageAuthority(
        donor_path,
        authority_id="persistent-lineage-root",
        signer=signer,
        keyring=keyring,
        broker_db_path=broker_path,
        replay_db_path=replay_path,
        mode="test-standalone",
    )
    _advance(donor, next_head="donor-head", binding="donor-binding")
    shutil.copy2(donor_path, authority_path)

    with pytest.raises(
        replay_lineage_api.ReplayLineageAuthoritySecurityError,
        match="external root.*local checkpoint diverge",
    ):
        if entrypoint == "open":
            replay_lineage_api.PersistentReplayLineageAuthority(
                authority_path,
                authority_id="persistent-lineage-root",
                signer=signer,
                keyring=keyring,
                broker_db_path=broker_path,
                replay_db_path=replay_path,
                **_production_root_kwargs(tmp_path),
            )
        elif entrypoint == "compare_and_advance":
            _advance(
                authority,
                operation="operation-2",
                previous="head-1",
                next_head="head-2",
                sequence=2,
                binding="binding-2",
            )
        else:
            authority.verify_current(
                replay_authority_id="source-replay-root",
                lineage_id="source-replay-lineage-1",
                head_hash=_digest("head-1"),
                sequence=1,
                receipt=original_receipt,
            )


def test_external_current_root_operation_id_must_bind_latest_commit(tmp_path: Path) -> None:
    authority, signer, keyring, authority_path, broker_path, replay_path = _authority(tmp_path)
    _advance(authority)
    root_path = tmp_path / "anti-rollback.sqlite3"
    with sqlite3.connect(root_path) as connection:
        stored = connection.execute(
            "SELECT root_json FROM root_state WHERE lineage_authority_id = ?",
            ("persistent-lineage-root",),
        ).fetchone()[0]
        payload = json.loads(stored)
        payload["operation_id"] = _digest("rebound-root-operation")
        connection.execute(
            "UPDATE root_state SET root_json = ? WHERE lineage_authority_id = ?",
            (json.dumps(payload, sort_keys=True), "persistent-lineage-root"),
        )

    with pytest.raises(
        replay_lineage_api.ReplayLineageAuthoritySecurityError,
        match="root operation.*latest committed operation",
    ):
        replay_lineage_api.PersistentReplayLineageAuthority(
            authority_path,
            authority_id="persistent-lineage-root",
            signer=signer,
            keyring=keyring,
            broker_db_path=broker_path,
            replay_db_path=replay_path,
            **_production_root_kwargs(tmp_path),
        )
