from __future__ import annotations

import os
import sqlite3
from datetime import UTC, datetime, timedelta
from pathlib import Path
from threading import Event
from types import SimpleNamespace
from uuid import uuid4

import pytest

from rquant.lab_claim_finalizer import LabClaimPublicationFinalizerAuthorityIssuer
from rquant.lab_claim_finalizer_trust import (
    LabClaimFinalizerTrustCertificate,
    LabClaimFinalizerTrustVerifier,
    sign_lab_claim_finalizer_trust_certificate,
)
from rquant.lab_claim_publication import (
    LabClaimPublicationFinalizerAuthority,
    LabClaimPublicationFinalizerRootKey,
)
from rquant.lab_jobs import ClaimPublicationConflictError, LabDatabaseIdentityError, LabJobStore
from rquant.lab_shard_protocol import LabClaimSpool
from rquant.lab_worker import LabDaemonConfigurationError, LabWorker

from .test_adapter_manifest import create_test_authorities
from .test_lab_jobs import _store
from .test_source_operation_contracts import MemoryCurrentClaimAuthority, _claim, _plan


def _root_key() -> LabClaimPublicationFinalizerRootKey:
    return LabClaimPublicationFinalizerRootKey(
        secret=b"test-finalizer-root-key-material-0001",
    )


def _test_only_preseed_finalizer_root_anchor(
    store: LabJobStore,
    root_key: LabClaimPublicationFinalizerRootKey | None = None,
) -> None:
    """Fixture-only stand-in for the future offline root-anchor bootstrap."""

    root = root_key or _root_key()
    with sqlite3.connect(store.path) as connection:
        connection.execute(
            "INSERT OR IGNORE INTO lab_claim_publication_finalizer_root_anchor "
            "(singleton, root_descriptor, root_key_digest) VALUES (1, ?, ?)",
            (root.descriptor, root.key_digest),
        )


_TRUST_BY_STORE: dict[
    str, tuple[LabClaimFinalizerTrustCertificate, LabClaimFinalizerTrustVerifier, object]
] = {}


def _issuer(store: LabJobStore) -> LabClaimPublicationFinalizerAuthorityIssuer:
    key = str(store.path.resolve())
    material = _TRUST_BY_STORE.get(key)
    if material is None:
        authorities = create_test_authorities(store.path.parent / "finalizer-trust")
        with store._connect() as connection:  # noqa: SLF001
            binding = store._finalizer_authority_binding(connection, path=store.path)  # noqa: SLF001
        unsigned = LabClaimFinalizerTrustCertificate(
            root_issuer=authorities.finalizer_trust_root.issuer,
            root_key_id=authorities.finalizer_trust_root.key_id,
            finalizer_issuer=authorities.finalizer_runtime.issuer,
            finalizer_key_id=authorities.finalizer_runtime.key_id,
            finalizer_public_key_fingerprint=authorities.finalizer_runtime.public_key_fingerprint,
            store_id=str(binding["store_id"]),
            database_device=binding["database_generation"][0],
            database_inode=binding["database_generation"][1],
            schema_version_bound=int(binding["schema_version"]),
            not_before=datetime(2020, 1, 1, tzinfo=UTC),
            expires_at=datetime(2030, 1, 1, tzinfo=UTC),
            signature="unsigned",
        )
        material = (
            sign_lab_claim_finalizer_trust_certificate(
                root_signer=authorities.finalizer_trust_root,
                certificate=unsigned,
            ),
            LabClaimFinalizerTrustVerifier(
                root_keyring=authorities.finalizer_trust_root_keyring,
                finalizer_keyring=authorities.finalizer_runtime_keyring,
            ),
            authorities.finalizer_runtime,
        )
        _TRUST_BY_STORE[key] = material
    certificate, verifier, signer = material
    return LabClaimPublicationFinalizerAuthorityIssuer(
        store=store,
        root_key=_root_key(),
        trust_certificate=certificate,
        trust_verifier=verifier,
        runtime_signer=signer,  # type: ignore[arg-type]
    )


def test_finalizer_mutation_authority_is_persistent_owner_scoped_and_fenced(tmp_path: Path) -> None:
    store = _store(tmp_path)
    _test_only_preseed_finalizer_root_anchor(store)
    issuer = _issuer(store)
    now = datetime.now(UTC)
    first = issuer.acquire(owner_id="finalizer-a", lease_seconds=30, now=now)

    assert first.scope == "rquant-lab-claim-publication-finalizer/v2"
    assert first.owner_id == "finalizer-a"
    assert first.fencing_token == 1
    with pytest.raises(ClaimPublicationConflictError, match="unavailable"):
        issuer.acquire(owner_id="finalizer-b", lease_seconds=30, now=now)

    replacement = issuer.acquire(owner_id="finalizer-a", lease_seconds=30, now=now)
    assert replacement.fencing_token == first.fencing_token
    assert replacement.lease_id == first.lease_id
    issuer.release(replacement, now=now + timedelta(seconds=1))
    next_owner = issuer.acquire(
        owner_id="finalizer-b", lease_seconds=30, now=now + timedelta(seconds=1)
    )
    assert next_owner.owner_id == "finalizer-b"
    assert next_owner.fencing_token == 2


def test_finalizer_db_anchor_is_not_a_runtime_trust_root(tmp_path: Path) -> None:
    store = _store(tmp_path)
    foreign = LabClaimPublicationFinalizerRootKey(
        secret=b"foreign-finalizer-root-key-material-00001",
    )
    now = datetime.now(UTC)

    first = _issuer(store).acquire(owner_id="finalizer-a", lease_seconds=5, now=now)
    issued = _issuer(store)
    foreign_issuer = LabClaimPublicationFinalizerAuthorityIssuer(
        store=store,
        root_key=foreign,
        trust_certificate=issued._trust_certificate,  # noqa: SLF001 - adversarial root-key swap
        trust_verifier=issued._trust_verifier,  # noqa: SLF001
        runtime_signer=issued._runtime_signer,  # noqa: SLF001
    )
    with pytest.raises(ClaimPublicationConflictError, match="unavailable"):
        foreign_issuer.acquire(owner_id="finalizer-b", lease_seconds=5, now=now)
    takeover = foreign_issuer.acquire(
        owner_id="finalizer-b", lease_seconds=5, now=now + timedelta(seconds=6)
    )
    assert takeover.fencing_token > first.fencing_token

    with pytest.raises(ClaimPublicationConflictError, match="conflict"):
        _issuer(store).renew(first, lease_seconds=5, now=now + timedelta(seconds=6))

    with sqlite3.connect(store.path) as connection:
        connection.execute("DELETE FROM lab_claim_publication_finalizer_root_anchor")
    renewed = foreign_issuer.renew(takeover, lease_seconds=5, now=now + timedelta(seconds=7))
    foreign_issuer.release(renewed, now=now + timedelta(seconds=7))


def test_raw_sql_cached_self_signed_certificate_cannot_acquire(tmp_path: Path) -> None:
    store = _store(tmp_path)
    legitimate = _issuer(store)
    foreign = create_test_authorities(tmp_path / "foreign-finalizer-trust")
    with store._connect() as connection:  # noqa: SLF001
        binding = store._finalizer_authority_binding(connection, path=store.path)  # noqa: SLF001
    unsigned = LabClaimFinalizerTrustCertificate(
        root_issuer=foreign.finalizer_trust_root.issuer,
        root_key_id=foreign.finalizer_trust_root.key_id,
        finalizer_issuer=foreign.finalizer_runtime.issuer,
        finalizer_key_id=foreign.finalizer_runtime.key_id,
        finalizer_public_key_fingerprint=foreign.finalizer_runtime.public_key_fingerprint,
        store_id=str(binding["store_id"]),
        database_device=binding["database_generation"][0],
        database_inode=binding["database_generation"][1],
        schema_version_bound=int(binding["schema_version"]),
        not_before=datetime(2020, 1, 1, tzinfo=UTC),
        expires_at=datetime(2030, 1, 1, tzinfo=UTC),
        signature="unsigned",
    )
    forged_certificate = sign_lab_claim_finalizer_trust_certificate(
        root_signer=foreign.finalizer_trust_root,
        certificate=unsigned,
    )
    with sqlite3.connect(store.path) as connection:
        raw = forged_certificate.model_dump_json().encode("utf-8")
        connection.execute(
            "INSERT INTO lab_claim_publication_finalizer_trust_cache "
            "(singleton, certificate_bytes, certificate_hash, cached_at) VALUES (1, ?, ?, ?)",
            (raw, "0" * 64, datetime.now(UTC).isoformat()),
        )
    forged_issuer = LabClaimPublicationFinalizerAuthorityIssuer(
        store=store,
        root_key=_root_key(),
        trust_certificate=forged_certificate,
        trust_verifier=legitimate._trust_verifier,  # noqa: SLF001 - fixed external root
        runtime_signer=foreign.finalizer_runtime,
    )
    with pytest.raises(ClaimPublicationConflictError, match="external_trust_invalid"):
        forged_issuer.acquire(owner_id="raw-sql", lease_seconds=30, now=datetime.now(UTC))


def test_finalizer_authority_rejects_forgery_and_replaced_store_root(tmp_path: Path) -> None:
    store = _store(tmp_path)
    _test_only_preseed_finalizer_root_anchor(store)
    issuer = _issuer(store)
    now = datetime.now(UTC)
    authority = issuer.acquire(owner_id="finalizer-a", lease_seconds=30, now=now)
    forged = LabClaimPublicationFinalizerAuthority(
        canonical_job_store_path=authority.canonical_job_store_path,
        database_generation=authority.database_generation,
        store_id=authority.store_id,
        schema_version=authority.schema_version,
        implementation_digest=authority.implementation_digest,
        owner_id=authority.owner_id,
        lease_id=authority.lease_id,
        fencing_token=authority.fencing_token,
        root_key=_root_key(),
        expires_at=authority.expires_at,
        lease_commitment=authority.lease_commitment,
        authority_mac="0" * 64,
    )
    with pytest.raises(ClaimPublicationConflictError, match="invalid"):
        issuer.renew(forged, lease_seconds=30, now=now)

    replacement = tmp_path / "replacement.sqlite3"
    with sqlite3.connect(store.path) as source, sqlite3.connect(replacement) as target:
        source.backup(target)
    os.replace(replacement, store.path)

    with pytest.raises(ClaimPublicationConflictError, match="invalid"):
        issuer.renew(authority, lease_seconds=30, now=now)


def test_v2_worker_does_not_consume_without_publication_verifier(tmp_path: Path) -> None:
    authorities = create_test_authorities(tmp_path / "keys")
    preimage = _claim(authorities)
    final_claim = preimage.bind_source_use_plan(
        _plan(
            authorities,
            claim=preimage,
            authority=MemoryCurrentClaimAuthority(preimage, authorities),
        )
    )
    spool = LabClaimSpool(tmp_path / "claims")
    entry = spool.publish(final_claim)
    worker = object.__new__(LabWorker)
    worker.claim_spool = spool
    worker.claim_publication_verifier = None
    worker._resource_retry_at = {}
    worker._verify_runtime_guard = lambda: None

    with pytest.raises(LabDaemonConfigurationError, match="verifier"):
        worker._consume_selected_claim(entry)
    assert spool.load(entry.path).claim == final_claim


def test_v15_to_v16_finalizer_authority_migration_is_reentrant_and_fail_closed(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    with sqlite3.connect(store.path) as connection:
        connection.execute("DROP TABLE lab_claim_publication_finalizer_attestation")
        connection.execute("DROP TABLE lab_claim_publication_finalizer_trust_cache")
        connection.execute(
            "INSERT INTO lab_claim_publication_finalizer_root_anchor "
            "(singleton, root_descriptor, root_key_digest) VALUES (1, ?, ?)",
            ("legacy-db-anchor", "f" * 64),
        )
        connection.execute("PRAGMA user_version = 15")

    migrated = LabJobStore(store.path)
    migrated.initialize()
    migrated.initialize()
    with sqlite3.connect(store.path) as connection:
        assert connection.execute("PRAGMA user_version").fetchone() == (16,)
        assert connection.execute(
            "SELECT COUNT(*) FROM lab_claim_publication_finalizer_lease"
        ).fetchone() == (0,)
        assert connection.execute(
            "SELECT COUNT(*) FROM lab_claim_publication_finalizer_root_anchor"
        ).fetchone() == (1,)
        assert connection.execute(
            "SELECT COUNT(*) FROM lab_claim_publication_finalizer_observation"
        ).fetchone() == (0,)
        assert connection.execute(
            "SELECT COUNT(*) FROM lab_claim_publication_finalizer_attestation"
        ).fetchone() == (0,)
        assert connection.execute(
            "SELECT COUNT(*) FROM lab_claim_publication_finalizer_trust_cache"
        ).fetchone() == (0,)

    # The legacy local anchor is retained as untrusted history.  A V2 lease can
    # only acquire after composition supplies an independently signed cert.
    assert (
        _issuer(migrated)
        .acquire(owner_id="migration-finalizer", lease_seconds=30, now=datetime.now(UTC))
        .fencing_token
        == 1
    )

    with sqlite3.connect(store.path) as connection:
        connection.execute("PRAGMA user_version = 99")

    with pytest.raises(LabDatabaseIdentityError, match="user_version|unexpectedly"):
        LabJobStore(store.path).initialize()


@pytest.mark.parametrize(
    "damage_sql",
    (
        "DROP TABLE lab_claim_publication_finalizer_attestation",
        "DROP TABLE lab_claim_publication_finalizer_trust_cache",
        "DROP TABLE lab_claim_publication_finalizer_observation",
        "DROP TABLE lab_claim_publication_finalizer_observation_degradation",
        "DROP INDEX ix_lab_claim_publication_finalizer_observation_attempt",
        "DROP INDEX ix_lab_claim_publication_finalizer_degradation_due",
        "ALTER TABLE lab_claim_publication_finalizer_attestation "
        "RENAME COLUMN certificate_hash TO missing_certificate_hash",
    ),
)
def test_every_runtime_connection_rejects_damaged_v16_schema_before_business_sql(
    tmp_path: Path,
    damage_sql: str,
) -> None:
    store = _store(tmp_path)
    with sqlite3.connect(store.path) as connection:
        connection.execute(damage_sql)

    with pytest.raises(LabDatabaseIdentityError, match="v16"):
        store.get_claim_publication(uuid4())
    with (
        pytest.raises(LabDatabaseIdentityError, match="v16"),
        store._transaction(),  # noqa: SLF001 - assert the runtime transaction boundary
    ):
        pytest.fail("damaged v16 schema reached business SQL")


@pytest.mark.parametrize("raise_from_tick", (False, True))
def test_worker_run_forever_always_closes_managed_children(raise_from_tick: bool) -> None:
    worker = object.__new__(LabWorker)
    closed: list[str] = []
    worker._stop = Event()
    worker.poll_interval_microseconds = 1
    worker._reap_managed_authority_children = lambda: closed.append("close")

    if raise_from_tick:

        def run_once() -> object:
            raise RuntimeError("tick failed")

        worker.run_once = run_once
        with pytest.raises(RuntimeError, match="tick failed"):
            worker.run_forever(install_signal_handlers=False)
    else:
        worker.run_once = lambda: SimpleNamespace(status="stopped")
        worker.run_forever(install_signal_handlers=False)

    assert closed == ["close"]
    worker.close()
    assert closed == ["close", "close"]
