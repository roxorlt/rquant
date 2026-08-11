from __future__ import annotations

import hashlib
import sqlite3
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from rquant.external_monotonic_root import (
    ExternalMonotonicRootRequest,
    UnixSocketExternalMonotonicRootClient,
)
from rquant.external_monotonic_root_service import PersistentExternalMonotonicRootBackend
from rquant.source_quota_authority import (
    SourceQuotaAuthorityIntegrityError,
    SourceQuotaAuthorityRepairRequiredError,
    SourceQuotaParentAuthority,
)
from rquant.source_quota_external_root_adapter import (
    SOURCE_QUOTA_ROOT_KEY_PURPOSE,
    SOURCE_QUOTA_ROOT_RECEIPT_NAMESPACE,
    SOURCE_QUOTA_ROOT_ROLE,
    SourceQuotaExternalCheckpoint,
    SourceQuotaExternalMonotonicRootAdapter,
    SourceQuotaExternalRootConfig,
    SourceQuotaExternalRootRoleHandler,
    SourceQuotaExternalRootSecurityError,
)
from rquant.source_quota_store import SourceQuotaStore
from rquant.strict_json import canonical_model_json_bytes, strict_model_validate_canonical_json

START = datetime(2026, 8, 9, 1, 30, tzinfo=UTC)
ROOT_AUTHORITY_ID = "source-quota-root-authority"
ROOT_STORE_ID = "source-quota-root-store"
ROOT_ISSUER = "source-quota-root-issuer"
ROOT_KEY_ID = "source-quota-root-key"
ROOT_FINGERPRINT = "e" * 64
MANIFEST_HASH = "f" * 64


class _QuotaSigner:
    key_id = "source-quota-local-key"

    def sign(self, payload: bytes) -> str:
        return hashlib.sha256(b"local-source-quota" + payload).hexdigest()

    def verify(self, payload: bytes, signature: str) -> bool:
        return signature == self.sign(payload)


class _RootSignerVerifier:
    issuer = ROOT_ISSUER
    key_id = ROOT_KEY_ID
    key_purpose = SOURCE_QUOTA_ROOT_KEY_PURPOSE
    signature_algorithm = "ed25519"
    public_key_fingerprint = ROOT_FINGERPRINT

    def sign(self, *, namespace: str, payload: bytes) -> str:
        return hashlib.sha256(namespace.encode("utf-8") + b"\0" + payload).hexdigest()

    def verify(self, *, namespace: str, payload: bytes, signature: str) -> bool:
        return signature == self.sign(namespace=namespace, payload=payload)


class _InProcessRootClient:
    role = SOURCE_QUOTA_ROOT_ROLE
    authority_id = ROOT_AUTHORITY_ID
    store_id = ROOT_STORE_ID
    transport = "nonproduction-inprocess-v1"
    manifest_hash = MANIFEST_HASH
    rollback_domain_id = "independent-root-domain"

    def __init__(self, path: Path, signer: _RootSignerVerifier) -> None:
        self._backend = PersistentExternalMonotonicRootBackend(
            path,
            role=self.role,
            authority_id=self.authority_id,
            store_id=self.store_id,
        )
        self._handler = SourceQuotaExternalRootRoleHandler(signer)
        self.fail_after_commit_once = False
        self.unavailable = False
        self.conflict_before_advance_once = False
        self.replay_response_once: str | None = None
        self.last_response: str | None = None
        self.requests: list[ExternalMonotonicRootRequest] = []

    def invoke(self, *, request_json: str) -> str | None:
        if self.unavailable:
            raise ConnectionError("root unavailable")
        request = strict_model_validate_canonical_json(
            ExternalMonotonicRootRequest,
            request_json,
        )
        self.requests.append(request)
        if self.replay_response_once is not None:
            response = self.replay_response_once
            self.replay_response_once = None
            return response
        if self.conflict_before_advance_once and request.kind == "advance":
            self.conflict_before_advance_once = False
            checkpoint = strict_model_validate_canonical_json(
                SourceQuotaExternalCheckpoint,
                request.checkpoint_json or "",
            ).model_copy(update={"local_checkpoint_signature_hash": "d" * 64})
            conflicting = ExternalMonotonicRootRequest.close(
                kind="advance",
                role=request.role,
                root_authority_id=request.root_authority_id,
                root_store_id=request.root_store_id,
                subject_authority_id=request.subject_authority_id,
                challenge_nonce="c" * 64,
                operation_id="b" * 64,
                previous_checkpoint_hash=request.previous_checkpoint_hash,
                checkpoint_contract=checkpoint.contract,
                checkpoint_hash=checkpoint.checkpoint_hash,
                checkpoint_json=canonical_model_json_bytes(checkpoint).decode("utf-8"),
            )
            self._backend.apply(conflicting)
        state = self._backend.apply(request)
        response = self._handler.response_json(request, state)
        self.last_response = response
        if self.fail_after_commit_once and request.kind in {"pin", "advance"}:
            self.fail_after_commit_once = False
            raise ConnectionError("response lost after external commit")
        return response


def _config() -> SourceQuotaExternalRootConfig:
    return SourceQuotaExternalRootConfig(
        transport="nonproduction-inprocess-v1",
        transport_manifest_hash=MANIFEST_HASH,
        root_authority_id=ROOT_AUTHORITY_ID,
        root_store_id=ROOT_STORE_ID,
        root_issuer=ROOT_ISSUER,
        root_key_id=ROOT_KEY_ID,
        root_public_key_fingerprint=ROOT_FINGERPRINT,
        witness_rollback_domain_id="independent-root-domain",
        local_rollback_domain_id="local-source-quota-domain",
    )


def _root_adapter(
    client: _InProcessRootClient,
    signer: _RootSignerVerifier,
) -> SourceQuotaExternalMonotonicRootAdapter:
    return SourceQuotaExternalMonotonicRootAdapter.for_nonproduction_test(
        config=_config(),
        client=client,
        root_verifiers=(signer,),
    )


def _declare_store(path: Path) -> None:
    SourceQuotaStore(path).declare_window(
        source="source",
        window_id="window",
        starts_at=START,
        resets_at=START + timedelta(minutes=1),
        total_units=20,
    )


def _authority(
    path: Path,
    root: SourceQuotaExternalMonotonicRootAdapter,
) -> SourceQuotaParentAuthority:
    return SourceQuotaParentAuthority.for_nonproduction_external_test(
        path,
        authority_id="source-quota-authority",
        signer=_QuotaSigner(),
        external_root=root,
    )


def _reserve(authority: SourceQuotaParentAuthority, operation_id: str = "reserve") -> object:
    return authority.reserve_parent(
        operation_id=operation_id,
        parent_id="parent",
        source="source",
        owner="owner",
        total_cost=10,
        now=START,
        expires_at=START + timedelta(seconds=30),
    )


def test_external_root_pins_and_advances_every_local_global_checkpoint(tmp_path: Path) -> None:
    path = tmp_path / "quota.sqlite3"
    _declare_store(path)
    signer = _RootSignerVerifier()
    client = _InProcessRootClient(tmp_path / "root.sqlite3", signer)
    authority = _authority(path, _root_adapter(client, signer))

    _reserve(authority)
    authority.record_intent(
        operation_id="intent",
        parent_id="parent",
        call_id="call",
        cost=3,
        now=START,
    )

    with sqlite3.connect(path) as connection:
        state = connection.execute(
            "SELECT binding_hash, config_hash, acknowledged_checkpoint_hash, "
            "acknowledged_receipt_json, pending_request_json "
            "FROM source_quota_external_root_state"
        ).fetchone()
        local = connection.execute(
            "SELECT journal_count, mutation_counter FROM source_quota_global_checkpoint"
        ).fetchone()
    assert state is not None
    assert len(state[0]) == 64
    assert state[1] == _config().config_hash
    assert len(state[2]) == 64
    assert state[3]
    assert state[4] is None
    assert local == (2, 2)
    assert [request.kind for request in client.requests].count("pin") == 1
    assert [request.kind for request in client.requests].count("advance") == 2


def test_external_commit_response_loss_recovers_exact_pending_request_after_restart(
    tmp_path: Path,
) -> None:
    path = tmp_path / "quota.sqlite3"
    _declare_store(path)
    signer = _RootSignerVerifier()
    client = _InProcessRootClient(tmp_path / "root.sqlite3", signer)
    root = _root_adapter(client, signer)
    authority = _authority(path, root)
    _reserve(authority)
    client.fail_after_commit_once = True

    with pytest.raises(SourceQuotaAuthorityIntegrityError, match="advance failed"):
        authority.record_intent(
            operation_id="intent",
            parent_id="parent",
            call_id="call",
            cost=3,
            now=START,
        )
    with sqlite3.connect(path) as connection:
        pending = connection.execute(
            "SELECT pending_request_json FROM source_quota_external_root_state"
        ).fetchone()[0]
        assert pending is not None
        pending_request = strict_model_validate_canonical_json(
            ExternalMonotonicRootRequest,
            pending,
        )

    reopened = _authority(path, root)
    replay = reopened.record_intent(
        operation_id="intent",
        parent_id="parent",
        call_id="call",
        cost=3,
        now=START,
    )
    matching = [
        request
        for request in client.requests
        if request.operation_id == pending_request.operation_id
    ]
    assert len(matching) == 2
    assert matching[0] == matching[1]
    assert replay.call is not None and replay.call.call_id == "call"
    with sqlite3.connect(path) as connection:
        assert connection.execute(
            "SELECT pending_request_json FROM source_quota_external_root_state"
        ).fetchone() == (None,)
        assert connection.execute(
            "SELECT COUNT(*) FROM source_quota_operation WHERE operation_id = 'intent'"
        ).fetchone() == (1,)


def test_external_pin_response_loss_recovers_exact_request_after_restart(tmp_path: Path) -> None:
    path = tmp_path / "quota.sqlite3"
    _declare_store(path)
    signer = _RootSignerVerifier()
    client = _InProcessRootClient(tmp_path / "root.sqlite3", signer)
    root = _root_adapter(client, signer)
    client.fail_after_commit_once = True

    with pytest.raises(SourceQuotaAuthorityIntegrityError, match="advance failed"):
        _authority(path, root)
    pin_requests = [request for request in client.requests if request.kind == "pin"]
    assert len(pin_requests) == 1
    reopened = _authority(path, root)
    pin_requests = [request for request in client.requests if request.kind == "pin"]
    assert len(pin_requests) == 2
    assert pin_requests[0] == pin_requests[1]
    assert reopened.get_parent("missing") is None


def test_external_root_rejects_whole_database_rollback_and_same_identity_donor(
    tmp_path: Path,
) -> None:
    path = tmp_path / "quota.sqlite3"
    snapshot = tmp_path / "old.sqlite3"
    _declare_store(path)
    signer = _RootSignerVerifier()
    client = _InProcessRootClient(tmp_path / "root.sqlite3", signer)
    root = _root_adapter(client, signer)
    authority = _authority(path, root)
    _reserve(authority)
    with sqlite3.connect(path) as source, sqlite3.connect(snapshot) as destination:
        source.backup(destination)
    authority.record_intent(
        operation_id="intent",
        parent_id="parent",
        call_id="call",
        cost=3,
        now=START,
    )
    with sqlite3.connect(snapshot) as source, sqlite3.connect(path) as destination:
        source.backup(destination)

    with pytest.raises(
        SourceQuotaAuthorityRepairRequiredError,
        match="external_root_ahead_without_pending_proof",
    ):
        _authority(path, root)

    donor_path = tmp_path / "donor.sqlite3"
    _declare_store(donor_path)
    with pytest.raises(
        SourceQuotaAuthorityRepairRequiredError,
        match="external_root_ahead_without_pending_proof",
    ):
        _authority(donor_path, root)


def test_old_external_response_replay_and_root_unavailable_fail_closed(tmp_path: Path) -> None:
    path = tmp_path / "quota.sqlite3"
    _declare_store(path)
    signer = _RootSignerVerifier()
    client = _InProcessRootClient(tmp_path / "root.sqlite3", signer)
    root = _root_adapter(client, signer)
    authority = _authority(path, root)
    _reserve(authority)
    old_response = client.last_response
    assert old_response is not None
    client.replay_response_once = old_response

    with pytest.raises(SourceQuotaAuthorityIntegrityError, match="unavailable or untrusted"):
        authority.record_intent(
            operation_id="intent",
            parent_id="parent",
            call_id="call",
            cost=3,
            now=START,
        )
    client.unavailable = True
    with pytest.raises(SourceQuotaAuthorityIntegrityError, match="unavailable or untrusted"):
        _authority(path, root)


def test_external_compare_and_advance_conflict_requires_repair(tmp_path: Path) -> None:
    path = tmp_path / "quota.sqlite3"
    _declare_store(path)
    signer = _RootSignerVerifier()
    client = _InProcessRootClient(tmp_path / "root.sqlite3", signer)
    root = _root_adapter(client, signer)
    authority = _authority(path, root)
    _reserve(authority)
    client.conflict_before_advance_once = True

    with pytest.raises(SourceQuotaAuthorityIntegrityError, match="advance failed"):
        authority.record_intent(
            operation_id="intent",
            parent_id="parent",
            call_id="call",
            cost=3,
            now=START,
        )
    with pytest.raises(
        SourceQuotaAuthorityRepairRequiredError,
        match="external_root_diverges_at_same_high_water",
    ):
        _authority(path, root)


def test_legacy_database_requires_repair_instead_of_silent_external_pin(tmp_path: Path) -> None:
    path = tmp_path / "quota.sqlite3"
    _declare_store(path)
    standalone = SourceQuotaParentAuthority.for_nonproduction_standalone(
        path,
        authority_id="source-quota-authority",
        signer=_QuotaSigner(),
    )
    _reserve(standalone)
    signer = _RootSignerVerifier()
    client = _InProcessRootClient(tmp_path / "root.sqlite3", signer)

    with pytest.raises(
        SourceQuotaAuthorityRepairRequiredError,
        match="legacy_database_without_external_binding",
    ):
        _authority(path, _root_adapter(client, signer))


def test_production_adapter_and_authority_reject_fake_client_and_verifier(
    tmp_path: Path,
) -> None:
    signer = _RootSignerVerifier()
    client = _InProcessRootClient(tmp_path / "root.sqlite3", signer)
    with pytest.raises(SourceQuotaExternalRootSecurityError, match="closed Unix"):
        SourceQuotaExternalMonotonicRootAdapter(
            config=_config(),
            client=client,  # type: ignore[arg-type]
            root_verifiers=(signer,),  # type: ignore[arg-type]
        )
    uninitialized_exact_client = object.__new__(UnixSocketExternalMonotonicRootClient)
    with pytest.raises(SourceQuotaExternalRootSecurityError, match="closed verifier"):
        SourceQuotaExternalMonotonicRootAdapter(
            config=_config(),
            client=uninitialized_exact_client,
            root_verifiers=(signer,),  # type: ignore[arg-type]
        )
    path = tmp_path / "quota.sqlite3"
    _declare_store(path)
    with pytest.raises(SourceQuotaAuthorityIntegrityError, match="closed external root"):
        SourceQuotaParentAuthority(
            path,
            authority_id="source-quota-authority",
            signer=_QuotaSigner(),
            external_root_config=_config(),
            external_root_client=client,  # type: ignore[arg-type]
            external_root_verifiers=(signer,),  # type: ignore[arg-type]
        )


def test_external_binding_tamper_fails_closed(tmp_path: Path) -> None:
    path = tmp_path / "quota.sqlite3"
    _declare_store(path)
    signer = _RootSignerVerifier()
    client = _InProcessRootClient(tmp_path / "root.sqlite3", signer)
    root = _root_adapter(client, signer)
    _authority(path, root)
    with sqlite3.connect(path) as connection:
        connection.execute(
            "UPDATE source_quota_external_root_state SET binding_hash = ?",
            ("1" * 64,),
        )
        connection.commit()

    with pytest.raises(SourceQuotaAuthorityRepairRequiredError, match="external_binding_changed"):
        _authority(path, root)


def test_external_receipt_tamper_and_binding_row_deletion_fail_closed(tmp_path: Path) -> None:
    path = tmp_path / "quota.sqlite3"
    _declare_store(path)
    signer = _RootSignerVerifier()
    client = _InProcessRootClient(tmp_path / "root.sqlite3", signer)
    root = _root_adapter(client, signer)
    _authority(path, root)
    with sqlite3.connect(path) as connection:
        connection.execute(
            "UPDATE source_quota_external_root_state "
            "SET acknowledged_receipt_json = acknowledged_receipt_json || ' '",
        )
        connection.commit()
    with pytest.raises(SourceQuotaAuthorityIntegrityError, match="receipt is untrusted"):
        _authority(path, root)

    second_path = tmp_path / "second.sqlite3"
    second_root_path = tmp_path / "second-root.sqlite3"
    _declare_store(second_path)
    second_client = _InProcessRootClient(second_root_path, signer)
    second_root = _root_adapter(second_client, signer)
    _authority(second_path, second_root)
    with sqlite3.connect(second_path) as connection:
        connection.execute("DELETE FROM source_quota_external_root_state")
        connection.commit()
    with pytest.raises(
        SourceQuotaAuthorityRepairRequiredError,
        match="legacy_database_without_external_binding",
    ):
        _authority(second_path, second_root)


def test_source_quota_root_role_is_closed() -> None:
    config = _config()
    assert config.role == SOURCE_QUOTA_ROOT_ROLE
    assert config.root_key_purpose == SOURCE_QUOTA_ROOT_KEY_PURPOSE
    assert config.root_receipt_namespace == SOURCE_QUOTA_ROOT_RECEIPT_NAMESPACE
