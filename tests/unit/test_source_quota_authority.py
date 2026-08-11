from __future__ import annotations

import hashlib
import importlib
import json
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from pathlib import Path
from threading import Barrier
from types import ModuleType

import pytest

from rquant.source_quota_store import (
    SourceQuotaConflictError,
    SourceQuotaExhaustedError,
    SourceQuotaStore,
)

START = datetime(2026, 8, 5, 1, 30, tzinfo=UTC)
END = START + timedelta(minutes=1)


class _Signer:
    key_id = "test-quota-authority-key"

    def __init__(self) -> None:
        self.fail = False

    def sign(self, payload: bytes) -> str:
        if self.fail:
            raise RuntimeError("signer unavailable")
        return hashlib.sha256(b"test-quota-authority" + payload).hexdigest()

    def verify(self, payload: bytes, signature: str) -> bool:
        return signature == hashlib.sha256(b"test-quota-authority" + payload).hexdigest()


class _VerifyRaisingSigner(_Signer):
    def verify(self, payload: bytes, signature: str) -> bool:
        raise RuntimeError("verify unavailable")


def _api() -> ModuleType:
    module = importlib.import_module("rquant.source_quota_authority")
    assert hasattr(module, "SourceQuotaParentAuthority"), "parent authority is not implemented"
    return module


def _standalone(
    api: ModuleType,
    path: Path,
    *,
    authority_id: str,
    signer: _Signer,
    busy_timeout_ms: int = 5_000,
) -> object:
    return api.SourceQuotaParentAuthority.for_nonproduction_standalone(
        path,
        authority_id=authority_id,
        signer=signer,
        busy_timeout_ms=busy_timeout_ms,
    )


def _authority(
    tmp_path: Path,
    *,
    total_units: int = 10,
    signer: _Signer | None = None,
) -> tuple[ModuleType, object, _Signer, Path]:
    path = tmp_path / "quota.sqlite3"
    store = SourceQuotaStore(path)
    store.declare_window(
        source="source",
        window_id="window",
        starts_at=START,
        resets_at=END,
        total_units=total_units,
    )
    api = _api()
    active_signer = signer or _Signer()
    authority = _standalone(
        api,
        path,
        authority_id="test-authority",
        signer=active_signer,
    )
    return api, authority, active_signer, path


def _reserve(authority: object, *, operation_id: str = "reserve-op", total_cost: int = 7) -> object:
    return authority.reserve_parent(
        operation_id=operation_id,
        parent_id="parent-1",
        source="source",
        owner="owner-1",
        total_cost=total_cost,
        now=START,
        expires_at=START + timedelta(seconds=30),
    )


def _finalize_call(
    api: ModuleType,
    authority: object,
    *,
    outcome: str,
    total_cost: int = 7,
    cost: int = 3,
) -> object:
    _reserve(authority, total_cost=total_cost)
    authority.record_intent(
        operation_id="intent-op",
        parent_id="parent-1",
        call_id="call-1",
        cost=cost,
        now=START,
    )
    authority.authorize_dispatch(
        operation_id="dispatch-op",
        parent_id="parent-1",
        call_id="call-1",
        now=START + timedelta(seconds=1),
    )
    return authority.finalize(
        operation_id="finalize-op",
        parent_id="parent-1",
        call_id="call-1",
        outcome=api.SourceQuotaCallOutcome(outcome),
        now=START + timedelta(seconds=2),
    )


def _release_terminal_parent(
    api: ModuleType,
    authority: object,
    *,
    total_cost: int,
    cost: int,
) -> object:
    _finalize_call(api, authority, outcome="SUCCESS", total_cost=total_cost, cost=cost)
    return authority.release_unused(
        operation_id="release-op",
        parent_id="parent-1",
        now=START + timedelta(seconds=3),
    )


def _replace_signed_result_json(
    api: ModuleType,
    signer: _Signer,
    path: Path,
    *,
    operation_id: str,
    result: dict[str, object],
) -> None:
    result_json = json.dumps(result, ensure_ascii=True, separators=(",", ":"), sort_keys=True)
    with sqlite3.connect(path) as connection:
        connection.row_factory = sqlite3.Row
        row = connection.execute(
            "SELECT * FROM source_quota_operation WHERE operation_id = ?", (operation_id,)
        ).fetchone()
        assert row is not None
        result_integrity_hash = api._replay_payload_hash(result_json)
        result_integrity_signature = signer.sign(
            api._replay_payload_signing_bytes(
                authority_id="test-authority",
                operation_id=row["operation_id"],
                effect_key=row["effect_key"],
                operation=row["operation"],
                payload_hash=row["payload_hash"],
                result_hash=row["result_hash"],
                result_integrity_hash=result_integrity_hash,
                key_id=signer.key_id,
            )
        )
        connection.execute(
            """
            UPDATE source_quota_operation
            SET result_json = ?, result_integrity_hash = ?, result_integrity_signature = ?
            WHERE operation_id = ?
            """,
            (result_json, result_integrity_hash, result_integrity_signature, operation_id),
        )
        connection.commit()


def _replace_fully_signed_result_json(
    api: ModuleType,
    signer: _Signer,
    path: Path,
    *,
    operation_id: str,
    result: dict[str, object],
) -> None:
    """Replace a journal result while recomputing every signer-protected binding."""

    result_json = json.dumps(result, ensure_ascii=True, separators=(",", ":"), sort_keys=True)
    with sqlite3.connect(path) as connection:
        connection.row_factory = sqlite3.Row
        row = connection.execute(
            "SELECT * FROM source_quota_operation WHERE operation_id = ?", (operation_id,)
        ).fetchone()
        assert row is not None
        operation = api.SourceQuotaOperationKind(row["operation"])
        parent = api.SourceQuotaParentSnapshot.model_validate(result["parent"])
        raw_call = result["call"]
        call = None if raw_call is None else api.SourceQuotaCallAllocation.model_validate(raw_call)
        payload_hash = api.canonical_sha256(
            api.SourceQuotaParentAuthority._operation_payload(operation, parent, call)
        )
        request_hash = api._request_hash(
            operation,
            api.SourceQuotaParentAuthority._operation_request_payload(operation, parent, call),
        )
        result_hash = api._result_hash(parent, call)
        unsigned = api.SourceQuotaOperationReceipt(
            authority_id="test-authority",
            operation_id=row["operation_id"],
            effect_key=row["effect_key"],
            operation=operation,
            payload_hash=payload_hash,
            result_hash=result_hash,
            key_id=signer.key_id,
            signature="pending",
        )
        receipt = unsigned.model_copy(update={"signature": signer.sign(unsigned.signing_bytes())})
        result_integrity_hash = api._replay_payload_hash(result_json)
        result_integrity_signature = signer.sign(
            api._replay_payload_signing_bytes(
                authority_id="test-authority",
                operation_id=row["operation_id"],
                effect_key=row["effect_key"],
                operation=operation.value,
                payload_hash=payload_hash,
                result_hash=result_hash,
                result_integrity_hash=result_integrity_hash,
                key_id=signer.key_id,
            )
        )
        connection.execute(
            """
            UPDATE source_quota_operation
            SET payload_hash = ?, request_hash = ?, result_hash = ?, result_json = ?,
                result_integrity_hash = ?, result_integrity_signature = ?, receipt_json = ?
            WHERE operation_id = ?
            """,
            (
                payload_hash,
                request_hash,
                result_hash,
                result_json,
                result_integrity_hash,
                result_integrity_signature,
                receipt.model_dump_json(),
                operation_id,
            ),
        )
        connection.commit()


def _replay_saved_operation(authority: object, operation_id: str) -> object:
    with authority._store._connect() as connection:
        row = connection.execute(
            "SELECT * FROM source_quota_operation WHERE operation_id = ?", (operation_id,)
        ).fetchone()
        assert row is not None
        return authority._replay_operation(connection, row)


def _downgrade_operation_journal_to_legacy(path: Path) -> None:
    with sqlite3.connect(path) as connection:
        connection.execute(
            "ALTER TABLE source_quota_operation RENAME TO source_quota_operation_with_integrity"
        )
        connection.execute(
            """
            CREATE TABLE source_quota_operation (
                operation_id TEXT PRIMARY KEY,
                effect_key TEXT NOT NULL UNIQUE,
                operation TEXT NOT NULL,
                payload_hash TEXT NOT NULL CHECK(length(payload_hash) = 64),
                result_hash TEXT NOT NULL CHECK(length(result_hash) = 64),
                result_json TEXT NOT NULL,
                receipt_json TEXT NOT NULL
            )
            """
        )
        connection.execute(
            """
            INSERT INTO source_quota_operation(
                operation_id, effect_key, operation, payload_hash, result_hash, result_json,
                receipt_json
            )
            SELECT operation_id, effect_key, operation, payload_hash, result_hash, result_json,
                   receipt_json
            FROM source_quota_operation_with_integrity
            """
        )
        connection.execute("DROP TABLE source_quota_operation_with_integrity")
        connection.commit()


def test_source_quota_parent_authority_module_exists() -> None:
    _api()


def test_schema_is_independent_and_preserves_v3_store_contract(tmp_path: Path) -> None:
    _api, authority, _signer, path = _authority(tmp_path)
    _reserve(authority)

    with sqlite3.connect(path) as connection:
        assert connection.execute("PRAGMA user_version").fetchone() == (3,)
        tables = {
            row[0]
            for row in connection.execute("SELECT name FROM sqlite_master WHERE type = 'table'")
        }
        assert {
            "source_parent_reservation",
            "source_call_allocation",
            "source_quota_operation",
            "quota_window",
            "quota_lease",
            "quota_usage",
        } <= tables
        assert "released_at" in {
            row[1] for row in connection.execute("PRAGMA table_info(quota_lease)")
        }


def test_reservation_coexists_with_remaining_and_old_limit_enforcement(tmp_path: Path) -> None:
    _api, authority, _signer, path = _authority(tmp_path)
    _reserve(authority, total_cost=7)
    store = SourceQuotaStore(path)

    assert store.remaining("source", now=START) == 3
    with pytest.raises(SourceQuotaExhaustedError):
        store.acquire(
            source="source",
            owner="outside-parent",
            units=4,
            now=START,
            expires_at=START + timedelta(seconds=30),
        )


def test_reserve_parent_replays_exact_receipt_and_result_across_reopen(tmp_path: Path) -> None:
    api, authority, signer, path = _authority(tmp_path)
    first = _reserve(authority)
    reopened = _standalone(api, path, authority_id="test-authority", signer=signer)
    replayed = _reserve(reopened)

    assert replayed == first
    assert first.parent.state is api.SourceQuotaParentState.OPEN
    assert SourceQuotaStore(path).remaining("source", now=START) == 3


def test_native_reserve_signed_chain_binds_claim_generation_and_fence(tmp_path: Path) -> None:
    api, authority, _signer, path = _authority(tmp_path)
    first = authority.reserve_parent(
        operation_id="reserve-bound",
        parent_id="parent-bound",
        source="source",
        owner="owner-bound",
        total_cost=7,
        now=START,
        expires_at=START + timedelta(seconds=30),
        claim_binding_hash="a" * 64,
        claim_generation=7,
        scheduler_fencing_token=11,
    )

    assert first.parent.claim_binding_hash == "a" * 64
    assert first.parent.claim_generation == 7
    assert first.parent.scheduler_fencing_token == 11
    with sqlite3.connect(path) as connection:
        row = connection.execute(
            "SELECT result_json, receipt_json FROM source_quota_operation "
            "WHERE operation_id = 'reserve-bound'"
        ).fetchone()
        assert row is not None
        assert '"claim_generation":7' in row[0]
        assert '"scheduler_fencing_token":11' in row[0]

    with pytest.raises(api.SourceQuotaAuthorityConflictError, match="operation_id payload"):
        authority.reserve_parent(
            operation_id="reserve-bound",
            parent_id="parent-bound",
            source="source",
            owner="owner-bound",
            total_cost=7,
            now=START,
            expires_at=START + timedelta(seconds=30),
            claim_binding_hash="b" * 64,
            claim_generation=6,
            scheduler_fencing_token=10,
        )


@pytest.mark.parametrize(
    ("column", "value"),
    (
        ("claim_binding_hash", "b" * 64),
        ("claim_generation", 8),
        ("scheduler_fencing_token", 12),
    ),
)
def test_native_replay_rejects_durable_parent_binding_mutation(
    tmp_path: Path,
    column: str,
    value: object,
) -> None:
    api, authority, signer, path = _authority(tmp_path)
    authority.reserve_parent(
        operation_id="reserve-bound",
        parent_id="parent-bound",
        source="source",
        owner="owner-bound",
        total_cost=7,
        now=START,
        expires_at=START + timedelta(seconds=30),
        claim_binding_hash="a" * 64,
        claim_generation=7,
        scheduler_fencing_token=11,
    )
    with sqlite3.connect(path) as connection:
        connection.execute(
            f"UPDATE source_parent_reservation SET {column} = ? WHERE parent_id = ?",
            (value, "parent-bound"),
        )
        connection.commit()
    reopened = _standalone(api, path, authority_id="test-authority", signer=signer)

    with pytest.raises(api.SourceQuotaAuthorityIntegrityError, match="immutable evidence"):
        reopened.reserve_parent(
            operation_id="reserve-bound",
            parent_id="parent-bound",
            source="source",
            owner="owner-bound",
            total_cost=7,
            now=START,
            expires_at=START + timedelta(seconds=30),
            claim_binding_hash="a" * 64,
            claim_generation=7,
            scheduler_fencing_token=11,
        )


@pytest.mark.parametrize("later_window", [False, True], ids=["no-active-window", "later-window"])
def test_reserve_replay_uses_original_contract_after_window_expiry(
    tmp_path: Path,
    later_window: bool,
) -> None:
    api, authority, signer, path = _authority(tmp_path)
    first = _reserve(authority)
    replay_at = END + timedelta(seconds=1)
    if later_window:
        SourceQuotaStore(path).declare_window(
            source="source",
            window_id="later-window",
            starts_at=END,
            resets_at=END + timedelta(minutes=1),
            total_units=99,
        )

    reopened = _standalone(api, path, authority_id="test-authority", signer=signer)

    replayed = reopened.reserve_parent(
        operation_id="reserve-op",
        parent_id="parent-1",
        source="source",
        owner="owner-1",
        total_cost=7,
        now=replay_at,
        expires_at=START + timedelta(seconds=30),
    )
    assert replayed == first
    assert replayed.parent.window_id == "window"
    with pytest.raises(SourceQuotaConflictError, match="operation_id payload conflicts"):
        reopened.reserve_parent(
            operation_id="reserve-op",
            parent_id="parent-1",
            source="source",
            owner="owner-1",
            total_cost=7,
            now=replay_at,
            expires_at=START + timedelta(seconds=31),
        )


@pytest.mark.parametrize("mutation", ["window", "lease"])
def test_reserve_replay_rejects_original_quota_contract_mutation_after_expiry(
    tmp_path: Path,
    mutation: str,
) -> None:
    api, authority, signer, path = _authority(tmp_path)
    _reserve(authority)
    with sqlite3.connect(path) as connection:
        if mutation == "window":
            connection.execute(
                "UPDATE quota_window SET total_units = total_units + 1 "
                "WHERE source = 'source' AND window_id = 'window'"
            )
        else:
            connection.execute(
                "UPDATE quota_lease SET expires_at = ? WHERE lease_id = ("
                "SELECT lease_id FROM source_parent_reservation WHERE parent_id = 'parent-1')",
                ((START + timedelta(seconds=31)).isoformat(),),
            )
        connection.commit()

    reopened = _standalone(api, path, authority_id="test-authority", signer=signer)

    with pytest.raises(api.SourceQuotaAuthorityIntegrityError, match="immutable evidence"):
        reopened.reserve_parent(
            operation_id="reserve-op",
            parent_id="parent-1",
            source="source",
            owner="owner-1",
            total_cost=7,
            now=END + timedelta(seconds=1),
            expires_at=START + timedelta(seconds=30),
        )


def test_reserve_replay_rejects_tampered_contract_before_caller_expiry_conflict(
    tmp_path: Path,
) -> None:
    api, authority, signer, path = _authority(tmp_path)
    _reserve(authority)
    with sqlite3.connect(path) as connection:
        connection.execute(
            "UPDATE quota_window SET total_units = total_units + 1 "
            "WHERE source = 'source' AND window_id = 'window'"
        )
        connection.commit()

    reopened = _standalone(api, path, authority_id="test-authority", signer=signer)

    with pytest.raises(api.SourceQuotaAuthorityIntegrityError, match="immutable evidence"):
        reopened.reserve_parent(
            operation_id="reserve-op",
            parent_id="parent-1",
            source="source",
            owner="owner-1",
            total_cost=7,
            now=END + timedelta(seconds=1),
            expires_at=START + timedelta(seconds=31),
        )


def test_receipt_signature_is_canonical_across_wall_times_and_transient_lease_ids(
    tmp_path: Path,
) -> None:
    first_api, first, _first_signer, _first_path = _authority(tmp_path / "first")
    second_api, second, _second_signer, _second_path = _authority(tmp_path / "second")
    assert first_api is second_api

    first_result = first.reserve_parent(
        operation_id="canonical-reserve",
        parent_id="canonical-parent",
        source="source",
        owner="owner-1",
        total_cost=7,
        now=START,
        expires_at=START + timedelta(seconds=30),
    )
    second_result = second.reserve_parent(
        operation_id="canonical-reserve",
        parent_id="canonical-parent",
        source="source",
        owner="owner-1",
        total_cost=7,
        now=START + timedelta(seconds=10),
        expires_at=START + timedelta(seconds=30),
    )

    assert first_result.parent.lease_id != second_result.parent.lease_id
    assert first_result.parent.reserved_at != second_result.parent.reserved_at
    assert first_result.parent.expires_at == second_result.parent.expires_at
    assert first_result.receipt.payload_hash == second_result.receipt.payload_hash
    assert first_result.receipt.result_hash != second_result.receipt.result_hash
    assert first_result.receipt.signing_bytes() != second_result.receipt.signing_bytes()


def test_reserve_same_operation_with_different_caller_expiry_conflicts_and_preserves_evidence(
    tmp_path: Path,
) -> None:
    api, authority, _signer, path = _authority(tmp_path)
    original = _reserve(authority)
    with sqlite3.connect(path) as connection:
        original_row = connection.execute(
            "SELECT payload_hash, result_hash, result_json, receipt_json "
            "FROM source_quota_operation WHERE operation_id = 'reserve-op'"
        ).fetchone()
        assert original_row is not None

    with pytest.raises(api.SourceQuotaAuthorityConflictError, match="operation_id"):
        authority.reserve_parent(
            operation_id="reserve-op",
            parent_id="parent-1",
            source="source",
            owner="owner-1",
            total_cost=7,
            now=START,
            expires_at=START + timedelta(seconds=31),
        )

    with sqlite3.connect(path) as connection:
        replay_row = connection.execute(
            "SELECT payload_hash, result_hash, result_json, receipt_json "
            "FROM source_quota_operation WHERE operation_id = 'reserve-op'"
        ).fetchone()
        assert replay_row == original_row
    assert authority.get_parent("parent-1") == original.parent
    assert original.parent.expires_at == START + timedelta(seconds=30)


@pytest.mark.parametrize("operation", ["intent", "authorize", "finalize", "pre_dispatch"])
def test_lifecycle_timestamp_one_second_before_durable_lower_bound_rejects_without_effect(
    tmp_path: Path,
    operation: str,
) -> None:
    api, authority, _signer, path = _authority(tmp_path)
    _reserve(authority)
    expected_remaining = SourceQuotaStore(path).remaining("source", now=START)

    if operation == "intent":
        with pytest.raises(ValueError, match="timestamp order"):
            authority.record_intent(
                operation_id="intent-too-early",
                parent_id="parent-1",
                call_id="call-1",
                cost=3,
                now=START - timedelta(seconds=1),
            )
        assert authority.get_call("call-1") is None
    else:
        authority.record_intent(
            operation_id="intent-op",
            parent_id="parent-1",
            call_id="call-1",
            cost=3,
            now=START + timedelta(seconds=1),
        )
        if operation == "authorize":
            with pytest.raises(ValueError, match="timestamp order"):
                authority.authorize_dispatch(
                    operation_id="authorize-too-early",
                    parent_id="parent-1",
                    call_id="call-1",
                    now=START,
                )
            assert authority.get_call("call-1").state is api.SourceQuotaCallState.INTENT
        elif operation == "finalize":
            authority.authorize_dispatch(
                operation_id="authorize-op",
                parent_id="parent-1",
                call_id="call-1",
                now=START + timedelta(seconds=2),
            )
            with pytest.raises(ValueError, match="timestamp order"):
                authority.finalize(
                    operation_id="finalize-too-early",
                    parent_id="parent-1",
                    call_id="call-1",
                    outcome=api.SourceQuotaCallOutcome.SUCCESS,
                    now=START + timedelta(seconds=1),
                )
            assert (
                authority.get_call("call-1").state is api.SourceQuotaCallState.DISPATCH_AUTHORIZED
            )
        else:
            with pytest.raises(ValueError, match="timestamp order"):
                authority.terminalize_unknown_before_dispatch(
                    operation_id="pre-dispatch-too-early",
                    parent_id="parent-1",
                    call_id="call-1",
                    now=START,
                )
            assert authority.get_call("call-1").state is api.SourceQuotaCallState.INTENT

    with sqlite3.connect(path) as connection:
        usage_count = connection.execute("SELECT COUNT(*) FROM quota_usage").fetchone()[0]
    assert usage_count == (1 if operation == "finalize" else 0)
    assert SourceQuotaStore(path).remaining("source", now=START) == expected_remaining


def test_lifecycle_timestamp_equal_to_every_durable_lower_bound_is_accepted(tmp_path: Path) -> None:
    api, authority, _signer, _path = _authority(tmp_path)
    _reserve(authority)

    intent = authority.record_intent(
        operation_id="intent-at-boundary",
        parent_id="parent-1",
        call_id="dispatched-call",
        cost=3,
        now=START,
    )
    authorized = authority.authorize_dispatch(
        operation_id="authorize-at-boundary",
        parent_id="parent-1",
        call_id="dispatched-call",
        now=START,
    )
    finalized = authority.finalize(
        operation_id="finalize-at-boundary",
        parent_id="parent-1",
        call_id="dispatched-call",
        outcome=api.SourceQuotaCallOutcome.SUCCESS,
        now=START,
    )
    authority.record_intent(
        operation_id="intent-before-dispatch-boundary",
        parent_id="parent-1",
        call_id="pre-dispatch-call",
        cost=3,
        now=START,
    )
    terminalized = authority.terminalize_unknown_before_dispatch(
        operation_id="pre-dispatch-at-boundary",
        parent_id="parent-1",
        call_id="pre-dispatch-call",
        now=START,
    )

    assert intent.call.intended_at == START
    assert authorized.call.authorized_at == START
    assert finalized.call.finalized_at == START
    assert terminalized.call.finalized_at == START


def test_authority_clock_rollback_after_reopen_rejects_without_new_allocation(
    tmp_path: Path,
) -> None:
    api, authority, signer, path = _authority(tmp_path)
    _reserve(authority)
    authority.record_intent(
        operation_id="first-intent",
        parent_id="parent-1",
        call_id="first-call",
        cost=3,
        now=START + timedelta(seconds=1),
    )
    before_remaining = SourceQuotaStore(path).remaining("source", now=START)
    reopened = _standalone(api, path, authority_id="test-authority", signer=signer)

    with pytest.raises(api.SourceQuotaAuthorityConflictError, match="clock rollback"):
        reopened.record_intent(
            operation_id="rolled-back-intent",
            parent_id="parent-1",
            call_id="second-call",
            cost=3,
            now=START,
        )

    assert reopened.get_call("second-call") is None
    assert SourceQuotaStore(path).remaining("source", now=START) == before_remaining


@pytest.mark.parametrize(
    ("mutation_sql", "parameters"),
    [
        (
            "UPDATE quota_lease SET expires_at = ? WHERE lease_id = "
            "(SELECT lease_id FROM source_parent_reservation WHERE parent_id = 'parent-1')",
            (START + timedelta(seconds=31),),
        ),
        (
            "UPDATE quota_lease SET window_id = 'alternate-window' WHERE lease_id = "
            "(SELECT lease_id FROM source_parent_reservation WHERE parent_id = 'parent-1')",
            (),
        ),
        (
            "UPDATE quota_window SET starts_at = ? "
            "WHERE source = 'source' AND window_id = 'window'",
            (START - timedelta(seconds=1),),
        ),
        (
            "UPDATE quota_window SET resets_at = ? "
            "WHERE source = 'source' AND window_id = 'window'",
            (END + timedelta(seconds=1),),
        ),
        (
            "UPDATE quota_lease SET quota_reset_at = ? WHERE lease_id = "
            "(SELECT lease_id FROM source_parent_reservation WHERE parent_id = 'parent-1')",
            (END - timedelta(seconds=1),),
        ),
        (
            "UPDATE quota_window SET total_units = 11 "
            "WHERE source = 'source' AND window_id = 'window'",
            (),
        ),
    ],
    ids=[
        "lease-expiry",
        "lease-window-link",
        "window-start",
        "window-end",
        "lease-reset",
        "capacity",
    ],
)
def test_replay_fails_closed_when_durable_quota_contract_is_mutated(
    tmp_path: Path,
    mutation_sql: str,
    parameters: tuple[object, ...],
) -> None:
    api, authority, _signer, path = _authority(tmp_path)
    _reserve(authority)
    with sqlite3.connect(path) as connection:
        connection.execute(
            """
            INSERT INTO quota_window(source, window_id, starts_at, resets_at, total_units)
            VALUES ('source', 'alternate-window', ?, ?, 10)
            """,
            (
                (START - timedelta(minutes=2)).isoformat(timespec="microseconds"),
                (START - timedelta(minutes=1)).isoformat(timespec="microseconds"),
            ),
        )
        serialized = tuple(
            value.isoformat(timespec="microseconds") if isinstance(value, datetime) else value
            for value in parameters
        )
        connection.execute(mutation_sql, serialized)
        connection.commit()

    with pytest.raises(api.SourceQuotaAuthorityIntegrityError, match="immutable evidence"):
        _reserve(authority)


def test_replay_fails_closed_when_parent_reservation_binding_is_mutated(tmp_path: Path) -> None:
    api, authority, _signer, path = _authority(tmp_path)
    _reserve(authority)
    with sqlite3.connect(path) as connection:
        connection.execute(
            "UPDATE source_parent_reservation SET owner = 'mutated-owner' "
            "WHERE parent_id = 'parent-1'"
        )
        connection.commit()

    with pytest.raises(api.SourceQuotaAuthorityIntegrityError, match="immutable evidence"):
        _reserve(authority)


def test_resigned_journal_result_cannot_mask_durable_quota_contract_mutation(
    tmp_path: Path,
) -> None:
    api, authority, signer, path = _authority(tmp_path)
    _reserve(authority)
    with sqlite3.connect(path) as connection:
        result = json.loads(
            connection.execute(
                "SELECT result_json FROM source_quota_operation WHERE operation_id = 'reserve-op'"
            ).fetchone()[0]
        )
        connection.execute(
            "UPDATE quota_lease SET expires_at = ? WHERE lease_id = "
            "(SELECT lease_id FROM source_parent_reservation WHERE parent_id = 'parent-1')",
            ((START + timedelta(seconds=31)).isoformat(timespec="microseconds"),),
        )
        connection.commit()
    _replace_signed_result_json(
        api,
        signer,
        path,
        operation_id="reserve-op",
        result=result,
    )

    with pytest.raises(api.SourceQuotaAuthorityIntegrityError, match="immutable evidence"):
        _reserve(authority)


def test_operation_conflict_matrix_rejects_identity_and_effect_reuse(tmp_path: Path) -> None:
    _api, authority, _signer, _path = _authority(tmp_path)
    first = _reserve(authority)

    assert _reserve(authority) == first
    with pytest.raises(SourceQuotaConflictError, match="operation_id"):
        _reserve(authority, total_cost=6)
    with pytest.raises(SourceQuotaConflictError, match="effect key"):
        _reserve(authority, operation_id="reserve-op-2")
    with pytest.raises(SourceQuotaConflictError, match="effect key"):
        authority.reserve_parent(
            operation_id="reserve-op-3",
            parent_id="parent-1",
            source="source",
            owner="owner-1",
            total_cost=6,
            now=START,
            expires_at=START + timedelta(seconds=30),
        )


@pytest.mark.parametrize(
    ("operation", "outcome"),
    [
        ("record_intent", None),
        ("authorize_dispatch", None),
        ("finalize", "SUCCESS"),
        ("finalize", "FAILURE"),
        ("finalize", "UNKNOWN"),
        ("unknown_before_dispatch", None),
        ("cancel", None),
        ("release_unused", None),
    ],
)
def test_every_operation_journal_replays_and_rejects_identity_or_effect_conflicts(
    tmp_path: Path,
    operation: str,
    outcome: str | None,
) -> None:
    api, authority, signer, path = _authority(tmp_path)
    _reserve(authority)
    call_id = "call-1"
    operation_id = f"{operation}-{outcome or 'operation'}"

    if operation in {"authorize_dispatch", "finalize"}:
        authority.record_intent(
            operation_id="setup-intent",
            parent_id="parent-1",
            call_id=call_id,
            cost=3,
            now=START,
        )
    if operation == "finalize":
        authority.authorize_dispatch(
            operation_id="setup-dispatch",
            parent_id="parent-1",
            call_id=call_id,
            now=START,
        )
    if operation in {"unknown_before_dispatch", "cancel"}:
        authority.record_intent(
            operation_id="setup-intent",
            parent_id="parent-1",
            call_id=call_id,
            cost=3,
            now=START,
        )

    def invoke(instance: object, *, identifier: str, different_payload: bool = False) -> object:
        current_call_id = "call-2" if different_payload else call_id
        if operation == "record_intent":
            return instance.record_intent(
                operation_id=identifier,
                parent_id="parent-1",
                call_id=current_call_id,
                cost=4 if different_payload else 3,
                now=START + timedelta(seconds=1),
            )
        if operation == "authorize_dispatch":
            return instance.authorize_dispatch(
                operation_id=identifier,
                parent_id="parent-1",
                call_id=current_call_id,
                now=START + timedelta(seconds=1),
            )
        if operation == "finalize":
            current_outcome = "FAILURE" if outcome == "SUCCESS" else "SUCCESS"
            outcome_name = current_outcome if different_payload else outcome
            return instance.finalize(
                operation_id=identifier,
                parent_id="parent-1",
                call_id=current_call_id,
                outcome=getattr(api.SourceQuotaCallOutcome, outcome_name),
                now=START + timedelta(seconds=1),
            )
        if operation == "unknown_before_dispatch":
            return instance.terminalize_unknown_before_dispatch(
                operation_id=identifier,
                parent_id="parent-1",
                call_id=current_call_id,
                now=START + timedelta(seconds=1),
            )
        if operation == "cancel":
            return instance.cancel(
                operation_id=identifier,
                parent_id="parent-1",
                call_id=current_call_id,
                now=START + timedelta(seconds=1),
            )
        assert operation == "release_unused"
        return instance.release_unused(
            operation_id=identifier,
            parent_id="parent-2" if different_payload else "parent-1",
            now=START + timedelta(seconds=1),
        )

    first = invoke(authority, identifier=operation_id)
    reopened = _standalone(api, path, authority_id="test-authority", signer=signer)
    assert invoke(reopened, identifier=operation_id) == first
    with pytest.raises(SourceQuotaConflictError, match="operation_id"):
        invoke(reopened, identifier=operation_id, different_payload=True)
    with pytest.raises(SourceQuotaConflictError, match="effect key"):
        invoke(reopened, identifier=f"{operation_id}-other")


def test_signer_failure_rolls_back_parent_lease_and_usage_effects(tmp_path: Path) -> None:
    api, authority, signer, path = _authority(tmp_path)
    signer.fail = True
    with pytest.raises(RuntimeError, match="signer unavailable"):
        _reserve(authority)
    with sqlite3.connect(path) as connection:
        assert connection.execute("SELECT COUNT(*) FROM quota_lease").fetchone() == (0,)
        assert connection.execute("SELECT COUNT(*) FROM source_parent_reservation").fetchone() == (
            0,
        )

    signer.fail = False
    _reserve(authority)
    authority.record_intent(
        operation_id="intent-op",
        parent_id="parent-1",
        call_id="call-1",
        cost=3,
        now=START,
    )
    signer.fail = True
    with pytest.raises(RuntimeError, match="signer unavailable"):
        authority.authorize_dispatch(
            operation_id="dispatch-op",
            parent_id="parent-1",
            call_id="call-1",
            now=START + timedelta(seconds=1),
        )
    snapshot = authority.get_parent("parent-1")
    assert snapshot is not None
    assert snapshot.calls[0].state is api.SourceQuotaCallState.INTENT
    with sqlite3.connect(path) as connection:
        assert connection.execute("SELECT COUNT(*) FROM quota_usage").fetchone() == (0,)


def test_intent_does_not_reserve_twice_and_aggregate_cannot_exceed_parent(tmp_path: Path) -> None:
    _api, authority, _signer, path = _authority(tmp_path)
    _reserve(authority)
    before = SourceQuotaStore(path).remaining("source", now=START)
    authority.record_intent(
        operation_id="intent-1",
        parent_id="parent-1",
        call_id="call-1",
        cost=3,
        now=START,
    )
    assert SourceQuotaStore(path).remaining("source", now=START) == before
    authority.record_intent(
        operation_id="intent-2",
        parent_id="parent-1",
        call_id="call-2",
        cost=4,
        now=START,
    )
    with pytest.raises(SourceQuotaConflictError, match="allocation"):
        authority.record_intent(
            operation_id="intent-3",
            parent_id="parent-1",
            call_id="call-3",
            cost=1,
            now=START,
        )


def test_authorize_dispatch_replay_response_loss_and_reopen_create_one_usage(
    tmp_path: Path,
) -> None:
    api, authority, signer, path = _authority(tmp_path)
    _reserve(authority)
    authority.record_intent(
        operation_id="intent-op",
        parent_id="parent-1",
        call_id="call-1",
        cost=3,
        now=START,
    )
    first = authority.authorize_dispatch(
        operation_id="dispatch-op",
        parent_id="parent-1",
        call_id="call-1",
        now=START + timedelta(seconds=1),
    )
    replayed = _standalone(
        api, path, authority_id="test-authority", signer=signer
    ).authorize_dispatch(
        operation_id="dispatch-op",
        parent_id="parent-1",
        call_id="call-1",
        now=START + timedelta(seconds=2),
    )

    assert replayed == first
    assert first.call.state is api.SourceQuotaCallState.DISPATCH_AUTHORIZED
    with sqlite3.connect(path) as connection:
        assert connection.execute("SELECT COUNT(*) FROM quota_usage").fetchone() == (1,)


def test_valid_and_invalid_transitions_keep_unknown_outcomes_distinct(tmp_path: Path) -> None:
    api, authority, _signer, path = _authority(tmp_path)
    _reserve(authority)
    authority.record_intent(
        operation_id="intent-before-unknown",
        parent_id="parent-1",
        call_id="before",
        cost=2,
        now=START,
    )
    before = authority.terminalize_unknown_before_dispatch(
        operation_id="unknown-before",
        parent_id="parent-1",
        call_id="before",
        now=START,
    )
    assert before.call.state is api.SourceQuotaCallState.CANCELLED_BEFORE_DISPATCH
    assert before.call.outcome is api.SourceQuotaCallOutcome.UNKNOWN_BEFORE_DISPATCH
    with pytest.raises(SourceQuotaConflictError, match="INTENT"):
        authority.authorize_dispatch(
            operation_id="dispatch-after-pre-dispatch-terminal",
            parent_id="parent-1",
            call_id="before",
            now=START,
        )

    authority.record_intent(
        operation_id="intent-dispatched",
        parent_id="parent-1",
        call_id="dispatched",
        cost=3,
        now=START,
    )
    with pytest.raises(SourceQuotaConflictError, match="DISPATCH_AUTHORIZED"):
        authority.finalize(
            operation_id="finalize-before-dispatch",
            parent_id="parent-1",
            call_id="dispatched",
            outcome=api.SourceQuotaCallOutcome.SUCCESS,
            now=START,
        )
    authority.authorize_dispatch(
        operation_id="dispatch-unknown",
        parent_id="parent-1",
        call_id="dispatched",
        now=START,
    )
    dispatched = authority.finalize(
        operation_id="unknown-dispatched",
        parent_id="parent-1",
        call_id="dispatched",
        outcome=api.SourceQuotaCallOutcome.UNKNOWN,
        now=START,
    )
    assert dispatched.call.state is api.SourceQuotaCallState.UNKNOWN
    assert dispatched.call.outcome is api.SourceQuotaCallOutcome.UNKNOWN
    with pytest.raises(ValueError, match="before dispatch"):
        authority.finalize(
            operation_id="wrong-unknown-kind",
            parent_id="parent-1",
            call_id="dispatched",
            outcome=api.SourceQuotaCallOutcome.UNKNOWN_BEFORE_DISPATCH,
            now=START,
        )
    with sqlite3.connect(path) as connection:
        assert connection.execute("SELECT SUM(units) FROM quota_usage").fetchone() == (3,)


def test_cancel_is_only_valid_from_intent(tmp_path: Path) -> None:
    api, authority, _signer, _path = _authority(tmp_path)
    _reserve(authority)
    authority.record_intent(
        operation_id="intent-cancelled",
        parent_id="parent-1",
        call_id="cancelled",
        cost=2,
        now=START,
    )
    cancelled = authority.cancel(
        operation_id="cancel-op",
        parent_id="parent-1",
        call_id="cancelled",
        now=START,
    )
    assert cancelled.call.state is api.SourceQuotaCallState.CANCELLED_BEFORE_DISPATCH
    assert cancelled.call.outcome is None
    authority.record_intent(
        operation_id="intent-dispatched-cancel",
        parent_id="parent-1",
        call_id="dispatched-cancel",
        cost=2,
        now=START,
    )
    authority.authorize_dispatch(
        operation_id="dispatch-before-cancel",
        parent_id="parent-1",
        call_id="dispatched-cancel",
        now=START,
    )
    with pytest.raises(SourceQuotaConflictError, match="INTENT"):
        authority.cancel(
            operation_id="cancel-after-dispatch",
            parent_id="parent-1",
            call_id="dispatched-cancel",
            now=START,
        )


def test_failure_is_a_valid_dispatched_terminal_outcome(tmp_path: Path) -> None:
    api, authority, _signer, _path = _authority(tmp_path)
    _reserve(authority)
    authority.record_intent(
        operation_id="intent-failure",
        parent_id="parent-1",
        call_id="failure",
        cost=2,
        now=START,
    )
    authority.authorize_dispatch(
        operation_id="dispatch-failure",
        parent_id="parent-1",
        call_id="failure",
        now=START,
    )
    result = authority.finalize(
        operation_id="finalize-failure",
        parent_id="parent-1",
        call_id="failure",
        outcome=api.SourceQuotaCallOutcome.FAILURE,
        now=START,
    )

    assert result.call.state is api.SourceQuotaCallState.FAILURE
    assert result.call.outcome is api.SourceQuotaCallOutcome.FAILURE


def test_dispatch_then_success_is_a_direct_call_state_transition(tmp_path: Path) -> None:
    api, authority, _signer, _path = _authority(tmp_path)
    _reserve(authority)
    intended = authority.record_intent(
        operation_id="intent-success",
        parent_id="parent-1",
        call_id="success",
        cost=3,
        now=START,
    )
    authorized = authority.authorize_dispatch(
        operation_id="dispatch-success",
        parent_id="parent-1",
        call_id="success",
        now=START + timedelta(seconds=1),
    )
    finalized = authority.finalize(
        operation_id="finalize-success",
        parent_id="parent-1",
        call_id="success",
        outcome=api.SourceQuotaCallOutcome.SUCCESS,
        now=START + timedelta(seconds=2),
    )

    assert intended.call.state is api.SourceQuotaCallState.INTENT
    assert authorized.call.state is api.SourceQuotaCallState.DISPATCH_AUTHORIZED
    assert finalized.call.state is api.SourceQuotaCallState.SUCCESS


def test_fully_consumed_parent_releases_to_closed_without_unused_capacity(tmp_path: Path) -> None:
    api, authority, _signer, _path = _authority(tmp_path)
    _reserve(authority, total_cost=7)
    authority.record_intent(
        operation_id="intent-full",
        parent_id="parent-1",
        call_id="full",
        cost=7,
        now=START,
    )
    authority.authorize_dispatch(
        operation_id="dispatch-full",
        parent_id="parent-1",
        call_id="full",
        now=START + timedelta(seconds=1),
    )
    authority.finalize(
        operation_id="finalize-full",
        parent_id="parent-1",
        call_id="full",
        outcome=api.SourceQuotaCallOutcome.SUCCESS,
        now=START + timedelta(seconds=2),
    )
    released = authority.release_unused(
        operation_id="release-full",
        parent_id="parent-1",
        now=START + timedelta(seconds=3),
    )

    assert released.parent.state is api.SourceQuotaParentState.CLOSED
    assert released.parent.unused_released == 0
    assert released.parent.reserved_cost == (
        released.parent.consumed_cost + released.parent.unused_released
    )


def test_unresolved_calls_block_release_and_release_proves_accounting(tmp_path: Path) -> None:
    api, authority, _signer, path = _authority(tmp_path)
    _reserve(authority, total_cost=10)
    authority.record_intent(
        operation_id="intent-a",
        parent_id="parent-1",
        call_id="a",
        cost=3,
        now=START,
    )
    with pytest.raises(SourceQuotaConflictError, match="unresolved"):
        authority.release_unused(
            operation_id="release-intent",
            parent_id="parent-1",
            now=START,
        )
    authority.authorize_dispatch(
        operation_id="dispatch-a",
        parent_id="parent-1",
        call_id="a",
        now=START,
    )
    with pytest.raises(SourceQuotaConflictError, match="unresolved"):
        authority.release_unused(
            operation_id="release-dispatched",
            parent_id="parent-1",
            now=START,
        )
    authority.finalize(
        operation_id="success-a",
        parent_id="parent-1",
        call_id="a",
        outcome=api.SourceQuotaCallOutcome.SUCCESS,
        now=START,
    )
    authority.record_intent(
        operation_id="intent-b",
        parent_id="parent-1",
        call_id="b",
        cost=4,
        now=START,
    )
    authority.cancel(
        operation_id="cancel-b",
        parent_id="parent-1",
        call_id="b",
        now=START,
    )
    released = authority.release_unused(
        operation_id="release-done",
        parent_id="parent-1",
        now=START + timedelta(seconds=1),
    )

    assert released.parent.state is api.SourceQuotaParentState.COMPENSATED
    assert released.parent.reserved_cost == 10
    assert released.parent.consumed_cost == 3
    assert released.parent.unused_released == 7
    assert SourceQuotaStore(path).remaining("source", now=START + timedelta(seconds=1)) == 7


def test_journal_replay_validates_hashes_and_receipt_signature(tmp_path: Path) -> None:
    api, authority, _signer, path = _authority(tmp_path)
    _reserve(authority)
    with sqlite3.connect(path) as connection:
        connection.execute(
            "UPDATE source_quota_operation SET result_hash = '0' || substr(result_hash, 2) "
            "WHERE operation_id = 'reserve-op'"
        )
        connection.commit()
    with pytest.raises(api.SourceQuotaAuthorityIntegrityError, match="journal"):
        _reserve(authority)


def test_journal_replay_rejects_a_tampered_payload_hash(tmp_path: Path) -> None:
    api, authority, _signer, path = _authority(tmp_path)
    _reserve(authority)
    with sqlite3.connect(path) as connection:
        connection.execute(
            "UPDATE source_quota_operation SET payload_hash = '0' || substr(payload_hash, 2) "
            "WHERE operation_id = 'reserve-op'"
        )
        connection.commit()
    with pytest.raises(api.SourceQuotaAuthorityIntegrityError, match="hash"):
        _reserve(authority)


def test_journal_replay_rejects_a_tampered_receipt_signature(tmp_path: Path) -> None:
    api, authority, _signer, path = _authority(tmp_path)
    _reserve(authority)
    with sqlite3.connect(path) as connection:
        receipt = json.loads(
            connection.execute(
                "SELECT receipt_json FROM source_quota_operation WHERE operation_id = 'reserve-op'"
            ).fetchone()[0]
        )
        receipt["signature"] = "tampered"
        connection.execute(
            "UPDATE source_quota_operation SET receipt_json = ? WHERE operation_id = 'reserve-op'",
            (json.dumps(receipt, separators=(",", ":"), sort_keys=True),),
        )
        connection.commit()
    with pytest.raises(api.SourceQuotaAuthorityIntegrityError, match="signature"):
        _reserve(authority)


def test_journal_replay_rejects_tampering_of_every_persisted_result_scalar(
    tmp_path: Path,
) -> None:
    api, authority, signer, path = _authority(tmp_path / "template")
    _reserve(authority)
    authority.record_intent(
        operation_id="intent-op",
        parent_id="parent-1",
        call_id="call-1",
        cost=3,
        now=START,
    )
    authority.authorize_dispatch(
        operation_id="dispatch-op",
        parent_id="parent-1",
        call_id="call-1",
        now=START + timedelta(seconds=1),
    )
    with sqlite3.connect(path) as connection:
        template = json.loads(
            connection.execute(
                "SELECT result_json FROM source_quota_operation WHERE operation_id = 'dispatch-op'"
            ).fetchone()[0]
        )

    def move_timestamp(value: str) -> str:
        return (datetime.fromisoformat(value) + timedelta(seconds=1)).isoformat()

    def mutate(value: dict[str, object], path: tuple[str | int, ...]) -> dict[str, object]:
        target: object = value
        for component in path[:-1]:
            target = target[component]  # type: ignore[index]
        leaf = path[-1]
        current = target[leaf]  # type: ignore[index]
        if leaf in {"lease_id", "usage_id"}:
            target[leaf] = "b" * 64  # type: ignore[index]
        else:
            target[leaf] = move_timestamp(current)  # type: ignore[index]
        return value

    dynamic_paths = [
        ("parent", "lease_id"),
        ("parent", "reserved_at"),
        ("parent", "calls", 0, "intended_at"),
        ("parent", "calls", 0, "authorized_at"),
        ("parent", "calls", 0, "usage_id"),
        ("call", "intended_at"),
        ("call", "authorized_at"),
        ("call", "usage_id"),
    ]
    accepted_paths: list[tuple[str | int, ...]] = []
    for index, path_to_mutate in enumerate(dynamic_paths):
        instance_path = tmp_path / f"scalar-{index}" / "quota.sqlite3"
        instance_path.parent.mkdir()
        with (
            sqlite3.connect(path) as source_connection,
            sqlite3.connect(instance_path) as destination_connection,
        ):
            source_connection.backup(destination_connection)
        with sqlite3.connect(instance_path) as connection:
            tampered = mutate(json.loads(json.dumps(template)), path_to_mutate)
            connection.execute(
                "UPDATE source_quota_operation SET result_json = ? "
                "WHERE operation_id = 'dispatch-op'",
                (json.dumps(tampered, ensure_ascii=True, separators=(",", ":"), sort_keys=True),),
            )
            connection.commit()
        replay = _standalone(
            api,
            instance_path,
            authority_id="test-authority",
            signer=signer,
        )
        try:
            replay.authorize_dispatch(
                operation_id="dispatch-op",
                parent_id="parent-1",
                call_id="call-1",
                now=START + timedelta(seconds=2),
            )
        except api.SourceQuotaAuthorityIntegrityError:
            continue
        accepted_paths.append(path_to_mutate)

    assert accepted_paths == []


@pytest.mark.parametrize(
    "state",
    [
        "DISPATCH_AUTHORIZED",
        "SUCCESS",
        "FAILURE",
        "UNKNOWN",
    ],
)
@pytest.mark.parametrize("missing_field", ["authorized_at", "usage_id"])
def test_call_allocation_requires_both_authorization_evidence_after_dispatch(
    state: str,
    missing_field: str,
) -> None:
    api = _api()
    allocation = {
        "call_id": "call-1",
        "parent_id": "parent-1",
        "cost": 1,
        "state": state,
        "outcome": None if state == "DISPATCH_AUTHORIZED" else state,
        "intended_at": START,
        "authorized_at": START,
        "finalized_at": None if state == "DISPATCH_AUTHORIZED" else START,
        "usage_id": "a" * 64,
    }
    allocation[missing_field] = None

    with pytest.raises(ValueError, match="authorization and quota usage"):
        api.SourceQuotaCallAllocation.model_validate(allocation)


@pytest.mark.parametrize("state", ["INTENT", "CANCELLED_BEFORE_DISPATCH"])
@pytest.mark.parametrize("present_field", ["authorized_at", "usage_id"])
def test_call_allocation_forbids_any_authorization_evidence_before_dispatch(
    state: str,
    present_field: str,
) -> None:
    api = _api()
    allocation = {
        "call_id": "call-1",
        "parent_id": "parent-1",
        "cost": 1,
        "state": state,
        "outcome": None,
        "intended_at": START,
        "authorized_at": None,
        "finalized_at": START if state == "CANCELLED_BEFORE_DISPATCH" else None,
        "usage_id": None,
    }
    allocation[present_field] = START if present_field == "authorized_at" else "a" * 64

    with pytest.raises(ValueError, match="authorization and quota usage"):
        api.SourceQuotaCallAllocation.model_validate(allocation)


def test_journal_replay_rejects_one_sided_pre_dispatch_authorization_evidence(
    tmp_path: Path,
) -> None:
    api, authority, signer, path = _authority(tmp_path)
    _reserve(authority)
    authority.record_intent(
        operation_id="intent-op",
        parent_id="parent-1",
        call_id="call-1",
        cost=3,
        now=START,
    )
    with sqlite3.connect(path) as connection:
        connection.row_factory = sqlite3.Row
        row = connection.execute(
            "SELECT * FROM source_quota_operation WHERE operation_id = 'intent-op'"
        ).fetchone()
        assert row is not None
        result = json.loads(
            connection.execute(
                "SELECT result_json FROM source_quota_operation WHERE operation_id = 'intent-op'"
            ).fetchone()[0]
        )
        result["call"]["authorized_at"] = START.isoformat()
        for allocation in result["parent"]["calls"]:
            allocation["authorized_at"] = START.isoformat()
        result_json = json.dumps(result, ensure_ascii=True, separators=(",", ":"), sort_keys=True)
        integrity_hash = api._replay_payload_hash(result_json)
        integrity_signature = signer.sign(
            api._replay_payload_signing_bytes(
                authority_id="test-authority",
                operation_id=row["operation_id"],
                effect_key=row["effect_key"],
                operation=row["operation"],
                payload_hash=row["payload_hash"],
                result_hash=row["result_hash"],
                result_integrity_hash=integrity_hash,
                key_id=signer.key_id,
            )
        )
        connection.execute(
            """
            UPDATE source_quota_operation
            SET result_json = ?, result_integrity_hash = ?, result_integrity_signature = ?
            WHERE operation_id = 'intent-op'
            """,
            (result_json, integrity_hash, integrity_signature),
        )
        connection.commit()

    with pytest.raises(api.SourceQuotaAuthorityIntegrityError, match="malformed"):
        authority.record_intent(
            operation_id="intent-op",
            parent_id="parent-1",
            call_id="call-1",
            cost=3,
            now=START,
        )


@pytest.mark.parametrize(
    ("evidence_path", "replacement"),
    [
        (("parent", "lease_id"), "b" * 64),
        (("parent", "reserved_at"), START + timedelta(seconds=4)),
        (("parent", "calls", 0, "intended_at"), START + timedelta(milliseconds=500)),
        (("parent", "calls", 0, "authorized_at"), START + timedelta(seconds=4)),
        (("parent", "calls", 0, "usage_id"), "c" * 64),
        (("call", "intended_at"), START + timedelta(milliseconds=500)),
        (("call", "authorized_at"), START + timedelta(seconds=4)),
        (("call", "usage_id"), "c" * 64),
    ],
)
def test_journal_replay_rejects_resigned_tampering_of_durable_immutable_evidence(
    tmp_path: Path,
    evidence_path: tuple[str | int, ...],
    replacement: str | datetime,
) -> None:
    api, authority, signer, path = _authority(tmp_path)
    _reserve(authority)
    authority.record_intent(
        operation_id="intent-op",
        parent_id="parent-1",
        call_id="call-1",
        cost=3,
        now=START,
    )
    authority.authorize_dispatch(
        operation_id="dispatch-op",
        parent_id="parent-1",
        call_id="call-1",
        now=START + timedelta(seconds=1),
    )
    with sqlite3.connect(path) as connection:
        result = json.loads(
            connection.execute(
                "SELECT result_json FROM source_quota_operation WHERE operation_id = 'dispatch-op'"
            ).fetchone()[0]
        )
    target: object = result
    for component in evidence_path[:-1]:
        target = target[component]  # type: ignore[index]
    target[evidence_path[-1]] = (  # type: ignore[index]
        replacement.isoformat() if isinstance(replacement, datetime) else replacement
    )
    _replace_signed_result_json(
        api,
        signer,
        path,
        operation_id="dispatch-op",
        result=result,
    )

    with pytest.raises(api.SourceQuotaAuthorityIntegrityError, match="immutable evidence"):
        _standalone(api, path, authority_id="test-authority", signer=signer).authorize_dispatch(
            operation_id="dispatch-op",
            parent_id="parent-1",
            call_id="call-1",
            now=START + timedelta(seconds=2),
        )


def test_legacy_operation_journal_is_backfilled_transactionally_and_replays_after_reopen(
    tmp_path: Path,
) -> None:
    api, authority, signer, path = _authority(tmp_path)
    first = _reserve(authority)
    _downgrade_operation_journal_to_legacy(path)

    reopened = _standalone(api, path, authority_id="test-authority", signer=signer)

    assert _reserve(reopened) == first
    with sqlite3.connect(path) as connection:
        row = connection.execute(
            """
            SELECT result_integrity_hash, result_integrity_signature
            FROM source_quota_operation WHERE operation_id = 'reserve-op'
            """
        ).fetchone()
        assert row is not None
        assert all(isinstance(value, str) and value for value in row)
        assert connection.execute("PRAGMA user_version").fetchone() == (3,)


def test_invalid_legacy_operation_journal_rolls_back_entire_backfill(tmp_path: Path) -> None:
    api, authority, signer, path = _authority(tmp_path)
    _reserve(authority)
    authority.record_intent(
        operation_id="intent-op",
        parent_id="parent-1",
        call_id="call-1",
        cost=3,
        now=START,
    )
    _downgrade_operation_journal_to_legacy(path)
    with sqlite3.connect(path) as connection:
        receipt = json.loads(
            connection.execute(
                "SELECT receipt_json FROM source_quota_operation WHERE operation_id = 'intent-op'"
            ).fetchone()[0]
        )
        receipt["signature"] = "tampered"
        connection.execute(
            "UPDATE source_quota_operation SET receipt_json = ? WHERE operation_id = 'intent-op'",
            (json.dumps(receipt, ensure_ascii=True, separators=(",", ":"), sort_keys=True),),
        )
        connection.commit()

    with pytest.raises(api.SourceQuotaAuthorityIntegrityError, match="signature"):
        _standalone(api, path, authority_id="test-authority", signer=signer)

    with sqlite3.connect(path) as connection:
        columns = {
            row[1] for row in connection.execute("PRAGMA table_info(source_quota_operation)")
        }
        assert "result_integrity_hash" not in columns
        assert "result_integrity_signature" not in columns
        assert connection.execute("PRAGMA user_version").fetchone() == (3,)


def test_legacy_backfill_signer_failure_rolls_back_schema_change(tmp_path: Path) -> None:
    api, authority, signer, path = _authority(tmp_path)
    _reserve(authority)
    _downgrade_operation_journal_to_legacy(path)
    signer.fail = True

    with pytest.raises(RuntimeError, match="signer unavailable"):
        _standalone(api, path, authority_id="test-authority", signer=signer)

    with sqlite3.connect(path) as connection:
        columns = {
            row[1] for row in connection.execute("PRAGMA table_info(source_quota_operation)")
        }
        assert "result_integrity_hash" not in columns
        assert "result_integrity_signature" not in columns
        assert connection.execute("PRAGMA user_version").fetchone() == (3,)


@pytest.mark.parametrize(
    ("state", "authorized_at", "finalized_at"),
    [
        ("DISPATCH_AUTHORIZED", START - timedelta(seconds=1), None),
        ("SUCCESS", START + timedelta(seconds=2), START + timedelta(seconds=1)),
        ("CANCELLED_BEFORE_DISPATCH", None, START - timedelta(seconds=1)),
    ],
)
def test_call_allocation_rejects_non_monotonic_lifecycle_timestamps(
    state: str,
    authorized_at: datetime | None,
    finalized_at: datetime | None,
) -> None:
    api = _api()

    with pytest.raises(ValueError, match="timestamp order"):
        api.SourceQuotaCallAllocation(
            call_id="call-1",
            parent_id="parent-1",
            cost=1,
            state=state,
            outcome="SUCCESS" if state == "SUCCESS" else None,
            intended_at=START,
            authorized_at=authorized_at,
            finalized_at=finalized_at,
            usage_id="a" * 64 if authorized_at is not None else None,
        )


def test_authorize_dispatch_rejects_timestamp_before_intent_without_consuming_quota(
    tmp_path: Path,
) -> None:
    api, authority, _signer, path = _authority(tmp_path)
    _reserve(authority)
    authority.record_intent(
        operation_id="intent-op",
        parent_id="parent-1",
        call_id="call-1",
        cost=3,
        now=START + timedelta(seconds=1),
    )

    with pytest.raises(ValueError, match="timestamp order"):
        authority.authorize_dispatch(
            operation_id="dispatch-op",
            parent_id="parent-1",
            call_id="call-1",
            now=START,
        )

    snapshot = authority.get_call("call-1")
    assert snapshot is not None
    assert snapshot.state is api.SourceQuotaCallState.INTENT
    with sqlite3.connect(path) as connection:
        assert connection.execute("SELECT COUNT(*) FROM quota_usage").fetchone() == (0,)


def test_operation_id_reuse_for_different_operation_kind_conflicts_without_state_change(
    tmp_path: Path,
) -> None:
    _api, authority, _signer, path = _authority(tmp_path)
    reserved = _reserve(authority, operation_id="shared-operation")
    before_parent = authority.get_parent("parent-1")
    before_remaining = SourceQuotaStore(path).remaining("source", now=START)

    with pytest.raises(SourceQuotaConflictError, match="operation_id"):
        authority.record_intent(
            operation_id="shared-operation",
            parent_id="parent-1",
            call_id="call-1",
            cost=3,
            now=START,
        )

    assert authority.get_parent("parent-1") == before_parent == reserved.parent
    assert authority.get_call("call-1") is None
    assert SourceQuotaStore(path).remaining("source", now=START) == before_remaining


def test_concurrent_dispatch_and_finalize_release_race_are_durable(tmp_path: Path) -> None:
    api, authority, signer, path = _authority(tmp_path)
    _reserve(authority)
    authority.record_intent(
        operation_id="intent-op",
        parent_id="parent-1",
        call_id="call-1",
        cost=3,
        now=START,
    )
    first = _standalone(api, path, authority_id="test-authority", signer=signer)
    second = _standalone(api, path, authority_id="test-authority", signer=signer)
    dispatch_barrier = Barrier(2)

    def dispatch(instance: object) -> object:
        dispatch_barrier.wait()
        return instance.authorize_dispatch(
            operation_id="dispatch-op",
            parent_id="parent-1",
            call_id="call-1",
            now=START,
        )

    with ThreadPoolExecutor(max_workers=2) as executor:
        left = executor.submit(dispatch, first)
        right = executor.submit(dispatch, second)
        assert left.result(timeout=10) == right.result(timeout=10)
    with sqlite3.connect(path) as connection:
        assert connection.execute("SELECT COUNT(*) FROM quota_usage").fetchone() == (1,)

    finalize_barrier = Barrier(2)

    def finalize() -> object:
        finalize_barrier.wait()
        return first.finalize(
            operation_id="finalize-op",
            parent_id="parent-1",
            call_id="call-1",
            outcome=api.SourceQuotaCallOutcome.SUCCESS,
            now=START,
        )

    def release() -> object:
        finalize_barrier.wait()
        return second.release_unused(
            operation_id="release-op",
            parent_id="parent-1",
            now=START,
        )

    with ThreadPoolExecutor(max_workers=2) as executor:
        final = executor.submit(finalize)
        release = executor.submit(release)
        assert final.result(timeout=10).call.state is api.SourceQuotaCallState.SUCCESS
        try:
            release.result(timeout=10)
        except SourceQuotaConflictError:
            second.release_unused(
                operation_id="release-after-finalize",
                parent_id="parent-1",
                now=START + timedelta(seconds=1),
            )
    reopened = _standalone(api, path, authority_id="test-authority", signer=signer)
    snapshot = reopened.get_parent("parent-1")
    assert snapshot is not None
    assert snapshot.state is api.SourceQuotaParentState.COMPENSATED
    assert snapshot.consumed_cost + snapshot.unused_released == snapshot.reserved_cost


@pytest.mark.parametrize(
    ("outcome", "durable_state", "durable_outcome"),
    [
        ("SUCCESS", "FAILURE", "FAILURE"),
        ("FAILURE", "UNKNOWN", "UNKNOWN"),
        ("UNKNOWN", "SUCCESS", "SUCCESS"),
    ],
)
def test_p1_finalize_replay_rejects_terminal_state_or_outcome_mutation(
    tmp_path: Path,
    outcome: str,
    durable_state: str,
    durable_outcome: str,
) -> None:
    api, authority, _signer, path = _authority(tmp_path)
    _finalize_call(api, authority, outcome=outcome)

    with sqlite3.connect(path) as connection:
        connection.execute(
            "UPDATE source_call_allocation SET state = ?, outcome = ? WHERE call_id = 'call-1'",
            (durable_state, durable_outcome),
        )
        connection.commit()

    with pytest.raises(api.SourceQuotaAuthorityIntegrityError):
        authority.finalize(
            operation_id="finalize-op",
            parent_id="parent-1",
            call_id="call-1",
            outcome=api.SourceQuotaCallOutcome(outcome),
            now=START + timedelta(seconds=20),
        )


@pytest.mark.parametrize(
    ("statement", "parameters"),
    [
        (
            "UPDATE source_call_allocation SET cost = ? WHERE call_id = 'call-1'",
            (4,),
        ),
        (
            "UPDATE source_call_allocation SET finalized_at = ? WHERE call_id = 'call-1'",
            ((START + timedelta(seconds=4)).isoformat(),),
        ),
        (
            "UPDATE quota_usage SET consumed_at = ? WHERE usage_id = ("
            "SELECT usage_id FROM source_call_allocation WHERE call_id = 'call-1')",
            ((START + timedelta(seconds=4)).isoformat(),),
        ),
    ],
    ids=["cost", "terminal-time", "usage-time"],
)
def test_p1_finalize_replay_rejects_terminal_cost_time_and_usage_mutation(
    tmp_path: Path,
    statement: str,
    parameters: tuple[object, ...],
) -> None:
    api, authority, _signer, path = _authority(tmp_path)
    _finalize_call(api, authority, outcome="SUCCESS")

    with sqlite3.connect(path) as connection:
        connection.execute(statement, parameters)
        connection.commit()

    with pytest.raises(api.SourceQuotaAuthorityIntegrityError):
        authority.finalize(
            operation_id="finalize-op",
            parent_id="parent-1",
            call_id="call-1",
            outcome=api.SourceQuotaCallOutcome.SUCCESS,
            now=START + timedelta(seconds=20),
        )


@pytest.mark.parametrize(
    ("total_cost", "cost", "expected_state", "durable_state"),
    [
        (3, 3, "CLOSED", "COMPENSATED"),
        (7, 3, "COMPENSATED", "CLOSED"),
    ],
)
def test_p1_release_replay_rejects_closed_compensated_state_swap(
    tmp_path: Path,
    total_cost: int,
    cost: int,
    expected_state: str,
    durable_state: str,
) -> None:
    api, authority, _signer, path = _authority(tmp_path)
    released = _release_terminal_parent(api, authority, total_cost=total_cost, cost=cost)
    assert released.parent.state.value == expected_state

    with sqlite3.connect(path) as connection:
        connection.execute(
            "UPDATE source_parent_reservation SET state = ? WHERE parent_id = 'parent-1'",
            (durable_state,),
        )
        connection.commit()

    with pytest.raises(api.SourceQuotaAuthorityIntegrityError):
        authority.release_unused(
            operation_id="release-op",
            parent_id="parent-1",
            now=START + timedelta(seconds=20),
        )


@pytest.mark.parametrize(
    ("statement", "parameters"),
    [
        (
            "UPDATE quota_lease SET used_units = ? WHERE lease_id = ("
            "SELECT lease_id FROM source_parent_reservation WHERE parent_id = 'parent-1')",
            (2,),
        ),
        (
            "UPDATE quota_lease SET released_at = ? WHERE lease_id = ("
            "SELECT lease_id FROM source_parent_reservation WHERE parent_id = 'parent-1')",
            ((START + timedelta(seconds=4)).isoformat(),),
        ),
    ],
    ids=["used-units", "released-at"],
)
def test_p1_release_replay_rejects_lease_ledger_mutation(
    tmp_path: Path,
    statement: str,
    parameters: tuple[object, ...],
) -> None:
    api, authority, _signer, path = _authority(tmp_path)
    _release_terminal_parent(api, authority, total_cost=7, cost=3)

    with sqlite3.connect(path) as connection:
        connection.execute(statement, parameters)
        connection.commit()

    with pytest.raises(api.SourceQuotaAuthorityIntegrityError):
        authority.release_unused(
            operation_id="release-op",
            parent_id="parent-1",
            now=START + timedelta(seconds=20),
        )


@pytest.mark.parametrize("mutation", ["delete", "extra"])
def test_p1_release_replay_requires_exact_usage_set(tmp_path: Path, mutation: str) -> None:
    api, authority, _signer, path = _authority(tmp_path)
    _release_terminal_parent(api, authority, total_cost=7, cost=3)

    with sqlite3.connect(path) as connection:
        if mutation == "delete":
            connection.execute(
                "DELETE FROM quota_usage WHERE usage_id = ("
                "SELECT usage_id FROM source_call_allocation WHERE call_id = 'call-1')"
            )
        else:
            lease_id = connection.execute(
                "SELECT lease_id FROM source_parent_reservation WHERE parent_id = 'parent-1'"
            ).fetchone()[0]
            connection.execute(
                "INSERT INTO quota_usage(usage_id, lease_id, units, consumed_at) "
                "VALUES (?, ?, ?, ?)",
                ("e" * 64, lease_id, 1, START.isoformat()),
            )
        connection.commit()

    with pytest.raises(api.SourceQuotaAuthorityIntegrityError):
        authority.release_unused(
            operation_id="release-op",
            parent_id="parent-1",
            now=START + timedelta(seconds=20),
        )


@pytest.mark.parametrize("mutation", ["extra-terminal-call", "nonterminal-call"])
def test_p1_release_replay_requires_exact_terminal_call_set(
    tmp_path: Path,
    mutation: str,
) -> None:
    api, authority, _signer, path = _authority(tmp_path)
    _release_terminal_parent(api, authority, total_cost=7, cost=3)

    with sqlite3.connect(path) as connection:
        if mutation == "extra-terminal-call":
            connection.execute(
                """
                INSERT INTO source_call_allocation(
                    call_id, parent_id, cost, state, outcome, intended_at,
                    authorized_at, finalized_at, usage_id
                ) VALUES (?, ?, ?, 'CANCELLED_BEFORE_DISPATCH', NULL, ?, NULL, ?, NULL)
                """,
                ("extra-call", "parent-1", 1, START.isoformat(), START.isoformat()),
            )
        else:
            connection.execute(
                "UPDATE source_call_allocation SET state = 'DISPATCH_AUTHORIZED', outcome = NULL "
                "WHERE call_id = 'call-1'"
            )
        connection.commit()

    with pytest.raises(api.SourceQuotaAuthorityIntegrityError):
        authority.release_unused(
            operation_id="release-op",
            parent_id="parent-1",
            now=START + timedelta(seconds=20),
        )


@pytest.mark.parametrize(
    ("total_cost", "cost", "terminal_state"),
    [(7, 3, "COMPENSATED"), (3, 3, "CLOSED")],
)
def test_p1_historical_nonterminal_receipts_replay_after_legal_forward_progress(
    tmp_path: Path,
    total_cost: int,
    cost: int,
    terminal_state: str,
) -> None:
    api, authority, signer, path = _authority(tmp_path)
    reserved = _reserve(authority, total_cost=total_cost)
    intended = authority.record_intent(
        operation_id="intent-op",
        parent_id="parent-1",
        call_id="call-1",
        cost=cost,
        now=START,
    )
    authorized = authority.authorize_dispatch(
        operation_id="dispatch-op",
        parent_id="parent-1",
        call_id="call-1",
        now=START + timedelta(seconds=1),
    )
    finalized = authority.finalize(
        operation_id="finalize-op",
        parent_id="parent-1",
        call_id="call-1",
        outcome=api.SourceQuotaCallOutcome.SUCCESS,
        now=START + timedelta(seconds=2),
    )
    released = authority.release_unused(
        operation_id="release-op",
        parent_id="parent-1",
        now=START + timedelta(seconds=3),
    )
    assert released.parent.state.value == terminal_state

    replay = _standalone(api, path, authority_id="test-authority", signer=signer)
    assert _reserve(replay, total_cost=total_cost) == reserved
    assert (
        replay.record_intent(
            operation_id="intent-op",
            parent_id="parent-1",
            call_id="call-1",
            cost=cost,
            now=START + timedelta(seconds=20),
        )
        == intended
    )
    assert (
        replay.authorize_dispatch(
            operation_id="dispatch-op",
            parent_id="parent-1",
            call_id="call-1",
            now=START + timedelta(seconds=20),
        )
        == authorized
    )
    assert (
        replay.finalize(
            operation_id="finalize-op",
            parent_id="parent-1",
            call_id="call-1",
            outcome=api.SourceQuotaCallOutcome.SUCCESS,
            now=START + timedelta(seconds=20),
        )
        == finalized
    )


@pytest.mark.parametrize("durable_state", ["INTENT", "CANCELLED_BEFORE_DISPATCH"])
def test_p1_authorize_replay_rejects_call_regression_and_bypass(
    tmp_path: Path,
    durable_state: str,
) -> None:
    api, authority, _signer, path = _authority(tmp_path)
    _reserve(authority)
    authority.record_intent(
        operation_id="intent-op",
        parent_id="parent-1",
        call_id="call-1",
        cost=3,
        now=START,
    )
    authority.authorize_dispatch(
        operation_id="dispatch-op",
        parent_id="parent-1",
        call_id="call-1",
        now=START + timedelta(seconds=1),
    )

    with sqlite3.connect(path) as connection:
        connection.execute(
            "UPDATE source_call_allocation SET state = ?, outcome = NULL, finalized_at = NULL "
            "WHERE call_id = 'call-1'",
            (durable_state,),
        )
        connection.commit()

    with pytest.raises(api.SourceQuotaAuthorityIntegrityError):
        authority.authorize_dispatch(
            operation_id="dispatch-op",
            parent_id="parent-1",
            call_id="call-1",
            now=START + timedelta(seconds=20),
        )


def test_p1_historical_replay_rejects_durable_parent_closing(tmp_path: Path) -> None:
    api, authority, _signer, path = _authority(tmp_path)
    _reserve(authority)

    with sqlite3.connect(path) as connection:
        connection.execute(
            "UPDATE source_parent_reservation SET state = 'CLOSING' WHERE parent_id = 'parent-1'"
        )
        connection.commit()

    with pytest.raises(api.SourceQuotaAuthorityIntegrityError):
        _reserve(authority)


@pytest.mark.parametrize(
    ("operation_id", "terminal_shape"),
    [
        ("cancel-op", "unknown-before-dispatch"),
        ("unknown-op", "cancel"),
    ],
)
def test_p1_pre_dispatch_terminal_replay_requires_exact_outcome_shape(
    tmp_path: Path,
    operation_id: str,
    terminal_shape: str,
) -> None:
    api, authority, _signer, path = _authority(tmp_path)
    _reserve(authority)
    authority.record_intent(
        operation_id="intent-op",
        parent_id="parent-1",
        call_id="call-1",
        cost=3,
        now=START,
    )
    if terminal_shape == "unknown-before-dispatch":
        authority.cancel(
            operation_id=operation_id,
            parent_id="parent-1",
            call_id="call-1",
            now=START + timedelta(seconds=1),
        )
        durable_outcome: str | None = "UNKNOWN_BEFORE_DISPATCH"
    else:
        authority.terminalize_unknown_before_dispatch(
            operation_id=operation_id,
            parent_id="parent-1",
            call_id="call-1",
            now=START + timedelta(seconds=1),
        )
        durable_outcome = None
    with sqlite3.connect(path) as connection:
        connection.execute(
            "UPDATE source_call_allocation SET outcome = ? WHERE call_id = 'call-1'",
            (durable_outcome,),
        )
        connection.commit()

    with pytest.raises(api.SourceQuotaAuthorityIntegrityError):
        if terminal_shape == "unknown-before-dispatch":
            authority.cancel(
                operation_id=operation_id,
                parent_id="parent-1",
                call_id="call-1",
                now=START + timedelta(seconds=20),
            )
        else:
            authority.terminalize_unknown_before_dispatch(
                operation_id=operation_id,
                parent_id="parent-1",
                call_id="call-1",
                now=START + timedelta(seconds=20),
            )


def test_p1_journal_finalize_shape_rejects_pre_dispatch_terminal_call(tmp_path: Path) -> None:
    api, authority, signer, path = _authority(tmp_path)
    _finalize_call(api, authority, outcome="SUCCESS")
    with sqlite3.connect(path) as connection:
        result = json.loads(
            connection.execute(
                "SELECT result_json FROM source_quota_operation WHERE operation_id = 'finalize-op'"
            ).fetchone()[0]
        )
    for allocation in [result["call"], *result["parent"]["calls"]]:
        allocation["state"] = "CANCELLED_BEFORE_DISPATCH"
        allocation["outcome"] = "UNKNOWN_BEFORE_DISPATCH"
        allocation["authorized_at"] = None
        allocation["usage_id"] = None
    _replace_fully_signed_result_json(
        api,
        signer,
        path,
        operation_id="finalize-op",
        result=result,
    )

    with pytest.raises(api.SourceQuotaAuthorityIntegrityError):
        _replay_saved_operation(authority, "finalize-op")


@pytest.mark.parametrize(
    ("operation_id", "terminal_shape"),
    [
        ("cancel-op", "unknown-before-dispatch"),
        ("unknown-op", "cancel"),
    ],
)
def test_p1_journal_pre_dispatch_operation_shape_is_exact(
    tmp_path: Path,
    operation_id: str,
    terminal_shape: str,
) -> None:
    api, authority, signer, path = _authority(tmp_path)
    _reserve(authority)
    authority.record_intent(
        operation_id="intent-op",
        parent_id="parent-1",
        call_id="call-1",
        cost=3,
        now=START,
    )
    if terminal_shape == "unknown-before-dispatch":
        authority.cancel(
            operation_id=operation_id,
            parent_id="parent-1",
            call_id="call-1",
            now=START + timedelta(seconds=1),
        )
        rewritten_outcome = "UNKNOWN_BEFORE_DISPATCH"
    else:
        authority.terminalize_unknown_before_dispatch(
            operation_id=operation_id,
            parent_id="parent-1",
            call_id="call-1",
            now=START + timedelta(seconds=1),
        )
        rewritten_outcome = None
    with sqlite3.connect(path) as connection:
        result = json.loads(
            connection.execute(
                "SELECT result_json FROM source_quota_operation WHERE operation_id = ?",
                (operation_id,),
            ).fetchone()[0]
        )
    for allocation in [result["call"], *result["parent"]["calls"]]:
        allocation["outcome"] = rewritten_outcome
    _replace_fully_signed_result_json(
        api,
        signer,
        path,
        operation_id=operation_id,
        result=result,
    )

    with pytest.raises(api.SourceQuotaAuthorityIntegrityError):
        _replay_saved_operation(authority, operation_id)


def test_p1_journal_release_shape_requires_closed_or_compensated(tmp_path: Path) -> None:
    api, authority, signer, path = _authority(tmp_path)
    _release_terminal_parent(api, authority, total_cost=7, cost=3)
    with sqlite3.connect(path) as connection:
        result = json.loads(
            connection.execute(
                "SELECT result_json FROM source_quota_operation WHERE operation_id = 'release-op'"
            ).fetchone()[0]
        )
    result["parent"]["state"] = "OPEN"
    result["parent"]["closed_at"] = None
    result["parent"]["unused_released"] = 0
    _replace_fully_signed_result_json(
        api,
        signer,
        path,
        operation_id="release-op",
        result=result,
    )

    with pytest.raises(api.SourceQuotaAuthorityIntegrityError):
        _replay_saved_operation(authority, "release-op")


@pytest.mark.parametrize("replay_operation", ["reserve", "intent", "authorize", "finalize"])
@pytest.mark.parametrize(
    "mutation",
    ["used-units", "released-at", "extra-usage", "usage-units", "usage-lease-id"],
)
def test_p1_historical_replay_reconciles_current_terminal_ledger(
    tmp_path: Path,
    replay_operation: str,
    mutation: str,
) -> None:
    api, authority, _signer, path = _authority(tmp_path)
    _release_terminal_parent(api, authority, total_cost=7, cost=3)

    with sqlite3.connect(path) as connection:
        lease_id = connection.execute(
            "SELECT lease_id FROM source_parent_reservation WHERE parent_id = 'parent-1'"
        ).fetchone()[0]
        if mutation == "used-units":
            connection.execute(
                "UPDATE quota_lease SET used_units = ? WHERE lease_id = ?",
                (2, lease_id),
            )
        elif mutation == "released-at":
            connection.execute(
                "UPDATE quota_lease SET released_at = ? WHERE lease_id = ?",
                ((START + timedelta(seconds=4)).isoformat(), lease_id),
            )
        else:
            if mutation == "extra-usage":
                connection.execute(
                    "INSERT INTO quota_usage(usage_id, lease_id, units, consumed_at) "
                    "VALUES (?, ?, ?, ?)",
                    ("e" * 64, lease_id, 1, START.isoformat()),
                )
            elif mutation == "usage-units":
                connection.execute(
                    "UPDATE quota_usage SET units = ? WHERE usage_id = ("
                    "SELECT usage_id FROM source_call_allocation WHERE call_id = 'call-1')",
                    (2,),
                )
            else:
                connection.execute(
                    "UPDATE quota_usage SET lease_id = ? WHERE usage_id = ("
                    "SELECT usage_id FROM source_call_allocation WHERE call_id = 'call-1')",
                    ("f" * 64,),
                )
        connection.commit()

    with pytest.raises(api.SourceQuotaAuthorityIntegrityError):
        if replay_operation == "reserve":
            _reserve(authority)
        elif replay_operation == "intent":
            authority.record_intent(
                operation_id="intent-op",
                parent_id="parent-1",
                call_id="call-1",
                cost=3,
                now=START + timedelta(seconds=20),
            )
        elif replay_operation == "authorize":
            authority.authorize_dispatch(
                operation_id="dispatch-op",
                parent_id="parent-1",
                call_id="call-1",
                now=START + timedelta(seconds=20),
            )
        else:
            authority.finalize(
                operation_id="finalize-op",
                parent_id="parent-1",
                call_id="call-1",
                outcome=api.SourceQuotaCallOutcome.SUCCESS,
                now=START + timedelta(seconds=20),
            )


@pytest.mark.parametrize("operation_id", ["reserve-op", "release-op"])
def test_p1_terminal_replay_wraps_malformed_durable_extra_call_as_integrity_error(
    tmp_path: Path,
    operation_id: str,
) -> None:
    api, authority, _signer, path = _authority(tmp_path)
    _release_terminal_parent(api, authority, total_cost=7, cost=3)

    with sqlite3.connect(path) as connection:
        connection.execute("PRAGMA ignore_check_constraints = ON")
        connection.execute(
            """
            INSERT INTO source_call_allocation(
                call_id, parent_id, cost, state, outcome, intended_at,
                authorized_at, finalized_at, usage_id
            ) VALUES (?, ?, ?, ?, NULL, ?, NULL, NULL, NULL)
            """,
            ("malformed-call", "parent-1", 1, "MALFORMED", START.isoformat()),
        )
        connection.commit()

    with pytest.raises(api.SourceQuotaAuthorityIntegrityError):
        _replay_saved_operation(authority, operation_id)


@pytest.mark.parametrize(
    ("operation_id", "terminalization"),
    [
        ("finalize-op", "finalize"),
        ("cancel-op", "cancel"),
        ("unknown-op", "unknown"),
    ],
)
def test_p1_signed_nonrelease_terminal_result_rejects_duplicate_sibling_call(
    tmp_path: Path,
    operation_id: str,
    terminalization: str,
) -> None:
    api, authority, signer, path = _authority(tmp_path)
    _reserve(authority)
    authority.record_intent(
        operation_id="sibling-intent-op",
        parent_id="parent-1",
        call_id="sibling-call",
        cost=1,
        now=START,
    )
    authority.record_intent(
        operation_id="target-intent-op",
        parent_id="parent-1",
        call_id="target-call",
        cost=3,
        now=START + timedelta(seconds=1),
    )
    if terminalization == "finalize":
        authority.authorize_dispatch(
            operation_id="target-authorize-op",
            parent_id="parent-1",
            call_id="target-call",
            now=START + timedelta(seconds=2),
        )
        authority.finalize(
            operation_id=operation_id,
            parent_id="parent-1",
            call_id="target-call",
            outcome=api.SourceQuotaCallOutcome.SUCCESS,
            now=START + timedelta(seconds=3),
        )
    elif terminalization == "cancel":
        authority.cancel(
            operation_id=operation_id,
            parent_id="parent-1",
            call_id="target-call",
            now=START + timedelta(seconds=2),
        )
    else:
        authority.terminalize_unknown_before_dispatch(
            operation_id=operation_id,
            parent_id="parent-1",
            call_id="target-call",
            now=START + timedelta(seconds=2),
        )

    with sqlite3.connect(path) as connection:
        result = json.loads(
            connection.execute(
                "SELECT result_json FROM source_quota_operation WHERE operation_id = ?",
                (operation_id,),
            ).fetchone()[0]
        )
    sibling = next(
        allocation
        for allocation in result["parent"]["calls"]
        if allocation["call_id"] == "sibling-call"
    )
    result["parent"]["calls"].append(sibling.copy())
    _replace_fully_signed_result_json(
        api,
        signer,
        path,
        operation_id=operation_id,
        result=result,
    )

    with pytest.raises(api.SourceQuotaAuthorityIntegrityError):
        _replay_saved_operation(authority, operation_id)


def test_release_replay_requires_signed_terminal_calls_to_match_durable_calls(
    tmp_path: Path,
) -> None:
    api, authority, signer, path = _authority(tmp_path)
    _release_terminal_parent(api, authority, total_cost=7, cost=3)

    with sqlite3.connect(path) as connection:
        result = json.loads(
            connection.execute(
                "SELECT result_json FROM source_quota_operation WHERE operation_id = 'release-op'"
            ).fetchone()[0]
        )
    signed_call = result["parent"]["calls"][0]
    signed_call.update(
        {
            "state": "INTENT",
            "outcome": None,
            "authorized_at": None,
            "finalized_at": None,
            "usage_id": None,
        }
    )
    _replace_fully_signed_result_json(
        api,
        signer,
        path,
        operation_id="release-op",
        result=result,
    )

    with pytest.raises(api.SourceQuotaAuthorityIntegrityError):
        _replay_saved_operation(authority, "release-op")


@pytest.mark.parametrize(
    ("replay_operation", "seconds_before_parent"),
    [
        ("reserve", 1),
        ("finalize", 1),
        ("reserve", 0),
        ("finalize", 0),
    ],
)
def test_historical_replay_rejects_durable_terminal_times_mismatching_signed_journal(
    tmp_path: Path,
    replay_operation: str,
    seconds_before_parent: int,
) -> None:
    api, authority, _signer, path = _authority(tmp_path)
    _reserve(authority)
    authority.record_intent(
        operation_id="first-intent-op",
        parent_id="parent-1",
        call_id="first-call",
        cost=3,
        now=START + timedelta(seconds=1),
    )
    authority.authorize_dispatch(
        operation_id="first-dispatch-op",
        parent_id="parent-1",
        call_id="first-call",
        now=START + timedelta(seconds=2),
    )
    authority.finalize(
        operation_id="first-finalize-op",
        parent_id="parent-1",
        call_id="first-call",
        outcome=api.SourceQuotaCallOutcome.SUCCESS,
        now=START + timedelta(seconds=3),
    )
    authority.record_intent(
        operation_id="later-intent-op",
        parent_id="parent-1",
        call_id="later-call",
        cost=1,
        now=START + timedelta(seconds=4),
    )
    authority.authorize_dispatch(
        operation_id="later-dispatch-op",
        parent_id="parent-1",
        call_id="later-call",
        now=START + timedelta(seconds=5),
    )
    authority.finalize(
        operation_id="later-finalize-op",
        parent_id="parent-1",
        call_id="later-call",
        outcome=api.SourceQuotaCallOutcome.SUCCESS,
        now=START + timedelta(seconds=6),
    )
    authority.release_unused(
        operation_id="release-op",
        parent_id="parent-1",
        now=START + timedelta(seconds=7),
    )

    durable_time = START - timedelta(seconds=seconds_before_parent)
    durable_timestamp = durable_time.isoformat(timespec="microseconds")
    with sqlite3.connect(path) as connection:
        connection.execute(
            """
            UPDATE source_call_allocation
            SET intended_at = ?, authorized_at = ?, finalized_at = ?
            WHERE call_id = 'later-call'
            """,
            (durable_timestamp,) * 3,
        )
        connection.execute(
            """
            UPDATE quota_usage SET consumed_at = ?
            WHERE usage_id = (
                SELECT usage_id FROM source_call_allocation WHERE call_id = 'later-call'
            )
            """,
            (durable_timestamp,),
        )
        connection.commit()

    with pytest.raises(api.SourceQuotaAuthorityIntegrityError):
        if replay_operation == "reserve":
            _reserve(authority)
        else:
            authority.finalize(
                operation_id="first-finalize-op",
                parent_id="parent-1",
                call_id="first-call",
                outcome=api.SourceQuotaCallOutcome.SUCCESS,
                now=START + timedelta(seconds=20),
            )


@pytest.mark.parametrize(
    ("statement", "parameters"),
    [
        (
            "UPDATE source_parent_reservation SET total_cost = ? WHERE parent_id = 'parent-1'",
            ("not-an-int",),
        ),
        (
            "UPDATE quota_lease SET units = ? WHERE lease_id = ("
            "SELECT lease_id FROM source_parent_reservation WHERE parent_id = 'parent-1')",
            ("not-an-int",),
        ),
        (
            "UPDATE quota_window SET total_units = ? "
            "WHERE source = 'source' AND window_id = 'window'",
            ("not-an-int",),
        ),
        (
            "UPDATE source_call_allocation SET cost = ? WHERE call_id = 'call-1'",
            ("not-an-int",),
        ),
    ],
    ids=["parent-total-cost", "lease-units", "window-units", "call-cost"],
)
def test_historical_terminal_replay_wraps_malformed_durable_scalars_as_integrity_error(
    tmp_path: Path,
    statement: str,
    parameters: tuple[object, ...],
) -> None:
    api, authority, _signer, path = _authority(tmp_path)
    _release_terminal_parent(api, authority, total_cost=7, cost=3)

    with sqlite3.connect(path) as connection:
        connection.execute(statement, parameters)
        connection.commit()

    with pytest.raises(api.SourceQuotaAuthorityIntegrityError):
        _reserve(authority)


@pytest.mark.parametrize("stored_time", [None, 1], ids=["null", "non-string"])
def test_parse_stored_time_wraps_non_string_values_as_integrity_error(stored_time: object) -> None:
    api = _api()

    with pytest.raises(api.SourceQuotaAuthorityIntegrityError):
        api._parse_stored_time(stored_time)


@pytest.mark.parametrize(
    "statement",
    [
        "UPDATE source_parent_reservation SET total_cost = ? WHERE parent_id = 'parent-1'",
        "UPDATE quota_lease SET units = ? WHERE lease_id = ("
        "SELECT lease_id FROM source_parent_reservation WHERE parent_id = 'parent-1')",
        "UPDATE quota_lease SET used_units = ? WHERE lease_id = ("
        "SELECT lease_id FROM source_parent_reservation WHERE parent_id = 'parent-1')",
        "UPDATE quota_window SET total_units = ? WHERE source = 'source' AND window_id = 'window'",
        "UPDATE source_call_allocation SET cost = ? WHERE call_id = 'call-1'",
        "UPDATE quota_usage SET units = ? WHERE usage_id = ("
        "SELECT usage_id FROM source_call_allocation WHERE call_id = 'call-1')",
    ],
    ids=[
        "parent-total-cost",
        "lease-units",
        "lease-used-units",
        "window-capacity",
        "call-cost",
        "usage-units",
    ],
)
@pytest.mark.parametrize("stored_value", [1.5, "1.5"], ids=["real", "numeric-text"])
def test_historical_replay_rejects_non_integer_quota_storage_values(
    tmp_path: Path,
    statement: str,
    stored_value: object,
) -> None:
    api, authority, _signer, path = _authority(tmp_path, total_units=1)
    _release_terminal_parent(api, authority, total_cost=1, cost=1)

    with sqlite3.connect(path) as connection:
        connection.execute(statement, (stored_value,))
        stored_type = connection.execute(
            f"SELECT typeof({statement.split(' SET ')[1].split(' = ?')[0]}) "
            f"FROM {statement.split('UPDATE ')[1].split(' SET')[0]} LIMIT 1"
        ).fetchone()[0]
        assert stored_type == "real"
        connection.commit()

    with pytest.raises(api.SourceQuotaAuthorityIntegrityError):
        _reserve(authority, total_cost=1)


@pytest.mark.parametrize("invalid_value", [True, 7.0, "7"], ids=["bool", "real", "text"])
def test_quota_request_inputs_reject_non_integer_values(
    tmp_path: Path,
    invalid_value: object,
) -> None:
    _api, authority, _signer, _path = _authority(tmp_path, total_units=10)

    with pytest.raises(ValueError):
        _reserve(authority, total_cost=invalid_value)  # type: ignore[arg-type]


@pytest.mark.parametrize("invalid_value", [True, 7.0, "7"], ids=["bool", "real", "text"])
def test_call_cost_input_rejects_non_integer_values(tmp_path: Path, invalid_value: object) -> None:
    _api, authority, _signer, _path = _authority(tmp_path, total_units=10)
    _reserve(authority)

    with pytest.raises(ValueError):
        authority.record_intent(
            operation_id="intent-op",
            parent_id="parent-1",
            call_id="call-1",
            cost=invalid_value,
            now=START,
        )


def test_reservation_rejects_non_integer_unrelated_active_lease_units(tmp_path: Path) -> None:
    _api, authority, _signer, path = _authority(tmp_path, total_units=10)
    with sqlite3.connect(path) as connection:
        connection.execute(
            """
            INSERT INTO quota_lease(
                lease_id, source, window_id, owner, units, used_units,
                granted_at, expires_at, quota_reset_at, released_at
            ) VALUES (?, 'source', 'window', 'other-owner', ?, 0, ?, ?, ?, NULL)
            """,
            (
                "a" * 64,
                4.5,
                START.isoformat(),
                (START + timedelta(seconds=30)).isoformat(),
                END.isoformat(),
            ),
        )
        assert connection.execute(
            "SELECT typeof(units) FROM quota_lease WHERE lease_id = ?", ("a" * 64,)
        ).fetchone() == ("real",)
        connection.commit()

    with pytest.raises(_api.SourceQuotaAuthorityIntegrityError):
        _reserve(authority, total_cost=6)


@pytest.mark.parametrize("state", ["DISPATCH_AUTHORIZED", "SUCCESS"])
def test_historical_intent_replay_rejects_forward_progression_after_lease_expiry(
    tmp_path: Path,
    state: str,
) -> None:
    api, authority, _signer, path = _authority(tmp_path)
    _reserve(authority)
    intended = authority.record_intent(
        operation_id="intent-op",
        parent_id="parent-1",
        call_id="call-1",
        cost=3,
        now=START,
    )
    authority.authorize_dispatch(
        operation_id="dispatch-op",
        parent_id="parent-1",
        call_id="call-1",
        now=START + timedelta(seconds=1),
    )
    if state == "SUCCESS":
        authority.finalize(
            operation_id="finalize-op",
            parent_id="parent-1",
            call_id="call-1",
            outcome=api.SourceQuotaCallOutcome.SUCCESS,
            now=START + timedelta(seconds=2),
        )

    expired_time = (START + timedelta(seconds=40)).isoformat(timespec="microseconds")
    with sqlite3.connect(path) as connection:
        connection.execute(
            "UPDATE source_call_allocation SET authorized_at = ?, finalized_at = ? "
            "WHERE call_id = 'call-1'",
            (expired_time, expired_time if state == "SUCCESS" else None),
        )
        connection.execute(
            "UPDATE quota_usage SET consumed_at = ? WHERE usage_id = ("
            "SELECT usage_id FROM source_call_allocation WHERE call_id = 'call-1')",
            (expired_time,),
        )
        connection.commit()

    with pytest.raises(api.SourceQuotaAuthorityIntegrityError, match="open ledger conflicts"):
        authority.record_intent(
            operation_id="intent-op",
            parent_id="parent-1",
            call_id="call-1",
            cost=3,
            now=START + timedelta(seconds=50),
        )
    assert intended.call is not None


@pytest.mark.parametrize("state", ["DISPATCH_AUTHORIZED", "SUCCESS"])
@pytest.mark.parametrize(
    "mutation",
    ["missing-usage", "wrong-usage-lease", "usage-units", "lease-used-units"],
)
def test_historical_intent_replay_rejects_tampered_open_forward_progression(
    tmp_path: Path,
    state: str,
    mutation: str,
) -> None:
    api, authority, _signer, path = _authority(tmp_path)
    _reserve(authority)
    authority.record_intent(
        operation_id="intent-op",
        parent_id="parent-1",
        call_id="call-1",
        cost=3,
        now=START,
    )
    authority.authorize_dispatch(
        operation_id="dispatch-op",
        parent_id="parent-1",
        call_id="call-1",
        now=START + timedelta(seconds=1),
    )
    if state == "SUCCESS":
        authority.finalize(
            operation_id="finalize-op",
            parent_id="parent-1",
            call_id="call-1",
            outcome=api.SourceQuotaCallOutcome.SUCCESS,
            now=START + timedelta(seconds=2),
        )

    with sqlite3.connect(path) as connection:
        lease_id = connection.execute(
            "SELECT lease_id FROM source_parent_reservation WHERE parent_id = 'parent-1'"
        ).fetchone()[0]
        if mutation == "missing-usage":
            connection.execute(
                "DELETE FROM quota_usage WHERE usage_id = ("
                "SELECT usage_id FROM source_call_allocation WHERE call_id = 'call-1')"
            )
        elif mutation == "wrong-usage-lease":
            connection.execute(
                "UPDATE quota_usage SET lease_id = ? WHERE usage_id = ("
                "SELECT usage_id FROM source_call_allocation WHERE call_id = 'call-1')",
                ("f" * 64,),
            )
        elif mutation == "usage-units":
            connection.execute(
                "UPDATE quota_usage SET units = 2 WHERE usage_id = ("
                "SELECT usage_id FROM source_call_allocation WHERE call_id = 'call-1')"
            )
        else:
            connection.execute(
                "UPDATE quota_lease SET used_units = 2 WHERE lease_id = ?",
                (lease_id,),
            )
        connection.commit()

    with pytest.raises(api.SourceQuotaAuthorityIntegrityError):
        authority.record_intent(
            operation_id="intent-op",
            parent_id="parent-1",
            call_id="call-1",
            cost=3,
            now=START + timedelta(seconds=20),
        )


@pytest.mark.parametrize("state", ["DISPATCH_AUTHORIZED", "SUCCESS"])
def test_historical_intent_replay_accepts_legal_open_forward_progression(
    tmp_path: Path,
    state: str,
) -> None:
    api, authority, signer, path = _authority(tmp_path)
    _reserve(authority)
    intended = authority.record_intent(
        operation_id="intent-op",
        parent_id="parent-1",
        call_id="call-1",
        cost=3,
        now=START,
    )
    authority.authorize_dispatch(
        operation_id="dispatch-op",
        parent_id="parent-1",
        call_id="call-1",
        now=START + timedelta(seconds=1),
    )
    if state == "SUCCESS":
        authority.finalize(
            operation_id="finalize-op",
            parent_id="parent-1",
            call_id="call-1",
            outcome=api.SourceQuotaCallOutcome.SUCCESS,
            now=START + timedelta(seconds=2),
        )

    replay = _standalone(api, path, authority_id="test-authority", signer=signer)
    assert (
        replay.record_intent(
            operation_id="intent-op",
            parent_id="parent-1",
            call_id="call-1",
            cost=3,
            now=START + timedelta(seconds=20),
        )
        == intended
    )


def test_historical_reserve_replay_rejects_terminal_parent_closed_before_reservation(
    tmp_path: Path,
) -> None:
    api, authority, _signer, path = _authority(tmp_path)
    _reserve(authority)
    premature_close = (START - timedelta(seconds=1)).isoformat(timespec="microseconds")
    with sqlite3.connect(path) as connection:
        connection.execute(
            "UPDATE source_parent_reservation SET state = 'COMPENSATED', closed_at = ? "
            "WHERE parent_id = 'parent-1'",
            (premature_close,),
        )
        connection.execute(
            "UPDATE quota_lease SET released_at = ? WHERE lease_id = ("
            "SELECT lease_id FROM source_parent_reservation WHERE parent_id = 'parent-1')",
            (premature_close,),
        )
        connection.commit()

    with pytest.raises(
        api.SourceQuotaAuthorityIntegrityError,
        match="terminal ledger lifecycle conflicts",
    ):
        _reserve(authority)


def test_release_rejects_foreign_usage_without_committing_unreplayable_receipt(
    tmp_path: Path,
) -> None:
    api, authority, _signer, path = _authority(tmp_path)
    reserved = _reserve(authority)
    SourceQuotaStore(path).consume(
        reserved.parent.lease_id,
        usage_id="foreign-usage",
        units=2,
        now=START + timedelta(seconds=1),
    )

    with pytest.raises(api.SourceQuotaAuthorityIntegrityError, match="usage set conflicts"):
        authority.release_unused(
            operation_id="release-op",
            parent_id="parent-1",
            now=START + timedelta(seconds=2),
        )

    parent = authority.get_parent("parent-1")
    assert parent is not None
    assert parent.state is api.SourceQuotaParentState.OPEN
    with sqlite3.connect(path) as connection:
        assert connection.execute(
            "SELECT COUNT(*) FROM source_quota_operation WHERE operation_id = 'release-op'"
        ).fetchone() == (0,)


def test_release_rejects_naive_granted_time_without_committing_terminal_effect(
    tmp_path: Path,
) -> None:
    api, authority, _signer, path = _authority(tmp_path)
    _reserve(authority)
    with sqlite3.connect(path) as connection:
        connection.execute(
            "UPDATE quota_lease SET granted_at = ? WHERE lease_id = ("
            "SELECT lease_id FROM source_parent_reservation WHERE parent_id = 'parent-1')",
            (START.replace(tzinfo=None).isoformat(timespec="microseconds"),),
        )
        connection.commit()

    with pytest.raises(api.SourceQuotaAuthorityIntegrityError, match="stored quota timestamp"):
        authority.release_unused(
            operation_id="release-op",
            parent_id="parent-1",
            now=START + timedelta(seconds=1),
        )

    parent = authority.get_parent("parent-1")
    assert parent is not None
    assert parent.state is api.SourceQuotaParentState.OPEN


def test_get_parent_uses_one_read_generation_during_concurrent_delete(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    api, authority, _signer, path = _authority(tmp_path)
    reserved = _reserve(authority)
    original_parent_row = authority._parent_row
    calls = 0

    def delete_after_first_parent_read(
        connection: sqlite3.Connection,
        parent_id: str,
    ) -> sqlite3.Row | None:
        nonlocal calls
        row = original_parent_row(connection, parent_id)
        calls += 1
        if calls == 1:
            with sqlite3.connect(path) as writer:
                writer.execute(
                    "DELETE FROM source_parent_reservation WHERE parent_id = ?",
                    (parent_id,),
                )
                writer.commit()
        return row

    monkeypatch.setattr(authority, "_parent_row", delete_after_first_parent_read)

    assert authority.get_parent("parent-1") == reserved.parent


def _rewrite_signed_journal_effect_key(
    api: ModuleType,
    signer: _Signer,
    path: Path,
    *,
    operation_id: str,
    effect_key: str,
) -> None:
    """Alter a journal identity while retaining every existing signature binding."""

    with sqlite3.connect(path) as connection:
        connection.row_factory = sqlite3.Row
        row = connection.execute(
            "SELECT * FROM source_quota_operation WHERE operation_id = ?", (operation_id,)
        ).fetchone()
        assert row is not None
        receipt = api.SourceQuotaOperationReceipt.model_validate_json(row["receipt_json"])
        rewritten_receipt = receipt.model_copy(
            update={"effect_key": effect_key, "signature": "pending"}
        )
        rewritten_receipt = rewritten_receipt.model_copy(
            update={"signature": signer.sign(rewritten_receipt.signing_bytes())}
        )
        integrity_signature = signer.sign(
            api._replay_payload_signing_bytes(
                authority_id="test-authority",
                operation_id=row["operation_id"],
                effect_key=effect_key,
                operation=row["operation"],
                payload_hash=row["payload_hash"],
                result_hash=row["result_hash"],
                result_integrity_hash=row["result_integrity_hash"],
                key_id=signer.key_id,
            )
        )
        connection.execute(
            """
            UPDATE source_quota_operation
            SET effect_key = ?, receipt_json = ?, result_integrity_signature = ?
            WHERE operation_id = ?
            """,
            (
                effect_key,
                rewritten_receipt.model_dump_json(),
                integrity_signature,
                operation_id,
            ),
        )
        connection.commit()


@pytest.mark.parametrize("replay_operation", ["reserve", "intent"])
def test_p1_historical_replay_requires_signed_chain_for_forged_success_durable_state(
    tmp_path: Path,
    replay_operation: str,
) -> None:
    api, authority, _signer, path = _authority(tmp_path)
    reserved = _reserve(authority)
    authority.record_intent(
        operation_id="intent-op",
        parent_id="parent-1",
        call_id="call-1",
        cost=3,
        now=START,
    )

    with sqlite3.connect(path) as connection:
        usage_id = "f" * 64
        authorized_at = (START + timedelta(seconds=1)).isoformat(timespec="microseconds")
        finalized_at = (START + timedelta(seconds=2)).isoformat(timespec="microseconds")
        connection.execute(
            """
            UPDATE source_call_allocation
            SET state = 'SUCCESS', outcome = 'SUCCESS', authorized_at = ?,
                finalized_at = ?, usage_id = ?
            WHERE call_id = 'call-1'
            """,
            (authorized_at, finalized_at, usage_id),
        )
        connection.execute(
            "UPDATE quota_lease SET used_units = 3 WHERE lease_id = ?",
            (reserved.parent.lease_id,),
        )
        connection.execute(
            "INSERT INTO quota_usage(usage_id, lease_id, units, consumed_at) VALUES (?, ?, 3, ?)",
            (usage_id, reserved.parent.lease_id, authorized_at),
        )
        connection.commit()

    with pytest.raises(api.SourceQuotaAuthorityIntegrityError, match="journal chain"):
        if replay_operation == "reserve":
            _reserve(authority)
        else:
            authority.record_intent(
                operation_id="intent-op",
                parent_id="parent-1",
                call_id="call-1",
                cost=3,
                now=START + timedelta(seconds=20),
            )


def test_p1_reserve_replay_requires_signed_chain_for_forged_compensated_durable_state(
    tmp_path: Path,
) -> None:
    api, authority, _signer, path = _authority(tmp_path)
    reserved = _reserve(authority)
    closed_at = (START + timedelta(seconds=1)).isoformat()
    with sqlite3.connect(path) as connection:
        connection.execute(
            "UPDATE source_parent_reservation SET state = 'COMPENSATED', closed_at = ? "
            "WHERE parent_id = 'parent-1'",
            (closed_at,),
        )
        connection.execute(
            "UPDATE quota_lease SET released_at = ? WHERE lease_id = ?",
            (closed_at, reserved.parent.lease_id),
        )
        connection.commit()

    with pytest.raises(api.SourceQuotaAuthorityIntegrityError, match="journal chain"):
        _reserve(authority)


@pytest.mark.parametrize(
    ("missing_operation", "replay_operation"),
    [
        ("dispatch-op", "intent-op"),
        ("finalize-op", "dispatch-op"),
        ("release-op", "finalize-op"),
    ],
    ids=["authorize", "finalize", "release"],
)
def test_p1_historical_replay_rejects_missing_signed_chain_operation(
    tmp_path: Path,
    missing_operation: str,
    replay_operation: str,
) -> None:
    api, authority, _signer, path = _authority(tmp_path)
    _release_terminal_parent(api, authority, total_cost=7, cost=3)
    with sqlite3.connect(path) as connection:
        connection.execute(
            "DELETE FROM source_quota_operation WHERE operation_id = ?", (missing_operation,)
        )
        connection.commit()

    with pytest.raises(api.SourceQuotaAuthorityIntegrityError, match="journal chain"):
        _replay_saved_operation(authority, replay_operation)


@pytest.mark.parametrize("signature_field", ["receipt", "result-binding"])
def test_p1_historical_replay_validates_every_chain_signature(
    tmp_path: Path,
    signature_field: str,
) -> None:
    api, authority, _signer, path = _authority(tmp_path)
    _release_terminal_parent(api, authority, total_cost=7, cost=3)
    with sqlite3.connect(path) as connection:
        if signature_field == "receipt":
            receipt = json.loads(
                connection.execute(
                    "SELECT receipt_json FROM source_quota_operation "
                    "WHERE operation_id = 'release-op'"
                ).fetchone()[0]
            )
            receipt["signature"] = "tampered"
            connection.execute(
                "UPDATE source_quota_operation SET receipt_json = ? "
                "WHERE operation_id = 'release-op'",
                (json.dumps(receipt, ensure_ascii=True, separators=(",", ":"), sort_keys=True),),
            )
        else:
            connection.execute(
                "UPDATE source_quota_operation SET result_integrity_signature = 'tampered' "
                "WHERE operation_id = 'release-op'"
            )
        connection.commit()

    with pytest.raises(api.SourceQuotaAuthorityIntegrityError, match="signature"):
        _replay_saved_operation(authority, "reserve-op")


@pytest.mark.parametrize("mismatch", ["call", "usage", "parent", "lease"])
def test_p1_historical_replay_rejects_signed_chain_content_mismatching_durable_evidence(
    tmp_path: Path,
    mismatch: str,
) -> None:
    api, authority, signer, path = _authority(tmp_path)
    _release_terminal_parent(api, authority, total_cost=7, cost=3)
    with sqlite3.connect(path) as connection:
        result = json.loads(
            connection.execute(
                "SELECT result_json FROM source_quota_operation WHERE operation_id = 'release-op'"
            ).fetchone()[0]
        )
    if mismatch == "call":
        result["parent"]["calls"][0]["cost"] = 4
    elif mismatch == "usage":
        result["parent"]["calls"][0]["usage_id"] = "a" * 64
    elif mismatch == "parent":
        result["parent"]["owner"] = "foreign-owner"
    else:
        result["parent"]["lease_id"] = "a" * 64
    _replace_fully_signed_result_json(api, signer, path, operation_id="release-op", result=result)

    with pytest.raises(api.SourceQuotaAuthorityIntegrityError):
        _replay_saved_operation(authority, "reserve-op")


@pytest.mark.parametrize(
    ("operation_id", "effect_key"),
    [
        ("intent-op", "record-intent:foreign-parent:call-1"),
        ("dispatch-op", "authorize-dispatch:parent-1:call-1:duplicate"),
    ],
    ids=["cross-parent", "duplicate-effect"],
)
def test_p1_historical_replay_rejects_foreign_or_conflicting_signed_effect(
    tmp_path: Path,
    operation_id: str,
    effect_key: str,
) -> None:
    api, authority, signer, path = _authority(tmp_path)
    _release_terminal_parent(api, authority, total_cost=7, cost=3)
    _rewrite_signed_journal_effect_key(
        api, signer, path, operation_id=operation_id, effect_key=effect_key
    )

    with pytest.raises(api.SourceQuotaAuthorityIntegrityError, match="journal chain"):
        _replay_saved_operation(authority, "reserve-op")


def test_p1_historical_replay_rejects_signed_chain_operation_order_conflict(
    tmp_path: Path,
) -> None:
    api, authority, _signer, path = _authority(tmp_path)
    _release_terminal_parent(api, authority, total_cost=7, cost=3)
    with sqlite3.connect(path) as connection:
        connection.execute(
            "UPDATE source_quota_operation SET rowid = 100 WHERE operation_id = 'reserve-op'"
        )
        connection.commit()

    with pytest.raises(api.SourceQuotaAuthorityIntegrityError, match="journal chain"):
        _replay_saved_operation(authority, "intent-op")


@pytest.mark.parametrize("field", ["request_hash", "operation", "effect_key"])
def test_p1_public_replay_rejects_tampered_target_identity_before_caller_conflict(
    tmp_path: Path,
    field: str,
) -> None:
    api, authority, signer, path = _authority(tmp_path)
    _reserve(authority)
    if field == "effect_key":
        _rewrite_signed_journal_effect_key(
            api,
            signer,
            path,
            operation_id="reserve-op",
            effect_key="reserve-parent:foreign-parent",
        )
    else:
        replacement = "0" * 64 if field == "request_hash" else "record_intent"
        with sqlite3.connect(path) as connection:
            connection.execute(
                f"UPDATE source_quota_operation SET {field} = ? WHERE operation_id = 'reserve-op'",
                (replacement,),
            )
            connection.commit()

    with pytest.raises(api.SourceQuotaAuthorityIntegrityError):
        _reserve(authority)


def test_p1_public_replay_preserves_caller_payload_conflict_for_healthy_journal(
    tmp_path: Path,
) -> None:
    api, authority, _signer, _path = _authority(tmp_path)
    _reserve(authority)

    with pytest.raises(
        api.SourceQuotaAuthorityConflictError, match="operation_id payload conflicts"
    ):
        _reserve(authority, total_cost=6)


@pytest.mark.parametrize(
    "column",
    [
        "effect_key",
        "operation",
        "payload_hash",
        "request_hash",
        "result_hash",
        "result_json",
        "result_integrity_hash",
        "result_integrity_signature",
        "receipt_json",
    ],
)
def test_p1_public_replay_normalizes_blob_journal_scalars_to_integrity_error(
    tmp_path: Path,
    column: str,
) -> None:
    api, authority, _signer, path = _authority(tmp_path)
    _reserve(authority)
    value = b"x" * 64 if column.endswith("hash") else b"{}"
    with sqlite3.connect(path) as connection:
        connection.execute(
            f"UPDATE source_quota_operation SET {column} = ? WHERE operation_id = 'reserve-op'",
            (sqlite3.Binary(value),),
        )
        connection.commit()

    with pytest.raises(api.SourceQuotaAuthorityIntegrityError):
        _reserve(authority)


def test_p1_public_replay_normalizes_verifier_exception_to_integrity_error(tmp_path: Path) -> None:
    api, authority, _signer, path = _authority(tmp_path)
    _reserve(authority)
    replay = _standalone(
        api,
        path,
        authority_id="test-authority",
        signer=_VerifyRaisingSigner(),
    )

    with pytest.raises(api.SourceQuotaAuthorityIntegrityError):
        _reserve(replay)


@pytest.mark.parametrize(
    "replacement",
    [
        "foreign-reserve-op",
        sqlite3.Binary(b"reserve-op"),
    ],
    ids=["foreign-text", "blob"],
)
def test_p1_public_effect_collision_rejects_tampered_persisted_operation_id(
    tmp_path: Path,
    replacement: object,
) -> None:
    api, authority, _signer, path = _authority(tmp_path)
    _reserve(authority)
    with sqlite3.connect(path) as connection:
        connection.execute(
            "UPDATE source_quota_operation SET operation_id = ? WHERE operation_id = 'reserve-op'",
            (replacement,),
        )
        connection.commit()

    with pytest.raises(api.SourceQuotaAuthorityIntegrityError):
        _reserve(authority)


def test_p1_public_effect_collision_preserves_conflict_for_healthy_new_operation_id(
    tmp_path: Path,
) -> None:
    api, authority, _signer, _path = _authority(tmp_path)
    _reserve(authority)

    with pytest.raises(api.SourceQuotaAuthorityConflictError, match="effect key"):
        authority.reserve_parent(
            operation_id="new-reserve-op",
            parent_id="parent-1",
            source="source",
            owner="owner-1",
            total_cost=7,
            now=START,
            expires_at=START + timedelta(seconds=30),
        )


def test_p1_journal_materializes_signed_global_and_parent_checkpoints(tmp_path: Path) -> None:
    _api_module, authority, _signer, path = _authority(tmp_path)
    _reserve(authority)
    authority.record_intent(
        operation_id="intent-op",
        parent_id="parent-1",
        call_id="call-1",
        cost=3,
        now=START + timedelta(seconds=1),
    )

    with sqlite3.connect(path) as connection:
        connection.row_factory = sqlite3.Row
        journal = connection.execute(
            """
            SELECT parent_id, parent_ordinal, previous_operation_hash, operation_hash
            FROM source_quota_operation
            WHERE parent_id = 'parent-1'
            ORDER BY parent_ordinal
            """
        ).fetchall()
        global_checkpoint = connection.execute(
            "SELECT * FROM source_quota_global_checkpoint WHERE singleton = 1"
        ).fetchone()
        parent_checkpoint = connection.execute(
            "SELECT * FROM source_quota_parent_checkpoint WHERE parent_id = 'parent-1'"
        ).fetchone()
        indexes = {
            row[1] for row in connection.execute("PRAGMA index_list(source_quota_operation)")
        }

    assert [(row["parent_id"], row["parent_ordinal"]) for row in journal] == [
        ("parent-1", 1),
        ("parent-1", 2),
    ]
    assert journal[0]["previous_operation_hash"] == "0" * 64
    assert journal[1]["previous_operation_hash"] == journal[0]["operation_hash"]
    assert global_checkpoint is not None
    assert global_checkpoint["journal_count"] == 2
    assert global_checkpoint["clock_high_water"] == (START + timedelta(seconds=1)).isoformat(
        timespec="microseconds"
    )
    assert parent_checkpoint is not None
    assert parent_checkpoint["operation_count"] == 2
    assert parent_checkpoint["head_operation_hash"] == journal[-1]["operation_hash"]
    assert "source_quota_operation_parent_ordinal_uq" in indexes


def test_p1_hot_replay_has_no_global_history_or_clock_scan_after_scale(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _api_module, authority, _signer, path = _authority(tmp_path, total_units=256)
    for index in range(128):
        authority.reserve_parent(
            operation_id=f"reserve-{index}",
            parent_id=f"parent-{index}",
            source="source",
            owner=f"owner-{index}",
            total_cost=1,
            now=START,
            expires_at=START + timedelta(seconds=30),
        )

    statements: list[str] = []
    original_connect = authority._store._connect

    def traced_connect() -> sqlite3.Connection:
        connection = original_connect()
        connection.set_trace_callback(statements.append)
        return connection

    monkeypatch.setattr(authority._store, "_connect", traced_connect)
    replayed = authority.reserve_parent(
        operation_id="reserve-0",
        parent_id="parent-0",
        source="source",
        owner="owner-0",
        total_cost=1,
        now=START,
        expires_at=START + timedelta(seconds=30),
    )

    normalized = [" ".join(statement.lower().split()) for statement in statements]
    assert replayed.parent.parent_id == "parent-0"
    assert not any(
        "from source_quota_operation order by rowid" in statement for statement in normalized
    )
    assert not any("select max(authority_at)" in statement for statement in normalized)
    assert not any(
        "from source_parent_reservation union all" in statement for statement in normalized
    )
    with sqlite3.connect(path) as connection:
        query_plan = connection.execute(
            "EXPLAIN QUERY PLAN SELECT * FROM source_quota_operation "
            "WHERE parent_id = ? AND parent_ordinal = ?",
            ("parent-0", 1),
        ).fetchall()
    assert any("source_quota_operation_parent_ordinal_uq" in row[3] for row in query_plan)


def test_p1_parent_hot_queries_use_covering_indexes_with_large_call_usage_history(
    tmp_path: Path,
) -> None:
    api, authority, _signer, path = _authority(tmp_path, total_units=128)
    _reserve(authority, total_cost=64)
    for index in range(64):
        call_id = f"call-{index:03d}"
        authority.record_intent(
            operation_id=f"intent-{index:03d}",
            parent_id="parent-1",
            call_id=call_id,
            cost=1,
            now=START,
        )
    for index in range(64):
        call_id = f"call-{index:03d}"
        authority.authorize_dispatch(
            operation_id=f"authorize-{index:03d}",
            parent_id="parent-1",
            call_id=call_id,
            now=START + timedelta(seconds=1),
        )
    for index in range(64):
        call_id = f"call-{index:03d}"
        authority.finalize(
            operation_id=f"finalize-{index:03d}",
            parent_id="parent-1",
            call_id=call_id,
            outcome=api.SourceQuotaCallOutcome.SUCCESS,
            now=START + timedelta(seconds=2),
        )

    with sqlite3.connect(path) as connection:
        assert connection.execute(
            "SELECT COUNT(*) FROM source_call_allocation WHERE parent_id = ?",
            ("parent-1",),
        ).fetchone() == (64,)
        assert connection.execute(
            "SELECT COUNT(*) FROM quota_usage WHERE lease_id = ("
            "SELECT lease_id FROM source_parent_reservation WHERE parent_id = ?)",
            ("parent-1",),
        ).fetchone() == (64,)
        plans = {
            "sum": connection.execute(
                "EXPLAIN QUERY PLAN SELECT COALESCE(SUM(cost), 0) "
                "FROM source_call_allocation WHERE parent_id = ?",
                ("parent-1",),
            ).fetchall(),
            "unterminated": connection.execute(
                "EXPLAIN QUERY PLAN SELECT call_id FROM source_call_allocation "
                "WHERE parent_id = ? AND state IN (?, ?) LIMIT 1",
                ("parent-1", "INTENT", "DISPATCH_AUTHORIZED"),
            ).fetchall(),
            "snapshot": connection.execute(
                "EXPLAIN QUERY PLAN SELECT * FROM source_call_allocation "
                "WHERE parent_id = ? ORDER BY intended_at, call_id",
                ("parent-1",),
            ).fetchall(),
            "usage": connection.execute(
                "EXPLAIN QUERY PLAN SELECT * FROM quota_usage WHERE lease_id = ? ORDER BY usage_id",
                (authority.get_parent("parent-1").lease_id,),
            ).fetchall(),
        }

    details = {name: " | ".join(row[3] for row in rows) for name, rows in plans.items()}
    assert "source_call_allocation_parent_cost_idx" in details["sum"]
    assert "source_call_allocation_parent_state_call_idx" in details["unterminated"]
    assert "source_call_allocation_parent_time_call_idx" in details["snapshot"]
    assert "USE TEMP B-TREE" not in details["snapshot"]
    assert "quota_usage_lease_usage_idx" in details["usage"]
    assert "USE TEMP B-TREE" not in details["usage"]


def test_p1_explicit_full_audit_detects_old_parent_chain_tamper(tmp_path: Path) -> None:
    api, authority, _signer, path = _authority(tmp_path)
    _release_terminal_parent(api, authority, total_cost=7, cost=3)
    authority.audit()
    with sqlite3.connect(path) as connection:
        connection.execute("DELETE FROM source_quota_operation WHERE operation_id = 'dispatch-op'")
        connection.commit()

    with pytest.raises(api.SourceQuotaAuthorityIntegrityError, match="journal chain"):
        authority.audit()


def test_p1_startup_full_audits_when_journal_guard_inventory_is_incomplete(
    tmp_path: Path,
) -> None:
    api, authority, signer, path = _authority(tmp_path)
    _release_terminal_parent(api, authority, total_cost=7, cost=3)
    with sqlite3.connect(path) as connection:
        connection.execute("DROP TRIGGER source_quota_operation_guard_delete")
        connection.execute("DELETE FROM source_quota_operation WHERE operation_id = 'dispatch-op'")
        connection.commit()

    with pytest.raises(api.SourceQuotaAuthorityIntegrityError, match="journal chain"):
        _standalone(
            api,
            path,
            authority_id="test-authority",
            signer=signer,
        )


def test_p1_signed_global_checkpoint_cannot_be_silently_recreated_after_deletion(
    tmp_path: Path,
) -> None:
    api, _authority_instance, signer, path = _authority(tmp_path)
    with sqlite3.connect(path) as connection:
        connection.execute("DELETE FROM source_quota_global_checkpoint")
        connection.commit()

    with pytest.raises(api.SourceQuotaAuthorityIntegrityError, match="checkpoint is missing"):
        _standalone(
            api,
            path,
            authority_id="test-authority",
            signer=signer,
        )
