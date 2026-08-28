from __future__ import annotations

import hashlib
import json
import sqlite3
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from rquant.source_quota_authority import SourceQuotaParentAuthority
from rquant.source_quota_store import SourceQuotaStore

START = datetime(2026, 8, 7, 1, 30, tzinfo=UTC)


class _Signer:
    key_id = "adapter-test-key"

    def sign(self, payload: bytes) -> str:
        return hashlib.sha256(b"adapter-test" + payload).hexdigest()

    def verify(self, payload: bytes, signature: str) -> bool:
        return signature == self.sign(payload)


def _authority(tmp_path: Path) -> SourceQuotaParentAuthority:
    path = tmp_path / "quota.sqlite3"
    store = SourceQuotaStore(path)
    store.declare_window(
        source="tushare",
        window_id="2026-08-07",
        starts_at=START,
        resets_at=START + timedelta(minutes=10),
        total_units=20,
    )
    return SourceQuotaParentAuthority.for_nonproduction_standalone(
        path,
        authority_id="quota-authority",
        signer=_Signer(),
    )


def _binding(
    parent_id: str = "parent-a",
    *,
    generation: int = 3,
    fence: int = 8,
    owner: str = "lab-worker-a",
) -> object:
    from rquant.source_quota_broker_adapter import SourceQuotaParentBindingV2

    return SourceQuotaParentBindingV2(
        parent_id=parent_id,
        source="tushare",
        owner=owner,
        claim_binding_hash="a" * 64,
        claim_generation=generation,
        scheduler_fencing_token=fence,
    )


def _adapter(tmp_path: Path) -> object:
    from rquant.source_quota_broker_adapter import SourceQuotaBrokerAdapterV2

    return SourceQuotaBrokerAdapterV2(
        tmp_path / "adapter.sqlite3",
        authority=_authority(tmp_path),
        adapter_id="broker-quota-v2",
    )


def _reserve(adapter: object, binding: object, *, total_cost: int = 5) -> object:
    return adapter.reserve_parent(
        operation_id="reserve-a",
        binding=binding,
        total_cost=total_cost,
        now=START,
        expires_at=START + timedelta(minutes=2),
    )


def _intent(adapter: object, binding: object, *, cost: int = 3) -> object:
    return adapter.record_intent(
        operation_id="intent-a",
        binding=binding,
        call_id="call-a",
        cost=cost,
        now=START,
    )


def _authorized(adapter: object, binding: object) -> object:
    return adapter.authorize_dispatch(
        operation_id="authorize-a",
        binding=binding,
        call_id="call-a",
        now=START + timedelta(seconds=1),
    )


def test_v2_adapter_preserves_native_signed_receipts_through_full_lifecycle(
    tmp_path: Path,
) -> None:
    from rquant.source_quota_broker_adapter import (
        SourceQuotaBrokerAdapterV2,
        SourceQuotaParentBindingV2,
    )

    adapter = SourceQuotaBrokerAdapterV2(
        tmp_path / "adapter.sqlite3",
        authority=_authority(tmp_path),
        adapter_id="broker-quota-v2",
    )
    binding = SourceQuotaParentBindingV2(
        parent_id="parent-a",
        source="tushare",
        owner="lab-worker-a",
        claim_binding_hash="a" * 64,
        claim_generation=3,
        scheduler_fencing_token=8,
    )

    reserved = adapter.reserve_parent(
        operation_id="reserve-a",
        binding=binding,
        total_cost=5,
        now=START,
        expires_at=START + timedelta(minutes=2),
    )
    intent = adapter.record_intent(
        operation_id="intent-a",
        binding=binding,
        call_id="call-a",
        cost=3,
        now=START,
    )
    authorized = adapter.authorize_dispatch(
        operation_id="authorize-a",
        binding=binding,
        call_id="call-a",
        now=START + timedelta(seconds=1),
    )
    finalized = adapter.finalize(
        operation_id="finalize-a",
        binding=binding,
        call_id="call-a",
        outcome="SUCCESS",
        now=START + timedelta(seconds=2),
    )
    released = adapter.release_unused(
        operation_id="release-a",
        binding=binding,
        now=START + timedelta(seconds=3),
    )

    assert reserved.authority_result.receipt.operation_id
    assert intent.authority_result.call is not None
    assert authorized.authority_result.call is not None
    assert finalized.authority_result.call is not None
    assert released.authority_result.parent.consumed_cost == 3
    assert released.authority_result.parent.unused_released == 2
    assert released.authority_result.parent.state.value == "COMPENSATED"
    assert type(reserved.authority_result.receipt).__name__ == "SourceQuotaOperationReceipt"


def test_v2_adapter_replays_response_loss_after_restart_without_duplicate_quota_effect(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from rquant.source_quota_broker_adapter import (
        SourceQuotaBrokerAdapterV2,
        SourceQuotaParentBindingV2,
    )

    authority = _authority(tmp_path)
    binding = SourceQuotaParentBindingV2(
        parent_id="parent-a",
        source="tushare",
        owner="lab-worker-a",
        claim_binding_hash="a" * 64,
        claim_generation=1,
        scheduler_fencing_token=1,
    )
    path = tmp_path / "adapter.sqlite3"
    first = SourceQuotaBrokerAdapterV2(path, authority=authority, adapter_id="broker-quota-v2")
    monkeypatch.setattr(
        first,
        "_commit_response",
        lambda **_kwargs: (_ for _ in ()).throw(ConnectionError("mapping commit lost")),
    )
    with pytest.raises(ConnectionError, match="mapping commit lost"):
        first.reserve_parent(
            operation_id="reserve-a",
            binding=binding,
            total_cost=5,
            now=START,
            expires_at=START + timedelta(minutes=2),
        )

    restarted = SourceQuotaBrokerAdapterV2(path, authority=authority, adapter_id="broker-quota-v2")
    result = restarted.reserve_parent(
        operation_id="reserve-a",
        binding=binding,
        total_cost=5,
        now=START,
        expires_at=START + timedelta(minutes=2),
    )
    assert result.authority_result.parent.reserved_cost == 5
    assert authority.get_parent("parent-a") == result.authority_result.parent


@pytest.mark.parametrize(
    "phase",
    ("reserve", "intent", "authorize", "finalize", "unknown", "release"),
)
def test_each_v2_phase_recovers_exact_signed_native_result_after_response_loss(
    tmp_path: Path,
    phase: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from rquant.source_quota_broker_adapter import SourceQuotaBrokerAdapterV2

    authority = _authority(tmp_path)
    path = tmp_path / "adapter.sqlite3"
    binding = _binding()
    adapter = SourceQuotaBrokerAdapterV2(path, authority=authority, adapter_id="broker-quota-v2")
    operations: dict[str, tuple[str, Callable[[object], object]]] = {
        "reserve": (
            "reserve-a",
            lambda value: value.reserve_parent(
                operation_id="reserve-a",
                binding=binding,
                total_cost=5,
                now=START,
                expires_at=START + timedelta(minutes=2),
            ),
        ),
        "intent": (
            "intent-a",
            lambda value: value.record_intent(
                operation_id="intent-a",
                binding=binding,
                call_id="call-a",
                cost=3,
                now=START,
            ),
        ),
        "authorize": (
            "authorize-a",
            lambda value: value.authorize_dispatch(
                operation_id="authorize-a",
                binding=binding,
                call_id="call-a",
                now=START + timedelta(seconds=1),
            ),
        ),
        "finalize": (
            "finalize-a",
            lambda value: value.finalize(
                operation_id="finalize-a",
                binding=binding,
                call_id="call-a",
                outcome="SUCCESS",
                now=START + timedelta(seconds=2),
            ),
        ),
        "unknown": (
            "unknown-a",
            lambda value: value.unknown_before_dispatch(
                operation_id="unknown-a",
                binding=binding,
                call_id="call-a",
                now=START + timedelta(seconds=1),
            ),
        ),
        "release": (
            "release-a",
            lambda value: value.release_unused(
                operation_id="release-a",
                binding=binding,
                now=START + timedelta(seconds=3),
            ),
        ),
    }
    if phase != "reserve":
        _reserve(adapter, binding)
    if phase in {"authorize", "finalize", "unknown", "release"}:
        _intent(adapter, binding)
    if phase in {"finalize", "release"}:
        _authorized(adapter, binding)
    if phase == "release":
        adapter.finalize(
            operation_id="finalize-a",
            binding=binding,
            call_id="call-a",
            outcome="SUCCESS",
            now=START + timedelta(seconds=2),
        )

    operation_id, invoke = operations[phase]
    monkeypatch.setattr(
        adapter,
        "_commit_response",
        lambda **_kwargs: (_ for _ in ()).throw(ConnectionError("mapping commit lost")),
    )
    with pytest.raises(ConnectionError, match="mapping commit lost"):
        invoke(adapter)
    with sqlite3.connect(path) as connection:
        adapter_row = connection.execute(
            "SELECT response_json FROM source_quota_broker_adapter_operation "
            "WHERE operation_id = ?",
            (operation_id,),
        ).fetchone()
        assert adapter_row == (None,)
    with sqlite3.connect(authority.path) as connection:
        native_count = connection.execute(
            "SELECT COUNT(*) FROM source_quota_operation WHERE operation_id = ?",
            (operation_id,),
        ).fetchone()[0]
        assert native_count == 1
    restarted = SourceQuotaBrokerAdapterV2(path, authority=authority, adapter_id="broker-quota-v2")
    recovered = invoke(restarted)
    assert recovered.operation_id == operation_id
    assert recovered.authority_result.receipt.operation_id == operation_id


def _sqlite_backup(source: Path, destination: Path) -> None:
    with (
        sqlite3.connect(source) as source_connection,
        sqlite3.connect(destination) as destination_connection,
    ):
        source_connection.backup(destination_connection)


def test_old_adapter_snapshot_cannot_rebind_native_parent_to_an_old_fence(tmp_path: Path) -> None:
    from rquant.source_quota_broker_adapter import (
        SourceQuotaBrokerAdapterConflictError,
        SourceQuotaBrokerAdapterV2,
    )

    authority = _authority(tmp_path)
    adapter_path = tmp_path / "adapter.sqlite3"
    empty_snapshot = tmp_path / "adapter-empty-snapshot.sqlite3"
    adapter = SourceQuotaBrokerAdapterV2(
        adapter_path,
        authority=authority,
        adapter_id="broker-quota-v2",
    )
    _sqlite_backup(adapter_path, empty_snapshot)
    current = _binding(generation=4, fence=9)
    _reserve(adapter, current)
    _sqlite_backup(empty_snapshot, adapter_path)

    rolled_back = SourceQuotaBrokerAdapterV2(
        adapter_path,
        authority=authority,
        adapter_id="broker-quota-v2",
    )
    with pytest.raises(SourceQuotaBrokerAdapterConflictError):
        _reserve(rolled_back, _binding(generation=3, fence=8))
    parent = authority.get_parent("parent-a")
    assert parent is not None
    assert parent.claim_generation == 4
    assert parent.scheduler_fencing_token == 9


def test_donor_adapter_database_cannot_rebind_a_current_native_parent(tmp_path: Path) -> None:
    from rquant.source_quota_broker_adapter import (
        SourceQuotaBrokerAdapterConflictError,
        SourceQuotaBrokerAdapterV2,
    )

    current_authority = _authority(tmp_path / "current")
    donor_authority = _authority(tmp_path / "donor")
    current = SourceQuotaBrokerAdapterV2(
        tmp_path / "current-adapter.sqlite3",
        authority=current_authority,
        adapter_id="broker-quota-v2",
    )
    donor_path = tmp_path / "donor-adapter.sqlite3"
    donor = SourceQuotaBrokerAdapterV2(
        donor_path,
        authority=donor_authority,
        adapter_id="broker-quota-v2",
    )
    _reserve(current, _binding(generation=4, fence=9))
    _reserve(donor, _binding(generation=3, fence=8))

    rebound = SourceQuotaBrokerAdapterV2(
        donor_path,
        authority=current_authority,
        adapter_id="broker-quota-v2",
    )
    with pytest.raises(SourceQuotaBrokerAdapterConflictError):
        _reserve(rebound, _binding(generation=3, fence=8))


def test_donor_binding_is_rejected_before_it_can_commit_a_native_intent(tmp_path: Path) -> None:
    from rquant.source_quota_broker_adapter import (
        SourceQuotaBrokerAdapterConflictError,
        SourceQuotaBrokerAdapterV2,
    )

    current_authority = _authority(tmp_path / "current")
    donor_authority = _authority(tmp_path / "donor")
    current = SourceQuotaBrokerAdapterV2(
        tmp_path / "current-adapter.sqlite3",
        authority=current_authority,
        adapter_id="broker-quota-v2",
    )
    donor_path = tmp_path / "donor-adapter.sqlite3"
    donor = SourceQuotaBrokerAdapterV2(
        donor_path,
        authority=donor_authority,
        adapter_id="broker-quota-v2",
    )
    _reserve(current, _binding(generation=4, fence=9))
    _reserve(donor, _binding(generation=3, fence=8))

    rebound = SourceQuotaBrokerAdapterV2(
        donor_path,
        authority=current_authority,
        adapter_id="broker-quota-v2",
    )
    with pytest.raises(SourceQuotaBrokerAdapterConflictError):
        _intent(rebound, _binding(generation=3, fence=8))
    assert current_authority.get_call("call-a") is None


@pytest.mark.parametrize(
    ("native_phase", "tampered_phase"),
    (
        ("reserve", "release_unused"),
        ("intent", "finalize"),
        ("authorize", "record_intent"),
        ("finalize", "authorize_dispatch"),
        ("unknown", "finalize"),
        ("release", "reserve_parent"),
    ),
)
def test_decode_rejects_outer_phase_that_does_not_match_native_signed_operation(
    tmp_path: Path,
    native_phase: str,
    tampered_phase: str,
) -> None:
    from rquant.source_quota_broker_adapter import (
        SourceQuotaBrokerAdapterIntegrityError,
        SourceQuotaBrokerAdapterV2,
        decode_source_quota_broker_receipt_v2,
        encode_source_quota_broker_receipt_v2,
    )
    from rquant.strict_json import canonical_json_bytes

    authority = _authority(tmp_path)
    adapter = SourceQuotaBrokerAdapterV2(
        tmp_path / "adapter.sqlite3",
        authority=authority,
        adapter_id="broker-quota-v2",
    )
    binding = _binding()
    receipts: dict[str, object] = {"reserve": _reserve(adapter, binding)}
    if native_phase != "reserve":
        receipts["intent"] = _intent(adapter, binding)
    if native_phase in {"authorize", "finalize", "release"}:
        receipts["authorize"] = _authorized(adapter, binding)
    if native_phase == "finalize":
        receipts["finalize"] = adapter.finalize(
            operation_id="finalize-a",
            binding=binding,
            call_id="call-a",
            outcome="SUCCESS",
            now=START + timedelta(seconds=2),
        )
    if native_phase == "unknown":
        receipts["unknown"] = adapter.unknown_before_dispatch(
            operation_id="unknown-a",
            binding=binding,
            call_id="call-a",
            now=START + timedelta(seconds=1),
        )
    if native_phase == "release":
        adapter.finalize(
            operation_id="finalize-a",
            binding=binding,
            call_id="call-a",
            outcome="SUCCESS",
            now=START + timedelta(seconds=2),
        )
        receipts["release"] = adapter.release_unused(
            operation_id="release-a",
            binding=binding,
            now=START + timedelta(seconds=3),
        )
    payload = json.loads(encode_source_quota_broker_receipt_v2(receipts[native_phase]))
    payload["phase"] = tampered_phase

    with pytest.raises(SourceQuotaBrokerAdapterIntegrityError):
        decode_source_quota_broker_receipt_v2(canonical_json_bytes(payload))


def test_stale_generation_and_foreign_parent_are_rejected_before_a_call_is_consumed(
    tmp_path: Path,
) -> None:
    from rquant.source_quota_broker_adapter import SourceQuotaBrokerAdapterConflictError

    adapter = _adapter(tmp_path)
    binding = _binding()
    _reserve(adapter, binding)
    with pytest.raises(SourceQuotaBrokerAdapterConflictError, match="stale|foreign"):
        _intent(adapter, _binding(generation=4), cost=1)
    with pytest.raises(SourceQuotaBrokerAdapterConflictError, match="stale|foreign"):
        _intent(adapter, _binding(parent_id="parent-b"), cost=1)
    assert adapter._authority.get_parent("parent-a").consumed_cost == 0


def test_overspend_and_success_then_predispatch_compensation_fail_closed(tmp_path: Path) -> None:
    from rquant.source_quota_broker_adapter import SourceQuotaBrokerAdapterConflictError

    adapter = _adapter(tmp_path)
    binding = _binding()
    _reserve(adapter, binding, total_cost=3)
    with pytest.raises(SourceQuotaBrokerAdapterConflictError):
        _intent(adapter, binding, cost=4)
    adapter.record_intent(
        operation_id="intent-good",
        binding=binding,
        call_id="call-a",
        cost=3,
        now=START,
    )
    _authorized(adapter, binding)
    adapter.finalize(
        operation_id="finalize-a",
        binding=binding,
        call_id="call-a",
        outcome="SUCCESS",
        now=START + timedelta(seconds=2),
    )
    with pytest.raises(SourceQuotaBrokerAdapterConflictError):
        adapter.unknown_before_dispatch(
            operation_id="unknown-after-success",
            binding=binding,
            call_id="call-a",
            now=START + timedelta(seconds=3),
        )


def test_unknown_before_dispatch_releases_only_on_parent_close(tmp_path: Path) -> None:
    adapter = _adapter(tmp_path)
    binding = _binding()
    _reserve(adapter, binding)
    _intent(adapter, binding)
    unknown = adapter.unknown_before_dispatch(
        operation_id="unknown-a",
        binding=binding,
        call_id="call-a",
        now=START + timedelta(seconds=1),
    )
    assert unknown.authority_result.call is not None
    assert unknown.authority_result.call.outcome.value == "UNKNOWN_BEFORE_DISPATCH"
    assert unknown.authority_result.parent.consumed_cost == 0
    closed = adapter.release_unused(
        operation_id="release-a",
        binding=binding,
        now=START + timedelta(seconds=2),
    )
    assert closed.authority_result.parent.consumed_cost == 0
    assert closed.authority_result.parent.unused_released == 5
    assert closed.authority_result.parent.state.value == "COMPENSATED"


def test_concurrent_finalize_and_unknown_before_dispatch_allow_one_terminal_effect(
    tmp_path: Path,
) -> None:
    from rquant.source_quota_broker_adapter import SourceQuotaBrokerAdapterConflictError

    adapter = _adapter(tmp_path)
    binding = _binding()
    _reserve(adapter, binding)
    _intent(adapter, binding)
    _authorized(adapter, binding)
    with ThreadPoolExecutor(max_workers=2) as pool:
        success = pool.submit(
            adapter.finalize,
            operation_id="finalize-a",
            binding=binding,
            call_id="call-a",
            outcome="SUCCESS",
            now=START + timedelta(seconds=2),
        )
        unknown = pool.submit(
            adapter.unknown_before_dispatch,
            operation_id="unknown-a",
            binding=binding,
            call_id="call-a",
            now=START + timedelta(seconds=2),
        )
    results = [future.exception() for future in (success, unknown)]
    assert sum(value is None for value in results) == 1
    assert isinstance(
        next(value for value in results if value is not None),
        SourceQuotaBrokerAdapterConflictError,
    )
    call = adapter._authority.get_call("call-a")
    assert call is not None and call.state.value == "SUCCESS"


def test_concurrent_finalize_replays_one_native_effect_and_second_release_is_rejected(
    tmp_path: Path,
) -> None:
    from rquant.source_quota_broker_adapter import SourceQuotaBrokerAdapterConflictError

    adapter = _adapter(tmp_path)
    binding = _binding()
    _reserve(adapter, binding)
    _intent(adapter, binding)
    _authorized(adapter, binding)
    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [
            pool.submit(
                adapter.finalize,
                operation_id="finalize-a",
                binding=binding,
                call_id="call-a",
                outcome="SUCCESS",
                now=START + timedelta(seconds=2),
            )
            for _ in range(2)
        ]
    first, second = [future.result() for future in futures]
    assert first == second
    adapter.release_unused(
        operation_id="release-a",
        binding=binding,
        now=START + timedelta(seconds=3),
    )
    with pytest.raises(SourceQuotaBrokerAdapterConflictError):
        adapter.release_unused(
            operation_id="release-b",
            binding=binding,
            now=START + timedelta(seconds=4),
        )


def test_a_call_id_cannot_cross_from_one_valid_parent_to_another(tmp_path: Path) -> None:
    from rquant.source_quota_broker_adapter import SourceQuotaBrokerAdapterConflictError

    adapter = _adapter(tmp_path)
    parent_a = _binding("parent-a")
    parent_b = _binding("parent-b", owner="lab-worker-b")
    _reserve(adapter, parent_a)
    adapter.reserve_parent(
        operation_id="reserve-b",
        binding=parent_b,
        total_cost=5,
        now=START,
        expires_at=START + timedelta(minutes=2),
    )
    _intent(adapter, parent_a)
    with pytest.raises(SourceQuotaBrokerAdapterConflictError):
        adapter.record_intent(
            operation_id="intent-b",
            binding=parent_b,
            call_id="call-a",
            cost=1,
            now=START,
        )


def test_tampered_or_missing_authority_journal_cannot_be_recovered_as_a_v2_result(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from rquant.source_quota_broker_adapter import (
        SourceQuotaBrokerAdapterIntegrityError,
        SourceQuotaBrokerAdapterV2,
    )

    authority = _authority(tmp_path)
    path = tmp_path / "adapter.sqlite3"
    binding = _binding()
    adapter = SourceQuotaBrokerAdapterV2(path, authority=authority, adapter_id="broker-quota-v2")
    _reserve(adapter, binding)
    with sqlite3.connect(path) as connection:
        connection.execute(
            "UPDATE source_quota_broker_adapter_operation "
            "SET response_json = ? WHERE operation_id = ?",
            ("{}", "reserve-a"),
        )
        connection.commit()
    with pytest.raises(SourceQuotaBrokerAdapterIntegrityError):
        _reserve(adapter, binding)

    # A real authority commit without an adapter response must be replayed from
    # the signed authority journal.  Removing that journal produces a conflict,
    # never a second allocation.
    original_commit = adapter._commit_response
    monkeypatch.setattr(
        adapter,
        "_commit_response",
        lambda **_kwargs: (_ for _ in ()).throw(ConnectionError("response dropped")),
    )
    with pytest.raises(ConnectionError, match="dropped"):
        _intent(adapter, binding)
    monkeypatch.setattr(adapter, "_commit_response", original_commit)
    with sqlite3.connect(authority.path) as connection:
        connection.execute(
            "DELETE FROM source_quota_operation WHERE operation_id = ?",
            ("intent-a",),
        )
        connection.commit()
    restarted = SourceQuotaBrokerAdapterV2(path, authority=authority, adapter_id="broker-quota-v2")
    with pytest.raises(SourceQuotaBrokerAdapterIntegrityError):
        _intent(restarted, binding)


def test_committed_v2_response_still_requires_the_native_signed_journal_on_replay(
    tmp_path: Path,
) -> None:
    from rquant.source_quota_broker_adapter import SourceQuotaBrokerAdapterIntegrityError

    adapter = _adapter(tmp_path)
    binding = _binding()
    _reserve(adapter, binding)
    with sqlite3.connect(adapter._authority.path) as connection:
        connection.execute(
            "DELETE FROM source_quota_operation WHERE operation_id = ?",
            ("reserve-a",),
        )
        connection.commit()
    with pytest.raises(SourceQuotaBrokerAdapterIntegrityError):
        _reserve(adapter, binding)


@pytest.mark.parametrize(
    "payload",
    (
        b'{"schema_version":2,"schema_version":2}',
        b'{"schema_version":2}\n',
        b'{"schema_version":NaN}',
        b'{"schema_version":"2"}',
        # An explicit id keeps a quarter megabyte of padding out of the nodeid.
        # The four cases above keep pytest's derived ids: this module is frozen
        # by tests/manifests/source_broker_v2_frozen.json, so every renamed id
        # moves that manifest's digest and only this one earns the move.
        pytest.param(b"{" + b"x" * (256 * 1024) + b"}", id="oversize"),
    ),
)
def test_v2_receipt_wire_rejects_noncanonical_and_oversized_json(payload: bytes) -> None:
    from rquant.source_quota_broker_adapter import (
        SourceQuotaBrokerAdapterIntegrityError,
        decode_source_quota_broker_receipt_v2,
    )

    with pytest.raises(SourceQuotaBrokerAdapterIntegrityError):
        decode_source_quota_broker_receipt_v2(payload)


def test_v2_receipt_wire_rejects_unknown_fields_even_when_the_json_is_canonical(
    tmp_path: Path,
) -> None:
    from rquant.source_quota_broker_adapter import (
        SourceQuotaBrokerAdapterIntegrityError,
        decode_source_quota_broker_receipt_v2,
        encode_source_quota_broker_receipt_v2,
    )
    from rquant.strict_json import canonical_json_bytes

    adapter = _adapter(tmp_path)
    wire = encode_source_quota_broker_receipt_v2(_reserve(adapter, _binding()))
    data = json.loads(wire)
    data["unexpected"] = "not-a-broker-field"
    with pytest.raises(SourceQuotaBrokerAdapterIntegrityError):
        decode_source_quota_broker_receipt_v2(canonical_json_bytes(data))


def test_native_receipt_roundtrip_does_not_drop_signed_authority_fields(tmp_path: Path) -> None:
    from rquant.source_quota_broker_adapter import (
        decode_source_quota_broker_receipt_v2,
        encode_source_quota_broker_receipt_v2,
    )

    adapter = _adapter(tmp_path)
    receipt = _reserve(adapter, _binding())
    decoded = decode_source_quota_broker_receipt_v2(encode_source_quota_broker_receipt_v2(receipt))
    assert decoded == receipt
    assert decoded.authority_result.receipt == receipt.authority_result.receipt
    assert decoded.authority_result.receipt.signature == receipt.authority_result.receipt.signature
    assert (
        decoded.authority_result.receipt.effect_key == receipt.authority_result.receipt.effect_key
    )


def test_authority_integrity_failure_maps_to_adapter_integrity_error(tmp_path: Path) -> None:
    from rquant.source_quota_broker_adapter import SourceQuotaBrokerAdapterIntegrityError

    adapter = _adapter(tmp_path)
    binding = _binding()
    _reserve(adapter, binding)
    with sqlite3.connect(adapter._authority.path) as connection:
        connection.execute(
            "UPDATE source_quota_operation SET result_hash = ? WHERE operation_id = ?",
            ("0" * 64, "reserve-a"),
        )
        connection.commit()

    with pytest.raises(SourceQuotaBrokerAdapterIntegrityError):
        _reserve(adapter, binding)


@pytest.mark.parametrize(
    "stored_response",
    [
        7,
        sqlite3.Binary(b"{}"),
        sqlite3.Binary(b"\xff\xfe"),
        "not-json",
    ],
    ids=["integer", "blob", "invalid-utf8", "malformed-text"],
)
def test_adapter_replay_normalizes_non_text_or_invalid_response_to_integrity_error(
    tmp_path: Path,
    stored_response: object,
) -> None:
    from rquant.source_quota_broker_adapter import SourceQuotaBrokerAdapterIntegrityError

    adapter = _adapter(tmp_path)
    binding = _binding()
    _reserve(adapter, binding)
    with sqlite3.connect(adapter.path) as connection:
        connection.execute(
            "UPDATE source_quota_broker_adapter_operation SET response_json = ? "
            "WHERE operation_id = 'reserve-a'",
            (stored_response,),
        )
        connection.commit()

    with pytest.raises(SourceQuotaBrokerAdapterIntegrityError):
        _reserve(adapter, binding)


def test_adapter_commit_normalizes_unexpected_decoder_exception(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import rquant.source_quota_broker_adapter as module

    adapter = _adapter(tmp_path)

    def decoder_failed(_payload: bytes) -> object:
        raise RuntimeError("decoder unavailable")

    monkeypatch.setattr(module, "decode_source_quota_broker_receipt_v2", decoder_failed)
    with pytest.raises(module.SourceQuotaBrokerAdapterIntegrityError, match="decoder"):
        _reserve(adapter, _binding())
