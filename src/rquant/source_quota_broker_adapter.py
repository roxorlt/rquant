"""Closed V2 bridge from signed parent quota authority to the future broker saga.

The legacy ``QuotaLedgerProtocol`` deliberately does not appear here.  It
returns lossy ``EffectReceipt`` values and carries broker payload fields that a
quota authority must never see.  This module owns only durable orchestration of
the native, signed parent/call quota receipts.
"""

from __future__ import annotations

import sqlite3
from datetime import datetime
from enum import StrEnum
from pathlib import Path
from typing import Literal

from pydantic import ConfigDict, Field, ValidationError, model_validator

from rquant.runtime_contracts import AwareUtcDatetime, RuntimeContractModel, canonical_sha256
from rquant.source_quota_authority import (
    SourceQuotaAuthorityConflictError,
    SourceQuotaAuthorityIntegrityError,
    SourceQuotaAuthorityResult,
    SourceQuotaCallOutcome,
    SourceQuotaParentAuthority,
)
from rquant.strict_json import (
    StrictJsonError,
    canonical_json_bytes,
    strict_canonical_json_loads,
    strict_model_validate_canonical_json,
)

SOURCE_QUOTA_BROKER_ADAPTER_V2_CONTRACT = "rquant-source-quota-broker-adapter/v2"
SOURCE_QUOTA_BROKER_ADAPTER_V2_MAX_WIRE_BYTES = 256 * 1024


class SourceQuotaBrokerAdapterError(RuntimeError):
    """Base error for closed V2 quota orchestration."""


class SourceQuotaBrokerAdapterConfigurationError(SourceQuotaBrokerAdapterError):
    """Construction attempted to inject an open-ended authority/provider."""


class SourceQuotaBrokerAdapterConflictError(SourceQuotaBrokerAdapterError):
    """A V2 operation identity or parent binding conflicts with durable state."""


class SourceQuotaBrokerAdapterIntegrityError(SourceQuotaBrokerAdapterError):
    """The adapter's immutable orchestration journal is malformed or tampered."""


class SourceQuotaBrokerPhaseV2(StrEnum):
    RESERVE_PARENT = "reserve_parent"
    RECORD_INTENT = "record_intent"
    AUTHORIZE_DISPATCH = "authorize_dispatch"
    FINALIZE = "finalize"
    UNKNOWN_BEFORE_DISPATCH = "unknown_before_dispatch"
    RELEASE_UNUSED = "release_unused"


_NATIVE_OPERATION_BY_PHASE = {
    SourceQuotaBrokerPhaseV2.RESERVE_PARENT: "reserve_parent",
    SourceQuotaBrokerPhaseV2.RECORD_INTENT: "record_intent",
    SourceQuotaBrokerPhaseV2.AUTHORIZE_DISPATCH: "authorize_dispatch",
    SourceQuotaBrokerPhaseV2.FINALIZE: "finalize",
    SourceQuotaBrokerPhaseV2.UNKNOWN_BEFORE_DISPATCH: "unknown_before_dispatch",
    SourceQuotaBrokerPhaseV2.RELEASE_UNUSED: "release_unused",
}


class _StrictAdapterModel(RuntimeContractModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)


class SourceQuotaParentBindingV2(_StrictAdapterModel):
    """Scheduler fencing identity permanently attached to one quota parent."""

    schema_version: Literal[2] = 2
    parent_id: str = Field(min_length=1, max_length=200)
    source: str = Field(min_length=1, max_length=200)
    owner: str = Field(min_length=1, max_length=200)
    claim_binding_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    claim_generation: int = Field(strict=True, ge=1)
    scheduler_fencing_token: int = Field(strict=True, ge=1)

    @property
    def binding_hash(self) -> str:
        return canonical_sha256(self.model_dump(mode="python"))


class SourceQuotaBrokerReceiptV2(_StrictAdapterModel):
    """Versioned wrapper retaining the complete native authority result."""

    schema_version: Literal[2] = 2
    contract: Literal["rquant-source-quota-broker-adapter/v2"] = (
        SOURCE_QUOTA_BROKER_ADAPTER_V2_CONTRACT
    )
    adapter_id: str = Field(min_length=1, max_length=200)
    phase: SourceQuotaBrokerPhaseV2
    operation_id: str = Field(min_length=1, max_length=200)
    binding: SourceQuotaParentBindingV2
    authority_result: SourceQuotaAuthorityResult

    @model_validator(mode="after")
    def validate_native_result(self) -> SourceQuotaBrokerReceiptV2:
        result = self.authority_result
        if result.receipt.operation.value != _NATIVE_OPERATION_BY_PHASE[self.phase]:
            raise ValueError("adapter phase conflicts with native signed operation")
        if result.parent.parent_id != self.binding.parent_id:
            raise ValueError("authority parent conflicts with adapter binding")
        if result.parent.source != self.binding.source or result.parent.owner != self.binding.owner:
            raise ValueError("authority quota parent identity conflicts with adapter binding")
        if (
            result.parent.claim_binding_hash != self.binding.claim_binding_hash
            or result.parent.claim_generation != self.binding.claim_generation
            or result.parent.scheduler_fencing_token != self.binding.scheduler_fencing_token
        ):
            raise ValueError("native quota parent claim binding conflicts with adapter binding")
        call = result.call
        if self.phase in {
            SourceQuotaBrokerPhaseV2.RECORD_INTENT,
            SourceQuotaBrokerPhaseV2.AUTHORIZE_DISPATCH,
            SourceQuotaBrokerPhaseV2.FINALIZE,
            SourceQuotaBrokerPhaseV2.UNKNOWN_BEFORE_DISPATCH,
        }:
            if call is None or call.parent_id != self.binding.parent_id:
                raise ValueError("call phase requires a native call for the bound parent")
        elif call is not None:
            raise ValueError("parent phase cannot carry a call result")
        return self

    @property
    def receipt_hash(self) -> str:
        return canonical_sha256(self.model_dump(mode="python"))


class _ReserveRequest(_StrictAdapterModel):
    binding: SourceQuotaParentBindingV2
    total_cost: int = Field(strict=True, ge=1)
    now: AwareUtcDatetime
    expires_at: AwareUtcDatetime

    @model_validator(mode="after")
    def validate_expiry(self) -> _ReserveRequest:
        if self.expires_at <= self.now:
            raise ValueError("quota parent expiry must follow reservation time")
        return self


class _CallRequest(_StrictAdapterModel):
    binding: SourceQuotaParentBindingV2
    call_id: str = Field(min_length=1, max_length=200)
    now: AwareUtcDatetime


class _IntentRequest(_CallRequest):
    cost: int = Field(strict=True, ge=1)


class _FinalizeRequest(_CallRequest):
    outcome: Literal["SUCCESS", "FAILURE", "UNKNOWN"]


class _ReleaseRequest(_StrictAdapterModel):
    binding: SourceQuotaParentBindingV2
    now: AwareUtcDatetime


_Request = _ReserveRequest | _IntentRequest | _CallRequest | _FinalizeRequest | _ReleaseRequest


def encode_source_quota_broker_receipt_v2(receipt: SourceQuotaBrokerReceiptV2) -> bytes:
    """Return canonical bounded wire bytes for a V2 quota receipt."""

    validated = SourceQuotaBrokerReceiptV2.model_validate(receipt, strict=True)
    payload = canonical_json_bytes(validated.model_dump(mode="json", round_trip=True))
    if len(payload) > SOURCE_QUOTA_BROKER_ADAPTER_V2_MAX_WIRE_BYTES:
        raise SourceQuotaBrokerAdapterIntegrityError("quota receipt exceeds V2 wire bound")
    return payload


def decode_source_quota_broker_receipt_v2(payload: bytes) -> SourceQuotaBrokerReceiptV2:
    """Strictly decode one V2 receipt, rejecting alternate JSON spellings."""

    if (
        type(payload) is not bytes
        or not payload
        or len(payload) > SOURCE_QUOTA_BROKER_ADAPTER_V2_MAX_WIRE_BYTES
    ):
        raise SourceQuotaBrokerAdapterIntegrityError("quota receipt violates V2 wire bound")
    try:
        strict_canonical_json_loads(payload)
        receipt = strict_model_validate_canonical_json(SourceQuotaBrokerReceiptV2, payload)
        if encode_source_quota_broker_receipt_v2(receipt) != payload:
            raise ValueError("quota receipt is not canonical")
        return receipt
    except (StrictJsonError, ValidationError, ValueError, TypeError) as exc:
        raise SourceQuotaBrokerAdapterIntegrityError("quota receipt is malformed") from exc


class SourceQuotaBrokerAdapterV2:
    """Durably map closed V2 saga phases onto one signed quota authority.

    Calls contain only quota identity, fencing and lifecycle evidence.  In
    particular, no request payload, provider callback, import path or callable
    crosses this boundary.
    """

    def __init__(
        self,
        path: Path,
        *,
        authority: SourceQuotaParentAuthority,
        adapter_id: str,
        busy_timeout_ms: int = 5_000,
    ) -> None:
        if type(authority) is not SourceQuotaParentAuthority:
            raise SourceQuotaBrokerAdapterConfigurationError(
                "V2 quota adapter requires the exact persistent parent authority"
            )
        identifier = adapter_id.strip()
        if not identifier:
            raise ValueError("adapter_id must be nonempty")
        if busy_timeout_ms < 1:
            raise ValueError("busy_timeout_ms must be positive")
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._authority = authority
        self.adapter_id = identifier
        self._busy_timeout_ms = busy_timeout_ms
        self._initialize()

    def reserve_parent(
        self,
        *,
        operation_id: str,
        binding: SourceQuotaParentBindingV2,
        total_cost: int,
        now: datetime,
        expires_at: datetime,
    ) -> SourceQuotaBrokerReceiptV2:
        request = _ReserveRequest(
            binding=binding,
            total_cost=total_cost,
            now=now,
            expires_at=expires_at,
        )
        return self._execute(
            phase=SourceQuotaBrokerPhaseV2.RESERVE_PARENT,
            operation_id=operation_id,
            request=request,
        )

    def record_intent(
        self,
        *,
        operation_id: str,
        binding: SourceQuotaParentBindingV2,
        call_id: str,
        cost: int,
        now: datetime,
    ) -> SourceQuotaBrokerReceiptV2:
        return self._execute(
            phase=SourceQuotaBrokerPhaseV2.RECORD_INTENT,
            operation_id=operation_id,
            request=_IntentRequest(binding=binding, call_id=call_id, cost=cost, now=now),
        )

    def authorize_dispatch(
        self,
        *,
        operation_id: str,
        binding: SourceQuotaParentBindingV2,
        call_id: str,
        now: datetime,
    ) -> SourceQuotaBrokerReceiptV2:
        return self._execute(
            phase=SourceQuotaBrokerPhaseV2.AUTHORIZE_DISPATCH,
            operation_id=operation_id,
            request=_CallRequest(binding=binding, call_id=call_id, now=now),
        )

    def finalize(
        self,
        *,
        operation_id: str,
        binding: SourceQuotaParentBindingV2,
        call_id: str,
        outcome: Literal["SUCCESS", "FAILURE", "UNKNOWN"],
        now: datetime,
    ) -> SourceQuotaBrokerReceiptV2:
        return self._execute(
            phase=SourceQuotaBrokerPhaseV2.FINALIZE,
            operation_id=operation_id,
            request=_FinalizeRequest(binding=binding, call_id=call_id, outcome=outcome, now=now),
        )

    def unknown_before_dispatch(
        self,
        *,
        operation_id: str,
        binding: SourceQuotaParentBindingV2,
        call_id: str,
        now: datetime,
    ) -> SourceQuotaBrokerReceiptV2:
        return self._execute(
            phase=SourceQuotaBrokerPhaseV2.UNKNOWN_BEFORE_DISPATCH,
            operation_id=operation_id,
            request=_CallRequest(binding=binding, call_id=call_id, now=now),
        )

    def release_unused(
        self,
        *,
        operation_id: str,
        binding: SourceQuotaParentBindingV2,
        now: datetime,
    ) -> SourceQuotaBrokerReceiptV2:
        return self._execute(
            phase=SourceQuotaBrokerPhaseV2.RELEASE_UNUSED,
            operation_id=operation_id,
            request=_ReleaseRequest(binding=binding, now=now),
        )

    def _execute(
        self,
        *,
        phase: SourceQuotaBrokerPhaseV2,
        operation_id: str,
        request: _Request,
    ) -> SourceQuotaBrokerReceiptV2:
        identifier = self._operation_id(operation_id)
        request_json = _request_json(request)
        request_hash = canonical_sha256({"phase": phase.value, "request": request_json})
        # A rejected stale/foreign fence must not create a pending operation that
        # prevents the rightful generation from using its operation identity.
        if phase is not SourceQuotaBrokerPhaseV2.RESERVE_PARENT:
            self._require_durable_binding(request.binding)
        existing = self._begin_or_replay(
            operation_id=identifier,
            phase=phase,
            binding=request.binding,
            request_json=request_json,
            request_hash=request_hash,
        )
        if existing is not None:
            # The wrapper is never an alternative source of truth.  Replaying a
            # V2 operation must make the native authority replay its complete
            # signed chain too, otherwise a locally cached response could hide
            # a deleted or tampered native journal.
            try:
                replayed = self._call_authority(
                    phase=phase,
                    operation_id=identifier,
                    request=request,
                )
            except SourceQuotaAuthorityIntegrityError as exc:
                raise SourceQuotaBrokerAdapterIntegrityError(
                    "native quota journal failed integrity validation"
                ) from exc
            except SourceQuotaAuthorityConflictError as exc:
                raise SourceQuotaBrokerAdapterConflictError(
                    "native quota journal cannot replay V2 operation"
                ) from exc
            native = SourceQuotaBrokerReceiptV2(
                adapter_id=self.adapter_id,
                phase=phase,
                operation_id=identifier,
                binding=request.binding,
                authority_result=replayed,
            )
            self._validate_authority_result(native)
            if native != existing:
                raise SourceQuotaBrokerAdapterIntegrityError(
                    "cached V2 quota response conflicts with native replay"
                )
            return existing
        try:
            result = self._call_authority(phase=phase, operation_id=identifier, request=request)
        except SourceQuotaAuthorityIntegrityError as exc:
            raise SourceQuotaBrokerAdapterIntegrityError(
                "quota authority journal failed integrity validation"
            ) from exc
        except SourceQuotaAuthorityConflictError as exc:
            raise SourceQuotaBrokerAdapterConflictError(
                "quota authority rejected V2 operation"
            ) from exc
        receipt = SourceQuotaBrokerReceiptV2(
            adapter_id=self.adapter_id,
            phase=phase,
            operation_id=identifier,
            binding=request.binding,
            authority_result=result,
        )
        self._validate_authority_result(receipt)
        encoded = encode_source_quota_broker_receipt_v2(receipt)
        self._commit_response(operation_id=identifier, request_hash=request_hash, payload=encoded)
        return receipt

    def _call_authority(
        self,
        *,
        phase: SourceQuotaBrokerPhaseV2,
        operation_id: str,
        request: _Request,
    ) -> SourceQuotaAuthorityResult:
        binding = request.binding
        if phase is SourceQuotaBrokerPhaseV2.RESERVE_PARENT:
            assert isinstance(request, _ReserveRequest)
            return self._authority.reserve_parent(
                operation_id=operation_id,
                parent_id=binding.parent_id,
                source=binding.source,
                owner=binding.owner,
                total_cost=request.total_cost,
                now=request.now,
                expires_at=request.expires_at,
                claim_binding_hash=binding.claim_binding_hash,
                claim_generation=binding.claim_generation,
                scheduler_fencing_token=binding.scheduler_fencing_token,
            )
        self._require_durable_binding(binding)
        if phase is SourceQuotaBrokerPhaseV2.RECORD_INTENT:
            assert isinstance(request, _IntentRequest)
            return self._authority.record_intent(
                operation_id=operation_id,
                parent_id=binding.parent_id,
                call_id=request.call_id,
                cost=request.cost,
                now=request.now,
            )
        if phase is SourceQuotaBrokerPhaseV2.AUTHORIZE_DISPATCH:
            assert isinstance(request, _CallRequest)
            return self._authority.authorize_dispatch(
                operation_id=operation_id,
                parent_id=binding.parent_id,
                call_id=request.call_id,
                now=request.now,
            )
        if phase is SourceQuotaBrokerPhaseV2.FINALIZE:
            assert isinstance(request, _FinalizeRequest)
            return self._authority.finalize(
                operation_id=operation_id,
                parent_id=binding.parent_id,
                call_id=request.call_id,
                outcome=SourceQuotaCallOutcome(request.outcome),
                now=request.now,
            )
        if phase is SourceQuotaBrokerPhaseV2.UNKNOWN_BEFORE_DISPATCH:
            assert isinstance(request, _CallRequest)
            return self._authority.terminalize_unknown_before_dispatch(
                operation_id=operation_id,
                parent_id=binding.parent_id,
                call_id=request.call_id,
                now=request.now,
            )
        assert phase is SourceQuotaBrokerPhaseV2.RELEASE_UNUSED
        assert isinstance(request, _ReleaseRequest)
        return self._authority.release_unused(
            operation_id=operation_id,
            parent_id=binding.parent_id,
            now=request.now,
        )

    def _validate_authority_result(self, receipt: SourceQuotaBrokerReceiptV2) -> None:
        result = receipt.authority_result
        if result.receipt.authority_id != self._authority.authority_id:
            raise SourceQuotaBrokerAdapterIntegrityError(
                "native quota receipt authority is foreign"
            )
        if result.receipt.operation_id != receipt.operation_id:
            raise SourceQuotaBrokerAdapterIntegrityError("native quota receipt operation conflicts")
        parent = result.parent
        if (
            parent.parent_id != receipt.binding.parent_id
            or parent.source != receipt.binding.source
            or parent.owner != receipt.binding.owner
            or parent.claim_binding_hash != receipt.binding.claim_binding_hash
            or parent.claim_generation != receipt.binding.claim_generation
            or parent.scheduler_fencing_token != receipt.binding.scheduler_fencing_token
        ):
            raise SourceQuotaBrokerAdapterIntegrityError("native quota receipt binding conflicts")
        call = result.call
        if call is not None and call.parent_id != parent.parent_id:
            raise SourceQuotaBrokerAdapterIntegrityError(
                "native quota call belongs to another parent"
            )

    def _require_durable_binding(self, binding: SourceQuotaParentBindingV2) -> None:
        encoded = _binding_json(binding)
        with self._connect() as connection:
            row = connection.execute(
                "SELECT binding_json FROM source_quota_broker_adapter_parent WHERE parent_id = ?",
                (binding.parent_id,),
            ).fetchone()
        if row is None or row["binding_json"] != encoded:
            raise SourceQuotaBrokerAdapterConflictError("stale or foreign parent generation/fence")
        parent = self._authority.get_parent(binding.parent_id)
        if parent is None:
            raise SourceQuotaBrokerAdapterIntegrityError("durable quota parent is missing")
        if (
            parent.source != binding.source
            or parent.owner != binding.owner
            or parent.claim_binding_hash != binding.claim_binding_hash
            or parent.claim_generation != binding.claim_generation
            or parent.scheduler_fencing_token != binding.scheduler_fencing_token
        ):
            raise SourceQuotaBrokerAdapterConflictError(
                "durable quota parent claim generation/fence conflicts"
            )

    def _begin_or_replay(
        self,
        *,
        operation_id: str,
        phase: SourceQuotaBrokerPhaseV2,
        binding: SourceQuotaParentBindingV2,
        request_json: str,
        request_hash: str,
    ) -> SourceQuotaBrokerReceiptV2 | None:
        binding_json = _binding_json(binding)
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                row = connection.execute(
                    "SELECT * FROM source_quota_broker_adapter_operation WHERE operation_id = ?",
                    (operation_id,),
                ).fetchone()
                if row is None:
                    connection.execute(
                        """
                        INSERT INTO source_quota_broker_adapter_operation(
                            operation_id, phase, parent_id, binding_json, request_json,
                            request_hash, response_json
                        ) VALUES (?, ?, ?, ?, ?, ?, NULL)
                        """,
                        (
                            operation_id,
                            phase.value,
                            binding.parent_id,
                            binding_json,
                            request_json,
                            request_hash,
                        ),
                    )
                    connection.commit()
                    return None
                if (
                    row["phase"] != phase.value
                    or row["binding_json"] != binding_json
                    or row["request_json"] != request_json
                    or row["request_hash"] != request_hash
                ):
                    raise SourceQuotaBrokerAdapterConflictError(
                        "V2 operation_id conflicts with its durable request"
                    )
                response = row["response_json"]
                connection.commit()
            except BaseException:
                connection.rollback()
                raise
        if response is None:
            return None
        if type(response) is not str:
            raise SourceQuotaBrokerAdapterIntegrityError(
                "stored V2 quota response must be SQLite TEXT"
            )
        try:
            return decode_source_quota_broker_receipt_v2(response.encode("utf-8"))
        except SourceQuotaBrokerAdapterIntegrityError:
            raise
        except Exception as exc:
            raise SourceQuotaBrokerAdapterIntegrityError(
                "stored V2 quota response decoder failed"
            ) from exc

    def _commit_response(self, *, operation_id: str, request_hash: str, payload: bytes) -> None:
        try:
            encoded = payload.decode("utf-8")
        except (AttributeError, UnicodeDecodeError) as exc:
            raise SourceQuotaBrokerAdapterIntegrityError(
                "V2 quota response is not valid UTF-8 bytes"
            ) from exc
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                row = connection.execute(
                    "SELECT request_hash, response_json FROM source_quota_broker_adapter_operation "
                    "WHERE operation_id = ?",
                    (operation_id,),
                ).fetchone()
                if row is None or row["request_hash"] != request_hash:
                    raise SourceQuotaBrokerAdapterIntegrityError(
                        "V2 quota operation disappeared before response commit"
                    )
                prior = row["response_json"]
                if prior is not None and type(prior) is not str:
                    raise SourceQuotaBrokerAdapterIntegrityError(
                        "stored V2 quota response must be SQLite TEXT"
                    )
                if prior is not None and prior != encoded:
                    raise SourceQuotaBrokerAdapterIntegrityError(
                        "V2 quota operation response changed after commit"
                    )
                connection.execute(
                    "UPDATE source_quota_broker_adapter_operation SET response_json = ? "
                    "WHERE operation_id = ? AND response_json IS NULL",
                    (encoded, operation_id),
                )
                try:
                    receipt = decode_source_quota_broker_receipt_v2(payload)
                except SourceQuotaBrokerAdapterIntegrityError:
                    raise
                except Exception as exc:
                    raise SourceQuotaBrokerAdapterIntegrityError(
                        "V2 quota response decoder failed"
                    ) from exc
                if receipt.phase is SourceQuotaBrokerPhaseV2.RESERVE_PARENT:
                    self._bind_parent_in_transaction(connection, receipt.binding)
                connection.commit()
            except BaseException:
                connection.rollback()
                raise

    def _bind_parent_in_transaction(
        self,
        connection: sqlite3.Connection,
        binding: SourceQuotaParentBindingV2,
    ) -> None:
        binding_json = _binding_json(binding)
        row = connection.execute(
            "SELECT binding_json FROM source_quota_broker_adapter_parent WHERE parent_id = ?",
            (binding.parent_id,),
        ).fetchone()
        if row is None:
            connection.execute(
                "INSERT INTO source_quota_broker_adapter_parent("
                "parent_id, binding_json) VALUES (?, ?)",
                (binding.parent_id, binding_json),
            )
        elif row["binding_json"] != binding_json:
            raise SourceQuotaBrokerAdapterIntegrityError("quota parent binding changed")

    def _operation_id(self, value: str) -> str:
        if not isinstance(value, str) or not value.strip() or len(value) > 200:
            raise ValueError("operation_id must be a nonempty bounded string")
        return value.strip()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(
            self.path,
            timeout=self._busy_timeout_ms / 1_000,
            isolation_level=None,
        )
        connection.row_factory = sqlite3.Row
        connection.execute(f"PRAGMA busy_timeout = {self._busy_timeout_ms}")
        connection.execute("PRAGMA journal_mode = WAL")
        connection.execute("PRAGMA synchronous = FULL")
        return connection

    def _initialize(self) -> None:
        with self._connect() as connection:
            connection.execute("BEGIN EXCLUSIVE")
            try:
                connection.execute(
                    """
                    CREATE TABLE IF NOT EXISTS source_quota_broker_adapter_operation (
                        operation_id TEXT PRIMARY KEY,
                        phase TEXT NOT NULL,
                        parent_id TEXT NOT NULL,
                        binding_json TEXT NOT NULL,
                        request_json TEXT NOT NULL,
                        request_hash TEXT NOT NULL,
                        response_json TEXT
                    )
                    """
                )
                connection.execute(
                    """
                    CREATE TABLE IF NOT EXISTS source_quota_broker_adapter_parent (
                        parent_id TEXT PRIMARY KEY,
                        binding_json TEXT NOT NULL
                    )
                    """
                )
                connection.commit()
            except BaseException:
                connection.rollback()
                raise


def _binding_json(binding: SourceQuotaParentBindingV2) -> str:
    return canonical_json_bytes(binding.model_dump(mode="json", round_trip=True)).decode("utf-8")


def _request_json(request: _Request) -> str:
    return canonical_json_bytes(request.model_dump(mode="json", round_trip=True)).decode("utf-8")


__all__ = [
    "SOURCE_QUOTA_BROKER_ADAPTER_V2_CONTRACT",
    "SOURCE_QUOTA_BROKER_ADAPTER_V2_MAX_WIRE_BYTES",
    "SourceQuotaBrokerAdapterConfigurationError",
    "SourceQuotaBrokerAdapterConflictError",
    "SourceQuotaBrokerAdapterError",
    "SourceQuotaBrokerAdapterIntegrityError",
    "SourceQuotaBrokerAdapterV2",
    "SourceQuotaBrokerPhaseV2",
    "SourceQuotaBrokerReceiptV2",
    "SourceQuotaParentBindingV2",
    "decode_source_quota_broker_receipt_v2",
    "encode_source_quota_broker_receipt_v2",
]
