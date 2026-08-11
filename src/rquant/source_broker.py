"""Fail-closed source broker with durable quota effects and signed evidence."""

from __future__ import annotations

import json
import math
import re
import sqlite3
import threading
import time
from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from types import NoneType, UnionType
from typing import Annotated, Any, Literal, Protocol, Union, cast, get_args, get_origin
from uuid import UUID, uuid4

from pydantic import BaseModel, Field, ValidationError, field_validator, model_validator

from rquant.adapter_manifest import (
    BROKER_OUTBOX_NAMESPACE,
    BROKER_RECEIPT_NAMESPACE,
    BROKER_STATEMENT_NAMESPACE,
    QUOTA_EFFECT_NAMESPACE,
    REPLAY_CLAIM_NAMESPACE,
    Ed25519ContractSigner,
    KeyPurpose,
    PydanticModelSchema,
    SourceUsePlan,
    VerifyOnlyEd25519Keyring,
)
from rquant.runtime_contracts import RuntimeContractModel, canonical_sha256


class SourceBrokerError(RuntimeError):
    """Raised when a source operation cannot be authorized or recovered safely."""


CallOutcome = Literal["success", "failure", "unknown", "unknown_before_dispatch"]
_IDEMPOTENCY_KEY_PATTERN = re.compile(r"^[A-Za-z0-9](?:[A-Za-z0-9._:-]{0,126}[A-Za-z0-9])?$")
_DEFAULT_BOOT_ID = f"local-boot-{int((time.time() - time.monotonic()) // 60)}"
OutboxKind = Literal[
    "replay",
    "reserve",
    "intent",
    "dispatch",
    "finalize",
    "release",
    "recover",
]


@dataclass(frozen=True)
class LeaseClockReading:
    wall_time: float
    monotonic_time: float
    boot_id: str

    def __post_init__(self) -> None:
        if (
            not math.isfinite(self.wall_time)
            or not math.isfinite(self.monotonic_time)
            or not self.boot_id.strip()
        ):
            raise ValueError("lease clock reading must be finite and identify the current boot")


@dataclass(frozen=True)
class _LeaseGuard:
    table: Literal["broker_session", "broker_call"]
    key_column: Literal["claim_token", "call_id"]
    aggregate_id: str
    owner_id: str
    fencing_token: int


class QuotaReservation(RuntimeContractModel):
    reservation_id: str = Field(min_length=1, max_length=500)
    claim_token: str = Field(min_length=1, max_length=500)
    source: str = Field(min_length=1, max_length=200)
    reserved_units: int = Field(strict=True, gt=0)


class EffectReceipt(RuntimeContractModel):
    authority_id: str = Field(min_length=1, max_length=200)
    operation_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    payload_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    effect: OutboxKind
    outcome: Literal["applied"]
    result_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    key_id: str = Field(min_length=1, max_length=200)
    signature_algorithm: Literal["ed25519"] = "ed25519"
    signature: str

    def signing_bytes(self) -> bytes:
        return _json_bytes(self.model_dump(mode="json", exclude={"signature"}))

    @property
    def receipt_hash(self) -> str:
        return canonical_sha256(self.model_dump(mode="python"))


class QuotaEffectResponse(RuntimeContractModel):
    receipt: EffectReceipt
    result: QuotaReservation | None = None

    @model_validator(mode="after")
    def validate_result_hash(self) -> QuotaEffectResponse:
        if self.receipt.result_hash != canonical_sha256(self.result):
            raise ValueError("quota effect result does not match its receipt")
        return self


class QuotaLedgerProtocol(Protocol):
    def reserve(
        self,
        *,
        operation_id: str,
        claim_token: str,
        source: str,
        units: int,
    ) -> QuotaEffectResponse: ...

    def record_intent(
        self,
        *,
        operation_id: str,
        reservation_id: str,
        call_id: str,
        claim_token: str,
        idempotency_key: str,
        manifest_hash: str,
        source: str,
        operation: str,
        request_hash: str,
        cost: int,
    ) -> QuotaEffectResponse: ...

    def mark_dispatched(
        self,
        *,
        operation_id: str,
        reservation_id: str,
        call_id: str,
    ) -> QuotaEffectResponse: ...

    def finalize(
        self,
        *,
        operation_id: str,
        reservation_id: str,
        call_id: str,
        outcome: str,
    ) -> QuotaEffectResponse: ...

    def recover(
        self,
        *,
        operation_id: str,
        reservation_id: str,
        call_id: str,
    ) -> QuotaEffectResponse: ...

    def release_unused(self, *, operation_id: str, reservation_id: str) -> QuotaEffectResponse: ...


class ReplayLineageCheckpointReceipt(RuntimeContractModel):
    schema_version: Literal[1]
    contract: Literal["rquant-source-replay-lineage-checkpoint/v1"]
    authority_id: str = Field(min_length=1, max_length=200)
    operation_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    replay_authority_id: str = Field(min_length=1, max_length=200)
    lineage_id: str = Field(min_length=1, max_length=200)
    previous_head_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    next_head_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    sequence: int = Field(strict=True, ge=1)
    claim_binding_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    outcome: Literal["applied"]
    key_id: str = Field(min_length=1, max_length=200)
    signature_algorithm: Literal["ed25519"] = "ed25519"
    signature: str

    def signing_bytes(self) -> bytes:
        return _json_bytes(self.model_dump(mode="json", exclude={"signature"}))

    @property
    def receipt_hash(self) -> str:
        return canonical_sha256(self.model_dump(mode="python"))


class ReplayLineageAuthorityProtocol(Protocol):
    @property
    def authority_id(self) -> str: ...

    @property
    def verifier_fingerprints(self) -> frozenset[str]: ...

    def compare_and_advance(
        self,
        *,
        operation_id: str,
        replay_authority_id: str,
        lineage_id: str,
        previous_head_hash: str,
        next_head_hash: str,
        sequence: int,
        claim_binding_hash: str,
    ) -> ReplayLineageCheckpointReceipt: ...

    def verify_current(
        self,
        *,
        replay_authority_id: str,
        lineage_id: str,
        head_hash: str,
        sequence: int,
        receipt: ReplayLineageCheckpointReceipt | None,
    ) -> None: ...


class ReplayAuthorityProtocol(Protocol):
    @property
    def authority_id(self) -> str: ...

    @property
    def lineage_verifier_fingerprints(self) -> frozenset[str]: ...

    def consume_once(
        self,
        *,
        operation_id: str,
        nonce: str,
        plan_hash: str,
        claim_token: str,
        broker_id: str,
    ) -> EffectReceipt: ...

    def verify_claim_binding(
        self,
        *,
        operation_id: str,
        nonce: str,
        plan_hash: str,
        claim_token: str,
        broker_id: str,
        receipt: EffectReceipt,
    ) -> EffectReceipt: ...


class ReplayGenesisRecord(RuntimeContractModel):
    schema_version: Literal[3]
    contract: Literal["rquant-source-replay-genesis/v3"]
    authority_id: str = Field(min_length=1, max_length=200)
    lineage_id: str = Field(min_length=1, max_length=200)
    sequence: int = Field(strict=True, ge=1)
    claim_token: str = Field(min_length=1, max_length=500)
    plan_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    nonce: str = Field(min_length=1, max_length=500)
    broker_id: str = Field(min_length=1, max_length=200)
    operation_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    previous_record_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    effect_receipt_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    key_id: str = Field(min_length=1, max_length=200)
    signature_algorithm: Literal["ed25519"] = "ed25519"
    signature: str

    def signing_bytes(self) -> bytes:
        return _json_bytes(self.model_dump(mode="json", exclude={"signature"}))

    @property
    def record_hash(self) -> str:
        return canonical_sha256(self.model_dump(mode="python"))


class ReplayAuthorityHead(RuntimeContractModel):
    schema_version: Literal[3]
    contract: Literal["rquant-source-replay-authority-head/v3"]
    authority_id: str = Field(min_length=1, max_length=200)
    lineage_id: str = Field(min_length=1, max_length=200)
    record_count: int = Field(strict=True, ge=0)
    head_record_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    key_id: str = Field(min_length=1, max_length=200)
    signature_algorithm: Literal["ed25519"] = "ed25519"
    signature: str

    def signing_bytes(self) -> bytes:
        return _json_bytes(self.model_dump(mode="json", exclude={"signature"}))

    @property
    def head_hash(self) -> str:
        return canonical_sha256(self.model_dump(mode="python"))


class ReplayLineageAdvance(RuntimeContractModel):
    schema_version: Literal[1]
    contract: Literal["rquant-source-replay-lineage-advance/v1"]
    operation_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    replay_authority_id: str = Field(min_length=1, max_length=200)
    lineage_id: str = Field(min_length=1, max_length=200)
    previous_head_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    next_head_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    sequence: int = Field(strict=True, ge=1)
    claim_binding_hash: str = Field(pattern=r"^[0-9a-f]{64}$")


class ReplayLineageOutboxEnvelope(RuntimeContractModel):
    schema_version: Literal[1]
    contract: Literal["rquant-source-replay-lineage-outbox/v1"]
    replay_authority_id: str = Field(min_length=1, max_length=200)
    lineage_id: str = Field(min_length=1, max_length=200)
    operation_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    sequence: int = Field(strict=True, ge=1)
    request_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    status: Literal["pending", "applied"]
    checkpoint_receipt_hash: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    previous_envelope_hash: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    transition_seq: int = Field(strict=True, ge=1, le=2)
    key_id: str = Field(min_length=1, max_length=200)
    signature_algorithm: Literal["ed25519"] = "ed25519"
    signature: str

    @model_validator(mode="after")
    def validate_transition(self) -> ReplayLineageOutboxEnvelope:
        if self.status == "pending" and (
            self.transition_seq != 1
            or self.checkpoint_receipt_hash is not None
            or self.previous_envelope_hash is not None
        ):
            raise ValueError("pending lineage outbox envelope is invalid")
        if self.status == "applied" and (
            self.transition_seq != 2
            or self.checkpoint_receipt_hash is None
            or self.previous_envelope_hash is None
        ):
            raise ValueError("applied lineage outbox envelope is invalid")
        return self

    def signing_bytes(self) -> bytes:
        return _json_bytes(self.model_dump(mode="json", exclude={"signature"}))

    @property
    def envelope_hash(self) -> str:
        return canonical_sha256(self.model_dump(mode="python"))


class SQLiteReplayAuthority:
    """SQLite replay state anchored by a separately trusted monotonic lineage authority."""

    _SCHEMA_VERSION = 3
    _CHAIN_ROOT = "0" * 64
    _META_COLUMNS = (
        "singleton",
        "schema_version",
        "authority_id",
        "lineage_id",
        "head_json",
        "checkpoint_receipt_json",
    )
    _GENESIS_COLUMNS = (
        "claim_token",
        "sequence",
        "plan_hash",
        "nonce",
        "broker_id",
        "operation_id",
        "record_json",
        "receipt_json",
    )
    _LINEAGE_OUTBOX_COLUMNS = (
        "operation_id",
        "sequence",
        "claim_token",
        "request_json",
        "genesis_record_json",
        "replay_receipt_json",
        "previous_head_json",
        "next_head_json",
        "status",
        "checkpoint_receipt_json",
        "previous_envelope_json",
        "envelope_json",
    )

    def __init__(
        self,
        path: Path,
        *,
        authority_id: str,
        signer: Ed25519ContractSigner,
        keyring: VerifyOnlyEd25519Keyring,
        lineage_authority: ReplayLineageAuthorityProtocol | None,
        fault_injector: Callable[[str, str, str], None] | None = None,
    ) -> None:
        self.path = Path(path)
        self.authority_id = authority_id.strip()
        if not self.authority_id:
            raise ValueError("replay authority id must be nonempty")
        if signer.key_purpose != "replay_claim" or signer.issuer != self.authority_id:
            raise ValueError("replay claim signer must match the shared authority")
        if not keyring.allows_signer(signer):
            raise ValueError("replay claim signer fingerprint is not allowed by its keyring")
        if lineage_authority is None:
            raise ValueError("an external replay lineage authority is required")
        lineage_authority_id = lineage_authority.authority_id.strip()
        lineage_fingerprints = lineage_authority.verifier_fingerprints
        if not lineage_authority_id or not lineage_fingerprints:
            raise ValueError("replay lineage authority trust identity is incomplete")
        if signer.public_key_fingerprint in lineage_fingerprints:
            raise ValueError("replay and lineage authority signing fingerprints must differ")
        self._signer = signer
        self._keyring = keyring
        self._lineage_authority = lineage_authority
        self._lineage_authority_id = lineage_authority_id
        self._fault_injector = fault_injector
        challenge = _json_bytes(
            {
                "contract": "rquant-replay-signer-binding/v1",
                "authority_id": self.authority_id,
                "key_id": signer.key_id,
                "public_key_fingerprint": signer.public_key_fingerprint,
            }
        )
        signature = signer.sign(namespace=REPLAY_CLAIM_NAMESPACE, payload=challenge)
        if not keyring.verify(
            issuer=signer.issuer,
            key_id=signer.key_id,
            key_purpose="replay_claim",
            namespace=REPLAY_CLAIM_NAMESPACE,
            payload=challenge,
            signature=signature,
        ):
            raise ValueError("replay signing client does not control its declared public key")
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                tables = {
                    row["name"]
                    for row in connection.execute(
                        "SELECT name FROM sqlite_master WHERE type = 'table'"
                    ).fetchall()
                }
                if "source_plan_nonce" in tables:
                    raise SourceBrokerError(
                        "legacy replay authority schema v1 requires explicit migration"
                    )
                expected_tables = {
                    "replay_authority_meta",
                    "source_claim_genesis",
                    "replay_lineage_outbox",
                }
                present = tables & expected_tables
                if not present:
                    self._create_schema(connection)
                elif present != expected_tables:
                    raise SourceBrokerError(
                        "legacy replay authority schema v2 requires explicit migration"
                    )
                self._validate_schema(connection)
                self._audit_state(connection)
                connection.commit()
            except BaseException:
                connection.rollback()
                raise
        self._recover_pending_checkpoints()
        self._verify_external_current()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path, timeout=5, isolation_level=None)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA busy_timeout = 5000")
        connection.execute("PRAGMA journal_mode = WAL")
        connection.execute("PRAGMA synchronous = FULL")
        return connection

    @property
    def lineage_verifier_fingerprints(self) -> frozenset[str]:
        return self._lineage_authority.verifier_fingerprints

    def _create_schema(self, connection: sqlite3.Connection) -> None:
        lineage_id = str(uuid4())
        connection.execute(
            """
            CREATE TABLE replay_authority_meta (
                singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
                schema_version INTEGER NOT NULL CHECK(schema_version = 3),
                authority_id TEXT NOT NULL,
                lineage_id TEXT NOT NULL UNIQUE,
                head_json TEXT NOT NULL,
                checkpoint_receipt_json TEXT
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE source_claim_genesis (
                claim_token TEXT PRIMARY KEY,
                sequence INTEGER NOT NULL UNIQUE CHECK(sequence > 0),
                plan_hash TEXT NOT NULL,
                nonce TEXT NOT NULL UNIQUE,
                broker_id TEXT NOT NULL,
                operation_id TEXT NOT NULL UNIQUE,
                record_json TEXT NOT NULL,
                receipt_json TEXT NOT NULL
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE replay_lineage_outbox (
                operation_id TEXT PRIMARY KEY,
                sequence INTEGER NOT NULL UNIQUE CHECK(sequence > 0),
                claim_token TEXT NOT NULL UNIQUE,
                request_json TEXT NOT NULL,
                genesis_record_json TEXT NOT NULL,
                replay_receipt_json TEXT NOT NULL,
                previous_head_json TEXT NOT NULL,
                next_head_json TEXT NOT NULL,
                status TEXT NOT NULL CHECK(status IN ('pending', 'applied')),
                checkpoint_receipt_json TEXT,
                previous_envelope_json TEXT,
                envelope_json TEXT NOT NULL
            )
            """
        )
        head = self._sign_head(
            lineage_id=lineage_id,
            record_count=0,
            head_record_hash=self._CHAIN_ROOT,
        )
        inserted = connection.execute(
            """
            INSERT INTO replay_authority_meta(
                singleton, schema_version, authority_id, lineage_id,
                head_json, checkpoint_receipt_json
            ) VALUES (1, 3, ?, ?, ?, NULL)
            """,
            (self.authority_id, lineage_id, head.model_dump_json()),
        ).rowcount
        if inserted != 1:
            raise SourceBrokerError("replay authority metadata initialization failed")

    def _validate_schema(self, connection: sqlite3.Connection) -> None:
        meta_columns = tuple(
            row["name"]
            for row in connection.execute("PRAGMA table_info(replay_authority_meta)").fetchall()
        )
        genesis_columns = tuple(
            row["name"]
            for row in connection.execute("PRAGMA table_info(source_claim_genesis)").fetchall()
        )
        lineage_outbox_columns = tuple(
            row["name"]
            for row in connection.execute("PRAGMA table_info(replay_lineage_outbox)").fetchall()
        )
        if (
            meta_columns != self._META_COLUMNS
            or genesis_columns != self._GENESIS_COLUMNS
            or lineage_outbox_columns != self._LINEAGE_OUTBOX_COLUMNS
        ):
            raise SourceBrokerError(
                "replay authority schema v3 does not match the supported schema"
            )

    def _fault(self, phase: str, operation_id: str) -> None:
        if self._fault_injector is not None:
            self._fault_injector("lineage", phase, operation_id)

    def _sign_head(
        self,
        *,
        lineage_id: str,
        record_count: int,
        head_record_hash: str,
    ) -> ReplayAuthorityHead:
        unsigned = ReplayAuthorityHead(
            schema_version=3,
            contract="rquant-source-replay-authority-head/v3",
            authority_id=self.authority_id,
            lineage_id=lineage_id,
            record_count=record_count,
            head_record_hash=head_record_hash,
            key_id=self._signer.key_id,
            signature="",
        )
        return unsigned.model_copy(
            update={
                "signature": self._signer.sign(
                    namespace=REPLAY_CLAIM_NAMESPACE,
                    payload=unsigned.signing_bytes(),
                )
            }
        )

    def _validate_head(self, head_json: str, *, lineage_id: str) -> ReplayAuthorityHead:
        try:
            head = ReplayAuthorityHead.model_validate_json(head_json)
        except ValidationError as exc:
            raise SourceBrokerError("replay authority head is structurally invalid") from exc
        if (
            head.authority_id != self.authority_id
            or head.lineage_id != lineage_id
            or not self._keyring.verify(
                issuer=head.authority_id,
                key_id=head.key_id,
                key_purpose="replay_claim",
                namespace=REPLAY_CLAIM_NAMESPACE,
                payload=head.signing_bytes(),
                signature=head.signature,
            )
        ):
            raise SourceBrokerError("replay authority head signature is invalid")
        return head

    def _sign_genesis_record(
        self,
        *,
        sequence: int,
        lineage_id: str,
        claim_token: str,
        plan_hash: str,
        nonce: str,
        broker_id: str,
        operation_id: str,
        previous_record_hash: str,
        effect_receipt_hash: str,
    ) -> ReplayGenesisRecord:
        unsigned = ReplayGenesisRecord(
            schema_version=3,
            contract="rquant-source-replay-genesis/v3",
            authority_id=self.authority_id,
            lineage_id=lineage_id,
            sequence=sequence,
            claim_token=claim_token,
            plan_hash=plan_hash,
            nonce=nonce,
            broker_id=broker_id,
            operation_id=operation_id,
            previous_record_hash=previous_record_hash,
            effect_receipt_hash=effect_receipt_hash,
            key_id=self._signer.key_id,
            signature="",
        )
        return unsigned.model_copy(
            update={
                "signature": self._signer.sign(
                    namespace=REPLAY_CLAIM_NAMESPACE,
                    payload=unsigned.signing_bytes(),
                )
            }
        )

    def _validate_genesis_row(
        self,
        row: sqlite3.Row,
        *,
        lineage_id: str,
        expected_sequence: int,
        previous_record_hash: str,
    ) -> tuple[ReplayGenesisRecord, EffectReceipt]:
        payload = {
            "nonce": row["nonce"],
            "plan_hash": row["plan_hash"],
            "claim_token": row["claim_token"],
            "broker_id": row["broker_id"],
        }
        try:
            receipt = self._validate_claim_receipt(
                row["receipt_json"],
                operation_id=row["operation_id"],
                payload=payload,
            )
        except SourceBrokerError as exc:
            raise SourceBrokerError(
                "replay authority genesis receipt signature or binding is invalid"
            ) from exc
        try:
            record = ReplayGenesisRecord.model_validate_json(row["record_json"])
        except ValidationError as exc:
            raise SourceBrokerError("replay authority genesis record is invalid") from exc
        expected = (
            self.authority_id,
            lineage_id,
            expected_sequence,
            row["claim_token"],
            row["plan_hash"],
            row["nonce"],
            row["broker_id"],
            row["operation_id"],
            previous_record_hash,
            receipt.receipt_hash,
        )
        actual = (
            record.authority_id,
            record.lineage_id,
            record.sequence,
            record.claim_token,
            record.plan_hash,
            record.nonce,
            record.broker_id,
            record.operation_id,
            record.previous_record_hash,
            record.effect_receipt_hash,
        )
        if actual != expected or not self._keyring.verify(
            issuer=record.authority_id,
            key_id=record.key_id,
            key_purpose="replay_claim",
            namespace=REPLAY_CLAIM_NAMESPACE,
            payload=record.signing_bytes(),
            signature=record.signature,
        ):
            raise SourceBrokerError("replay authority genesis binding is invalid")
        return record, receipt

    def _claim_binding_hash(
        self,
        *,
        operation_id: str,
        nonce: str,
        plan_hash: str,
        claim_token: str,
        broker_id: str,
    ) -> str:
        return canonical_sha256(
            {
                "contract": "rquant-source-replay-claim-binding/v1",
                "replay_authority_id": self.authority_id,
                "operation_id": operation_id,
                "nonce": nonce,
                "plan_hash": plan_hash,
                "claim_token": claim_token,
                "broker_id": broker_id,
            }
        )

    def _lineage_operation_id(
        self,
        *,
        lineage_id: str,
        previous_head_hash: str,
        next_head_hash: str,
        sequence: int,
        claim_binding_hash: str,
    ) -> str:
        return canonical_sha256(
            {
                "contract": "rquant-source-replay-lineage-operation/v1",
                "replay_authority_id": self.authority_id,
                "lineage_id": lineage_id,
                "previous_head_hash": previous_head_hash,
                "next_head_hash": next_head_hash,
                "sequence": sequence,
                "claim_binding_hash": claim_binding_hash,
            }
        )

    def _sign_lineage_envelope(
        self,
        *,
        request: ReplayLineageAdvance,
        status: Literal["pending", "applied"],
        checkpoint_receipt: ReplayLineageCheckpointReceipt | None,
        previous: ReplayLineageOutboxEnvelope | None,
    ) -> ReplayLineageOutboxEnvelope:
        unsigned = ReplayLineageOutboxEnvelope(
            schema_version=1,
            contract="rquant-source-replay-lineage-outbox/v1",
            replay_authority_id=self.authority_id,
            lineage_id=request.lineage_id,
            operation_id=request.operation_id,
            sequence=request.sequence,
            request_hash=canonical_sha256(request),
            status=status,
            checkpoint_receipt_hash=(
                checkpoint_receipt.receipt_hash if checkpoint_receipt is not None else None
            ),
            previous_envelope_hash=(previous.envelope_hash if previous is not None else None),
            transition_seq=1 if status == "pending" else 2,
            key_id=self._signer.key_id,
            signature="",
        )
        return unsigned.model_copy(
            update={
                "signature": self._signer.sign(
                    namespace=REPLAY_CLAIM_NAMESPACE,
                    payload=unsigned.signing_bytes(),
                )
            }
        )

    def _verify_lineage_envelope(self, envelope: ReplayLineageOutboxEnvelope) -> bool:
        return self._keyring.verify(
            issuer=envelope.replay_authority_id,
            key_id=envelope.key_id,
            key_purpose="replay_claim",
            namespace=REPLAY_CLAIM_NAMESPACE,
            payload=envelope.signing_bytes(),
            signature=envelope.signature,
        )

    def _validate_checkpoint_receipt_shape(
        self,
        request: ReplayLineageAdvance,
        receipt: ReplayLineageCheckpointReceipt,
    ) -> None:
        expected = (
            self._lineage_authority_id,
            request.operation_id,
            request.replay_authority_id,
            request.lineage_id,
            request.previous_head_hash,
            request.next_head_hash,
            request.sequence,
            request.claim_binding_hash,
            "applied",
        )
        actual = (
            receipt.authority_id,
            receipt.operation_id,
            receipt.replay_authority_id,
            receipt.lineage_id,
            receipt.previous_head_hash,
            receipt.next_head_hash,
            receipt.sequence,
            receipt.claim_binding_hash,
            receipt.outcome,
        )
        if actual != expected:
            raise SourceBrokerError("replay lineage checkpoint receipt binding is invalid")

    def _validate_lineage_outbox_row(
        self,
        row: sqlite3.Row,
    ) -> tuple[
        ReplayLineageAdvance,
        ReplayGenesisRecord,
        EffectReceipt,
        ReplayAuthorityHead,
        ReplayAuthorityHead,
        ReplayLineageOutboxEnvelope,
        ReplayLineageCheckpointReceipt | None,
    ]:
        try:
            request = ReplayLineageAdvance.model_validate_json(row["request_json"])
            record = ReplayGenesisRecord.model_validate_json(row["genesis_record_json"])
            previous_head = ReplayAuthorityHead.model_validate_json(row["previous_head_json"])
            next_head = ReplayAuthorityHead.model_validate_json(row["next_head_json"])
            envelope = ReplayLineageOutboxEnvelope.model_validate_json(row["envelope_json"])
        except ValidationError as exc:
            raise SourceBrokerError("replay lineage outbox structure is invalid") from exc
        if (
            request.operation_id != row["operation_id"]
            or request.sequence != int(row["sequence"])
            or record.claim_token != row["claim_token"]
            or request.replay_authority_id != self.authority_id
            or request.lineage_id != record.lineage_id
            or request.lineage_id != previous_head.lineage_id
            or request.lineage_id != next_head.lineage_id
            or request.previous_head_hash != previous_head.head_hash
            or request.next_head_hash != next_head.head_hash
            or request.operation_id
            != self._lineage_operation_id(
                lineage_id=request.lineage_id,
                previous_head_hash=request.previous_head_hash,
                next_head_hash=request.next_head_hash,
                sequence=request.sequence,
                claim_binding_hash=request.claim_binding_hash,
            )
            or request.claim_binding_hash
            != self._claim_binding_hash(
                operation_id=record.operation_id,
                nonce=record.nonce,
                plan_hash=record.plan_hash,
                claim_token=record.claim_token,
                broker_id=record.broker_id,
            )
            or next_head.record_count != request.sequence
            or next_head.head_record_hash != record.record_hash
            or record.sequence != request.sequence
            or record.previous_record_hash != previous_head.head_record_hash
        ):
            raise SourceBrokerError("replay lineage outbox candidate binding is invalid")
        self._validate_head(row["previous_head_json"], lineage_id=request.lineage_id)
        self._validate_head(row["next_head_json"], lineage_id=request.lineage_id)
        if not self._keyring.verify(
            issuer=record.authority_id,
            key_id=record.key_id,
            key_purpose="replay_claim",
            namespace=REPLAY_CLAIM_NAMESPACE,
            payload=record.signing_bytes(),
            signature=record.signature,
        ):
            raise SourceBrokerError("replay lineage outbox genesis signature is invalid")
        replay_payload = {
            "nonce": record.nonce,
            "plan_hash": record.plan_hash,
            "claim_token": record.claim_token,
            "broker_id": record.broker_id,
        }
        replay_receipt = self._validate_claim_receipt(
            row["replay_receipt_json"],
            operation_id=record.operation_id,
            payload=replay_payload,
        )
        if record.effect_receipt_hash != replay_receipt.receipt_hash:
            raise SourceBrokerError("replay lineage outbox replay receipt is invalid")
        common = (
            envelope.replay_authority_id,
            envelope.lineage_id,
            envelope.operation_id,
            envelope.sequence,
            envelope.request_hash,
            envelope.status,
        )
        expected_common = (
            self.authority_id,
            request.lineage_id,
            request.operation_id,
            request.sequence,
            canonical_sha256(request),
            row["status"],
        )
        if common != expected_common or not self._verify_lineage_envelope(envelope):
            raise SourceBrokerError("replay lineage outbox envelope is invalid")
        if row["status"] == "pending":
            if (
                row["checkpoint_receipt_json"] is not None
                or row["previous_envelope_json"] is not None
                or envelope.transition_seq != 1
            ):
                raise SourceBrokerError("pending replay lineage outbox was modified")
            return (
                request,
                record,
                replay_receipt,
                previous_head,
                next_head,
                envelope,
                None,
            )
        if row["status"] != "applied":
            raise SourceBrokerError("replay lineage outbox status is invalid")
        try:
            checkpoint_receipt = ReplayLineageCheckpointReceipt.model_validate_json(
                row["checkpoint_receipt_json"]
            )
            previous_envelope = ReplayLineageOutboxEnvelope.model_validate_json(
                row["previous_envelope_json"]
            )
        except ValidationError as exc:
            raise SourceBrokerError("applied replay lineage outbox is invalid") from exc
        self._validate_checkpoint_receipt_shape(request, checkpoint_receipt)
        if (
            previous_envelope.status != "pending"
            or previous_envelope.operation_id != request.operation_id
            or not self._verify_lineage_envelope(previous_envelope)
            or envelope.checkpoint_receipt_hash != checkpoint_receipt.receipt_hash
            or envelope.previous_envelope_hash != previous_envelope.envelope_hash
            or envelope.transition_seq != 2
        ):
            raise SourceBrokerError("replay lineage outbox transition is invalid")
        return (
            request,
            record,
            replay_receipt,
            previous_head,
            next_head,
            envelope,
            checkpoint_receipt,
        )

    def _audit_state(
        self, connection: sqlite3.Connection
    ) -> tuple[sqlite3.Row, ReplayAuthorityHead]:
        meta = connection.execute(
            "SELECT * FROM replay_authority_meta WHERE singleton = 1"
        ).fetchone()
        if (
            meta is None
            or int(meta["schema_version"]) != self._SCHEMA_VERSION
            or meta["authority_id"] != self.authority_id
            or not str(meta["lineage_id"]).strip()
        ):
            raise SourceBrokerError("replay authority metadata binding is invalid")
        lineage_id = cast(str, meta["lineage_id"])
        head = self._validate_head(meta["head_json"], lineage_id=lineage_id)
        rows = connection.execute("SELECT * FROM source_claim_genesis ORDER BY sequence").fetchall()
        if len(rows) != head.record_count:
            raise SourceBrokerError("replay authority genesis record count is invalid")
        outbox_rows = connection.execute(
            "SELECT * FROM replay_lineage_outbox ORDER BY sequence"
        ).fetchall()
        validated_outbox = [self._validate_lineage_outbox_row(row) for row in outbox_rows]
        applied = [
            item
            for row, item in zip(outbox_rows, validated_outbox, strict=True)
            if row["status"] == "applied"
        ]
        pending = [
            item
            for row, item in zip(outbox_rows, validated_outbox, strict=True)
            if row["status"] == "pending"
        ]
        if len(applied) != len(rows) or len(pending) > 1:
            raise SourceBrokerError("replay lineage outbox sequence is invalid")
        previous_hash = self._CHAIN_ROOT
        previous_head_hash: str | None = None
        for sequence, (row, outbox_item) in enumerate(zip(rows, applied, strict=True), start=1):
            record, _ = self._validate_genesis_row(
                row,
                lineage_id=lineage_id,
                expected_sequence=sequence,
                previous_record_hash=previous_hash,
            )
            request, candidate, _, previous_head, next_head, _, _ = outbox_item
            if (
                request.sequence != sequence
                or candidate.record_hash != record.record_hash
                or previous_head.record_count != sequence - 1
                or previous_head.head_record_hash != previous_hash
                or (
                    previous_head_hash is not None and previous_head.head_hash != previous_head_hash
                )
            ):
                raise SourceBrokerError("replay lineage applied chain is invalid")
            previous_hash = record.record_hash
            previous_head_hash = next_head.head_hash
        if previous_hash != head.head_record_hash:
            raise SourceBrokerError("replay authority genesis chain head is invalid")
        if applied and previous_head_hash != head.head_hash:
            raise SourceBrokerError("replay authority signed head does not match its lineage")
        if not applied and (head.record_count != 0 or head.head_record_hash != self._CHAIN_ROOT):
            raise SourceBrokerError("empty replay authority head is invalid")
        if pending:
            request, record, _, previous_head, _, _, _ = pending[0]
            if (
                request.sequence != head.record_count + 1
                or previous_head.head_hash != head.head_hash
                or record.previous_record_hash != head.head_record_hash
            ):
                raise SourceBrokerError("pending replay lineage checkpoint is not current")
        if head.record_count == 0:
            if meta["checkpoint_receipt_json"] is not None:
                raise SourceBrokerError("empty replay lineage has a checkpoint receipt")
        else:
            latest_receipt = applied[-1][-1]
            try:
                meta_receipt = ReplayLineageCheckpointReceipt.model_validate_json(
                    meta["checkpoint_receipt_json"]
                )
            except ValidationError as exc:
                raise SourceBrokerError("replay lineage current receipt is invalid") from exc
            if latest_receipt is None or meta_receipt.receipt_hash != latest_receipt.receipt_hash:
                raise SourceBrokerError("replay lineage current receipt does not match head")
        return meta, head

    def consume_once(
        self,
        *,
        operation_id: str,
        nonce: str,
        plan_hash: str,
        claim_token: str,
        broker_id: str,
    ) -> EffectReceipt:
        payload = {
            "nonce": nonce,
            "plan_hash": plan_hash,
            "claim_token": claim_token,
            "broker_id": broker_id,
        }
        expected = (claim_token, plan_hash, nonce, broker_id, operation_id)
        while True:
            self._recover_pending_checkpoints()
            existing_receipt: EffectReceipt | None = None
            pending_operation: str | None = None
            created_operation: str | None = None
            with self._connect() as connection:
                connection.execute("BEGIN IMMEDIATE")
                try:
                    meta, head = self._audit_state(connection)
                    existing = connection.execute(
                        "SELECT * FROM source_claim_genesis WHERE claim_token = ?",
                        (claim_token,),
                    ).fetchone()
                    if existing is not None:
                        actual = (
                            existing["claim_token"],
                            existing["plan_hash"],
                            existing["nonce"],
                            existing["broker_id"],
                            existing["operation_id"],
                        )
                        if actual != expected:
                            raise SourceBrokerError(
                                "replay claim genesis binding cannot be changed"
                            )
                        existing_receipt = self._validate_claim_receipt(
                            existing["receipt_json"],
                            operation_id=operation_id,
                            payload=payload,
                        )
                    else:
                        pending = connection.execute(
                            """
                            SELECT operation_id FROM replay_lineage_outbox
                            WHERE status = 'pending' ORDER BY sequence LIMIT 1
                            """
                        ).fetchone()
                        if pending is not None:
                            pending_operation = cast(str, pending["operation_id"])
                        else:
                            conflict = connection.execute(
                                """
                                SELECT claim_token FROM source_claim_genesis
                                WHERE nonce = ? OR operation_id = ?
                                """,
                                (nonce, operation_id),
                            ).fetchone()
                            if conflict is not None:
                                raise SourceBrokerError("replay authority identity conflicts")
                            receipt = self._sign_claim_receipt(
                                operation_id=operation_id,
                                payload=payload,
                            )
                            sequence = head.record_count + 1
                            lineage_id = cast(str, meta["lineage_id"])
                            record = self._sign_genesis_record(
                                sequence=sequence,
                                lineage_id=lineage_id,
                                claim_token=claim_token,
                                plan_hash=plan_hash,
                                nonce=nonce,
                                broker_id=broker_id,
                                operation_id=operation_id,
                                previous_record_hash=head.head_record_hash,
                                effect_receipt_hash=receipt.receipt_hash,
                            )
                            next_head = self._sign_head(
                                lineage_id=lineage_id,
                                record_count=sequence,
                                head_record_hash=record.record_hash,
                            )
                            claim_binding_hash = self._claim_binding_hash(
                                operation_id=operation_id,
                                nonce=nonce,
                                plan_hash=plan_hash,
                                claim_token=claim_token,
                                broker_id=broker_id,
                            )
                            lineage_operation = self._lineage_operation_id(
                                lineage_id=lineage_id,
                                previous_head_hash=head.head_hash,
                                next_head_hash=next_head.head_hash,
                                sequence=sequence,
                                claim_binding_hash=claim_binding_hash,
                            )
                            request = ReplayLineageAdvance(
                                schema_version=1,
                                contract="rquant-source-replay-lineage-advance/v1",
                                operation_id=lineage_operation,
                                replay_authority_id=self.authority_id,
                                lineage_id=lineage_id,
                                previous_head_hash=head.head_hash,
                                next_head_hash=next_head.head_hash,
                                sequence=sequence,
                                claim_binding_hash=claim_binding_hash,
                            )
                            envelope = self._sign_lineage_envelope(
                                request=request,
                                status="pending",
                                checkpoint_receipt=None,
                                previous=None,
                            )
                            inserted = connection.execute(
                                """
                                INSERT INTO replay_lineage_outbox(
                                    operation_id, sequence, claim_token, request_json,
                                    genesis_record_json, replay_receipt_json,
                                    previous_head_json, next_head_json, status,
                                    checkpoint_receipt_json, previous_envelope_json,
                                    envelope_json
                                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'pending', NULL, NULL, ?)
                                """,
                                (
                                    lineage_operation,
                                    sequence,
                                    claim_token,
                                    request.model_dump_json(),
                                    record.model_dump_json(),
                                    receipt.model_dump_json(),
                                    head.model_dump_json(),
                                    next_head.model_dump_json(),
                                    envelope.model_dump_json(),
                                ),
                            ).rowcount
                            if inserted != 1:
                                raise SourceBrokerError("replay lineage pending insert CAS failed")
                            created_operation = lineage_operation
                    connection.commit()
                except BaseException:
                    connection.rollback()
                    raise
            if existing_receipt is not None:
                self._verify_external_current()
                return existing_receipt
            operation_to_advance = created_operation or pending_operation
            if operation_to_advance is None:
                raise SourceBrokerError("replay lineage operation was not prepared")
            if created_operation is not None:
                self._fault("after_persist", created_operation)
            self._advance_pending_checkpoint(operation_to_advance)

    def verify_claim_binding(
        self,
        *,
        operation_id: str,
        nonce: str,
        plan_hash: str,
        claim_token: str,
        broker_id: str,
        receipt: EffectReceipt,
    ) -> EffectReceipt:
        payload = {
            "nonce": nonce,
            "plan_hash": plan_hash,
            "claim_token": claim_token,
            "broker_id": broker_id,
        }
        expected = (claim_token, plan_hash, nonce, broker_id, operation_id)
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                self._audit_state(connection)
                existing = connection.execute(
                    "SELECT * FROM source_claim_genesis WHERE claim_token = ?",
                    (claim_token,),
                ).fetchone()
                if existing is None:
                    raise SourceBrokerError("replay claim genesis binding is missing")
                actual = (
                    existing["claim_token"],
                    existing["plan_hash"],
                    existing["nonce"],
                    existing["broker_id"],
                    existing["operation_id"],
                )
                if actual != expected:
                    raise SourceBrokerError("replay claim genesis binding does not match")
                stored = self._validate_claim_receipt(
                    existing["receipt_json"],
                    operation_id=operation_id,
                    payload=payload,
                )
                supplied = self._validate_claim_receipt(
                    receipt.model_dump_json(),
                    operation_id=operation_id,
                    payload=payload,
                )
                if supplied.receipt_hash != stored.receipt_hash:
                    raise SourceBrokerError("replay claim receipt does not match genesis")
                connection.commit()
            except BaseException:
                connection.rollback()
                raise
        self._verify_external_current()
        return stored

    def _recover_pending_checkpoints(self) -> None:
        while True:
            with self._connect() as connection:
                connection.execute("BEGIN IMMEDIATE")
                try:
                    self._audit_state(connection)
                    pending = connection.execute(
                        """
                        SELECT operation_id FROM replay_lineage_outbox
                        WHERE status = 'pending' ORDER BY sequence LIMIT 1
                        """
                    ).fetchone()
                    connection.commit()
                except BaseException:
                    connection.rollback()
                    raise
            if pending is None:
                return
            self._advance_pending_checkpoint(cast(str, pending["operation_id"]))

    def _advance_pending_checkpoint(self, operation_id: str) -> EffectReceipt:
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                self._audit_state(connection)
                row = connection.execute(
                    "SELECT * FROM replay_lineage_outbox WHERE operation_id = ?",
                    (operation_id,),
                ).fetchone()
                if row is None:
                    raise SourceBrokerError("replay lineage outbox operation is missing")
                validated = self._validate_lineage_outbox_row(row)
                request, _, replay_receipt, _, _, _, checkpoint_receipt = validated
                connection.commit()
            except BaseException:
                connection.rollback()
                raise
        if checkpoint_receipt is not None:
            self._verify_external_checkpoint(request, checkpoint_receipt)
            return replay_receipt
        checkpoint_receipt = self._lineage_authority.compare_and_advance(
            operation_id=request.operation_id,
            replay_authority_id=request.replay_authority_id,
            lineage_id=request.lineage_id,
            previous_head_hash=request.previous_head_hash,
            next_head_hash=request.next_head_hash,
            sequence=request.sequence,
            claim_binding_hash=request.claim_binding_hash,
        )
        self._validate_checkpoint_receipt_shape(request, checkpoint_receipt)
        self._verify_external_checkpoint(request, checkpoint_receipt)
        self._fault("after_effect", operation_id)
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                meta, head = self._audit_state(connection)
                row = connection.execute(
                    "SELECT * FROM replay_lineage_outbox WHERE operation_id = ?",
                    (operation_id,),
                ).fetchone()
                if row is None:
                    raise SourceBrokerError("replay lineage outbox operation is missing")
                current = self._validate_lineage_outbox_row(row)
                (
                    current_request,
                    record,
                    replay_receipt,
                    previous_head,
                    next_head,
                    pending_envelope,
                    current_checkpoint,
                ) = current
                if current_request != request:
                    raise SourceBrokerError("replay lineage request changed during checkpoint")
                if current_checkpoint is None:
                    if (
                        head.head_hash != request.previous_head_hash
                        or previous_head.head_hash != head.head_hash
                        or request.sequence != head.record_count + 1
                    ):
                        raise SourceBrokerError("replay lineage checkpoint is stale")
                    inserted = connection.execute(
                        """
                        INSERT INTO source_claim_genesis(
                            claim_token, sequence, plan_hash, nonce, broker_id,
                            operation_id, record_json, receipt_json
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            record.claim_token,
                            record.sequence,
                            record.plan_hash,
                            record.nonce,
                            record.broker_id,
                            record.operation_id,
                            record.model_dump_json(),
                            replay_receipt.model_dump_json(),
                        ),
                    ).rowcount
                    if inserted != 1:
                        raise SourceBrokerError("replay genesis checkpoint insert CAS failed")
                    updated_meta = connection.execute(
                        """
                        UPDATE replay_authority_meta
                        SET head_json = ?, checkpoint_receipt_json = ?
                        WHERE singleton = 1 AND lineage_id = ? AND head_json = ?
                          AND checkpoint_receipt_json IS ?
                        """,
                        (
                            next_head.model_dump_json(),
                            checkpoint_receipt.model_dump_json(),
                            request.lineage_id,
                            previous_head.model_dump_json(),
                            meta["checkpoint_receipt_json"],
                        ),
                    ).rowcount
                    if updated_meta != 1:
                        raise SourceBrokerError("replay lineage head CAS failed")
                    applied_envelope = self._sign_lineage_envelope(
                        request=request,
                        status="applied",
                        checkpoint_receipt=checkpoint_receipt,
                        previous=pending_envelope,
                    )
                    updated_outbox = connection.execute(
                        """
                        UPDATE replay_lineage_outbox
                        SET status = 'applied', checkpoint_receipt_json = ?,
                            previous_envelope_json = ?, envelope_json = ?
                        WHERE operation_id = ? AND status = 'pending'
                          AND envelope_json = ?
                        """,
                        (
                            checkpoint_receipt.model_dump_json(),
                            pending_envelope.model_dump_json(),
                            applied_envelope.model_dump_json(),
                            operation_id,
                            pending_envelope.model_dump_json(),
                        ),
                    ).rowcount
                    if updated_outbox != 1:
                        raise SourceBrokerError("replay lineage outbox apply CAS failed")
                elif current_checkpoint.receipt_hash != checkpoint_receipt.receipt_hash:
                    raise SourceBrokerError("replay lineage checkpoint receipt changed")
                connection.commit()
            except BaseException:
                connection.rollback()
                raise
        self._fault("after_checkpoint", operation_id)
        self._verify_external_current()
        return replay_receipt

    def _verify_external_checkpoint(
        self,
        request: ReplayLineageAdvance,
        receipt: ReplayLineageCheckpointReceipt,
    ) -> None:
        self._validate_checkpoint_receipt_shape(request, receipt)
        self._lineage_authority.verify_current(
            replay_authority_id=request.replay_authority_id,
            lineage_id=request.lineage_id,
            head_hash=request.next_head_hash,
            sequence=request.sequence,
            receipt=receipt,
        )

    def _verify_external_current(self) -> None:
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                meta, head = self._audit_state(connection)
                try:
                    receipt = (
                        ReplayLineageCheckpointReceipt.model_validate_json(
                            meta["checkpoint_receipt_json"]
                        )
                        if meta["checkpoint_receipt_json"] is not None
                        else None
                    )
                except ValidationError as exc:
                    raise SourceBrokerError("replay lineage current receipt is invalid") from exc
                lineage_id = cast(str, meta["lineage_id"])
                connection.commit()
            except BaseException:
                connection.rollback()
                raise
        self._lineage_authority.verify_current(
            replay_authority_id=self.authority_id,
            lineage_id=lineage_id,
            head_hash=head.head_hash,
            sequence=head.record_count,
            receipt=receipt,
        )

    def _sign_claim_receipt(
        self, *, operation_id: str, payload: dict[str, object]
    ) -> EffectReceipt:
        unsigned = EffectReceipt(
            authority_id=self.authority_id,
            operation_id=operation_id,
            payload_hash=canonical_sha256(payload),
            effect="replay",
            outcome="applied",
            result_hash=canonical_sha256(None),
            key_id=self._signer.key_id,
            signature="",
        )
        return unsigned.model_copy(
            update={
                "signature": self._signer.sign(
                    namespace=REPLAY_CLAIM_NAMESPACE,
                    payload=unsigned.signing_bytes(),
                )
            }
        )

    def _validate_claim_receipt(
        self,
        receipt_json: str,
        *,
        operation_id: str,
        payload: dict[str, object],
    ) -> EffectReceipt:
        try:
            receipt = EffectReceipt.model_validate_json(receipt_json)
        except ValidationError as exc:
            raise SourceBrokerError("replay claim receipt is invalid") from exc
        if (
            receipt.authority_id != self.authority_id
            or receipt.operation_id != operation_id
            or receipt.payload_hash != canonical_sha256(payload)
            or receipt.effect != "replay"
            or receipt.outcome != "applied"
            or receipt.result_hash != canonical_sha256(None)
            or not self._keyring.verify(
                issuer=receipt.authority_id,
                key_id=receipt.key_id,
                key_purpose="replay_claim",
                namespace=REPLAY_CLAIM_NAMESPACE,
                payload=receipt.signing_bytes(),
                signature=receipt.signature,
            )
        ):
            raise SourceBrokerError("replay claim receipt signature or binding is invalid")
        return receipt


class ProviderCapability(RuntimeContractModel):
    operation: str = Field(min_length=1, max_length=200)
    request: BaseModel

    @field_validator("request", mode="before")
    @classmethod
    def require_model_instance(cls, value: object) -> object:
        if not isinstance(value, BaseModel):
            raise ValueError("provider capability requires a Pydantic request model")
        return value


class SourceProviderProtocol(Protocol):
    def dispatch(self, capability: ProviderCapability) -> BaseModel: ...


@dataclass(frozen=True)
class _ProviderBinding:
    provider: SourceProviderProtocol
    request_model: type[BaseModel]
    response_model: type[BaseModel]
    request_schema: PydanticModelSchema
    response_schema: PydanticModelSchema


_CREDENTIAL_NAMES = frozenset(
    {
        "apikey",
        "authorization",
        "bearer",
        "cookie",
        "credential",
        "endpoint",
        "header",
        "headers",
        "secret",
        "token",
        "accesstoken",
        "url",
        "uri",
    }
)
_CREDENTIAL_FRAGMENTS = (
    "apikey",
    "authorization",
    "cookie",
    "credential",
    "endpoint",
    "header",
    "secret",
    "token",
    "url",
    "uri",
)


def _normalized_field_name(value: str) -> str:
    return "".join(character.lower() for character in value if character.isalnum())


def _is_credential_field(value: str) -> bool:
    normalized = _normalized_field_name(value)
    return normalized in _CREDENTIAL_NAMES or any(
        fragment in normalized for fragment in _CREDENTIAL_FRAGMENTS
    )


_ALLOWED_PROVIDER_SCALARS = (str, bool, int, float, Decimal, date, datetime, UUID, NoneType)
_MUTABLE_PROVIDER_ORIGINS = (dict, list, set, frozenset, Mapping)


def _validate_provider_annotation(
    annotation: object,
    *,
    visited: set[type[BaseModel]],
) -> None:
    if annotation is Any or annotation is object:
        raise SourceBrokerError("provider schema contains an unsupported open type")
    if annotation in _ALLOWED_PROVIDER_SCALARS:
        return
    if isinstance(annotation, type) and issubclass(annotation, BaseModel):
        _validate_closed_model(annotation, visited=visited)
        return
    origin = get_origin(annotation)
    arguments = get_args(annotation)
    if origin in _MUTABLE_PROVIDER_ORIGINS:
        kind = "mapping" if origin in {dict, Mapping} else "mutable collection"
        raise SourceBrokerError(f"provider schema contains a {kind}")
    if origin is Annotated:
        _validate_provider_annotation(arguments[0], visited=visited)
        return
    if origin in {Union, UnionType}:
        for argument in arguments:
            _validate_provider_annotation(argument, visited=visited)
        return
    if origin is Literal:
        if not all(isinstance(value, _ALLOWED_PROVIDER_SCALARS[:-1]) for value in arguments):
            raise SourceBrokerError("provider schema literal contains an unsupported type")
        return
    if origin is tuple:
        for argument in arguments:
            if argument is Ellipsis:
                continue
            _validate_provider_annotation(argument, visited=visited)
        return
    raise SourceBrokerError("provider schema contains an unsupported type")


def _validate_closed_model(
    model: type[BaseModel],
    *,
    visited: set[type[BaseModel]] | None = None,
) -> None:
    seen = visited or set()
    if model in seen:
        return
    seen.add(model)
    if model.model_config.get("extra") != "forbid":
        raise SourceBrokerError("provider schemas must use closed Pydantic models")
    if model.model_config.get("frozen") is not True:
        raise SourceBrokerError("provider schemas must use frozen Pydantic models")
    for name, field in model.model_fields.items():
        if (
            field.alias is not None
            or field.validation_alias is not None
            or field.serialization_alias is not None
        ):
            raise SourceBrokerError("provider schema aliases are forbidden")
        if _is_credential_field(name):
            raise SourceBrokerError("provider schema contains a credential or transport field")
        _validate_provider_annotation(field.annotation, visited=seen)


class ProviderRegistry:
    def __init__(self) -> None:
        self._bindings: dict[tuple[str, str], _ProviderBinding] = {}

    def register(
        self,
        *,
        source: str,
        operation: str,
        provider: SourceProviderProtocol,
        request_model: type[BaseModel],
        response_model: type[BaseModel],
    ) -> None:
        source_id = source.strip()
        operation_id = operation.strip()
        if not source_id or not operation_id:
            raise SourceBrokerError("source and operation must be nonempty")
        _validate_closed_model(request_model)
        _validate_closed_model(response_model)
        key = (source_id, operation_id)
        if key in self._bindings:
            raise SourceBrokerError("duplicate provider capability registration")
        self._bindings[key] = _ProviderBinding(
            provider=provider,
            request_model=request_model,
            response_model=response_model,
            request_schema=PydanticModelSchema.from_model(request_model),
            response_schema=PydanticModelSchema.from_model(response_model),
        )

    def resolve(self, *, source: str, operation: str) -> _ProviderBinding:
        try:
            return self._bindings[(source, operation)]
        except KeyError as exc:
            raise SourceBrokerError("provider capability is not registered") from exc


class SourceCallReceipt(RuntimeContractModel):
    broker_id: str = Field(min_length=1, max_length=200)
    call_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    claim_token: str = Field(min_length=1, max_length=500)
    idempotency_key: str = Field(min_length=1, max_length=128)
    manifest_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    source: str = Field(min_length=1, max_length=200)
    operation: str = Field(min_length=1, max_length=200)
    call_seq: int = Field(strict=True, ge=1)
    request_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    response_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    cost: int = Field(strict=True, gt=0)
    outcome: CallOutcome
    key_id: str = Field(min_length=1, max_length=200)
    signature_algorithm: Literal["ed25519"] = "ed25519"
    signature: str

    def signing_bytes(self) -> bytes:
        return _json_bytes(self.model_dump(mode="json", exclude={"signature"}))

    @property
    def receipt_hash(self) -> str:
        return canonical_sha256(self.model_dump(mode="python"))


class SourceUseStatement(RuntimeContractModel):
    schema_version: Literal[2]
    broker_id: str = Field(min_length=1, max_length=200)
    claim_token: str = Field(min_length=1, max_length=500)
    plan_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    manifest_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    reservation_id: str = Field(min_length=1, max_length=500)
    calls_dispatched: int = Field(strict=True, ge=0)
    calls_unknown: int = Field(strict=True, ge=0)
    calls_consumed_unknown: int = Field(strict=True, ge=0)
    cost_reserved: int = Field(strict=True, ge=0)
    cost_consumed: int = Field(strict=True, ge=0)
    cost_released: int = Field(strict=True, ge=0)
    receipt_hashes: tuple[str, ...]
    key_id: str = Field(min_length=1, max_length=200)
    signature_algorithm: Literal["ed25519"] = "ed25519"
    signature: str

    @model_validator(mode="after")
    def validate_statement(self) -> SourceUseStatement:
        if self.cost_consumed + self.cost_released != self.cost_reserved:
            raise ValueError("statement costs do not reconcile")
        if self.calls_unknown > self.calls_dispatched:
            raise ValueError("unknown calls exceed dispatched calls")
        if len(self.receipt_hashes) != (self.calls_dispatched + self.calls_consumed_unknown):
            raise ValueError("statement receipts do not match dispatched calls")
        return self

    def signing_bytes(self) -> bytes:
        return _json_bytes(self.model_dump(mode="json", exclude={"signature"}))


class BrokerOutboxEnvelope(RuntimeContractModel):
    broker_id: str = Field(min_length=1, max_length=200)
    operation_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    kind: OutboxKind
    aggregate_id: str = Field(min_length=1, max_length=500)
    payload_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    status: Literal["pending", "applied"]
    result_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    effect_receipt_hash: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    previous_envelope_hash: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    transition_seq: int = Field(strict=True, ge=1, le=2)
    key_id: str = Field(min_length=1, max_length=200)
    signature_algorithm: Literal["ed25519"] = "ed25519"
    signature: str

    @model_validator(mode="after")
    def validate_transition_shape(self) -> BrokerOutboxEnvelope:
        if self.status == "pending" and (
            self.transition_seq != 1
            or self.effect_receipt_hash is not None
            or self.previous_envelope_hash is not None
        ):
            raise ValueError("pending outbox envelope has an invalid transition shape")
        if self.status == "applied" and (
            self.transition_seq != 2
            or self.effect_receipt_hash is None
            or self.previous_envelope_hash is None
        ):
            raise ValueError("applied outbox envelope has an invalid transition shape")
        return self

    def signing_bytes(self) -> bytes:
        return _json_bytes(self.model_dump(mode="json", exclude={"signature"}))

    @property
    def envelope_hash(self) -> str:
        return canonical_sha256(self.model_dump(mode="python"))


def _json_bytes(value: object) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def _json_text(value: object) -> str:
    return json.dumps(
        value,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def _validate_idempotency_key(value: str) -> str:
    if not isinstance(value, str) or _IDEMPOTENCY_KEY_PATTERN.fullmatch(value) is None:
        raise SourceBrokerError(
            "idempotency key must be 1-128 ASCII letters, digits, '.', '_', ':', or '-'"
        )
    return value


class SourceBroker:
    def __init__(
        self,
        *,
        state_path: Path,
        broker_id: str,
        quota_ledger: QuotaLedgerProtocol,
        replay_authority: ReplayAuthorityProtocol,
        provider_registry: ProviderRegistry,
        authorization_keyring: VerifyOnlyEd25519Keyring,
        receipt_signer: Ed25519ContractSigner,
        receipt_keyring: VerifyOnlyEd25519Keyring,
        quota_effect_keyring: VerifyOnlyEd25519Keyring,
        replay_claim_keyring: VerifyOnlyEd25519Keyring,
        outbox_signer: Ed25519ContractSigner,
        outbox_keyring: VerifyOnlyEd25519Keyring,
        now: Callable[[], datetime] | None = None,
        fault_injector: Callable[[str, str, str], None] | None = None,
        lease_clock: Callable[[], LeaseClockReading] | None = None,
        lease_ttl_seconds: float = 30.0,
        heartbeat_interval_seconds: float = 5.0,
    ) -> None:
        self.state_path = Path(state_path)
        self.broker_id = broker_id.strip()
        if not self.broker_id:
            raise ValueError("broker_id must be nonempty")
        if replay_authority is None:
            raise ValueError("a shared replay authority is required")
        if (
            receipt_signer.key_purpose != "broker_receipt"
            or receipt_signer.issuer != self.broker_id
        ):
            raise ValueError("receipt signer must be a broker-specific Ed25519 signer")
        if outbox_signer.key_purpose != "broker_outbox" or outbox_signer.issuer != self.broker_id:
            raise ValueError("outbox signer must be a broker-specific Ed25519 signer")
        if not receipt_keyring.allows_signer(receipt_signer):
            raise ValueError("receipt signer fingerprint is not allowed by its keyring")
        if not outbox_keyring.allows_signer(outbox_signer):
            raise ValueError("outbox signer fingerprint is not allowed by its keyring")
        self._require_signer_binding(
            signer=receipt_signer,
            keyring=receipt_keyring,
            namespace=BROKER_RECEIPT_NAMESPACE,
            label="receipt",
        )
        self._require_signer_binding(
            signer=outbox_signer,
            keyring=outbox_keyring,
            namespace=BROKER_OUTBOX_NAMESPACE,
            label="outbox",
        )
        role_keyrings: dict[KeyPurpose, VerifyOnlyEd25519Keyring] = {
            "adapter_manifest": authorization_keyring,
            "source_use_plan": authorization_keyring,
            "broker_receipt": receipt_keyring,
            "quota_effect": quota_effect_keyring,
            "replay_claim": replay_claim_keyring,
            "broker_outbox": outbox_keyring,
        }
        role_fingerprints: dict[KeyPurpose, frozenset[str]] = {}
        for purpose, keyring in role_keyrings.items():
            fingerprints = keyring.fingerprints_for_purpose(purpose)
            if not fingerprints:
                raise ValueError(f"{purpose} verify keyring has no allowed fingerprint")
            for previous_purpose, previous in role_fingerprints.items():
                if fingerprints & previous:
                    raise ValueError(
                        "signing role public key fingerprints overlap: "
                        f"{previous_purpose} and {purpose}"
                    )
            role_fingerprints[purpose] = fingerprints
        lineage_fingerprints = replay_authority.lineage_verifier_fingerprints
        if not lineage_fingerprints:
            raise ValueError("replay lineage authority has no verifier fingerprint")
        for purpose, fingerprints in role_fingerprints.items():
            if lineage_fingerprints & fingerprints:
                raise ValueError(
                    f"signing role public key fingerprints overlap: {purpose} and replay_lineage"
                )
        self._quota_ledger = quota_ledger
        self._replay_authority = replay_authority
        self._provider_registry = provider_registry
        self._authorization_keyring = authorization_keyring
        self._receipt_signer = receipt_signer
        self._receipt_keyring = receipt_keyring
        self._quota_effect_keyring = quota_effect_keyring
        self._replay_claim_keyring = replay_claim_keyring
        self._outbox_signer = outbox_signer
        self._outbox_keyring = outbox_keyring
        self._now = now or (lambda: datetime.now(UTC))
        self._fault_injector = fault_injector
        if not math.isfinite(lease_ttl_seconds) or lease_ttl_seconds <= 0:
            raise ValueError("lease ttl must be a positive finite duration")
        if not math.isfinite(heartbeat_interval_seconds) or heartbeat_interval_seconds <= 0:
            raise ValueError("lease heartbeat interval must be a positive finite duration")
        self._lease_clock = lease_clock or (
            lambda: LeaseClockReading(
                wall_time=time.time(),
                monotonic_time=time.monotonic(),
                boot_id=_DEFAULT_BOOT_ID,
            )
        )
        self._lease_ttl_seconds = lease_ttl_seconds
        self._heartbeat_interval_seconds = heartbeat_interval_seconds
        self._lease_wait_seconds = min(0.05, heartbeat_interval_seconds)
        self._instance_id = str(uuid4())
        self._active_call_ids: set[str] = set()
        self._active_calls_condition = threading.Condition()
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        self._initialize()
        self._state_id = self._load_state_id()
        self._preflight_persisted_plans()
        self._recover_open_sessions()
        self._recover_calls()
        self._recover_finalizing_sessions()

    @property
    def instance_id(self) -> str:
        return self._instance_id

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.state_path, timeout=5, isolation_level=None)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA busy_timeout = 5000")
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA journal_mode = WAL")
        connection.execute("PRAGMA synchronous = FULL")
        return connection

    def _require_signer_binding(
        self,
        *,
        signer: Ed25519ContractSigner,
        keyring: VerifyOnlyEd25519Keyring,
        namespace: str,
        label: str,
    ) -> None:
        challenge = _json_bytes(
            {
                "contract": "rquant-source-signer-binding/v1",
                "broker_id": self.broker_id,
                "key_id": signer.key_id,
                "key_purpose": signer.key_purpose,
                "public_key_fingerprint": signer.public_key_fingerprint,
            }
        )
        signature = signer.sign(namespace=namespace, payload=challenge)
        if not keyring.verify(
            issuer=signer.issuer,
            key_id=signer.key_id,
            key_purpose=signer.key_purpose,
            namespace=namespace,
            payload=challenge,
            signature=signature,
        ):
            raise ValueError(f"{label} signing client does not control its declared public key")

    @contextmanager
    def _immediate(self) -> Iterator[sqlite3.Connection]:
        connection = self._connect()
        connection.execute("BEGIN IMMEDIATE")
        try:
            yield connection
            connection.commit()
        except BaseException:
            connection.rollback()
            raise
        finally:
            connection.close()

    def _initialize(self) -> None:
        with self._immediate() as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS broker_meta (
                    singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
                    state_id TEXT NOT NULL UNIQUE
                );
                CREATE TABLE IF NOT EXISTS broker_session (
                    claim_token TEXT PRIMARY KEY,
                    plan_hash TEXT NOT NULL UNIQUE,
                    manifest_hash TEXT NOT NULL,
                    plan_json TEXT NOT NULL,
                    status TEXT NOT NULL CHECK(
                        status IN (
                            'claiming', 'reserving', 'active', 'finalizing',
                            'finalized', 'rejected'
                        )
                    ),
                    reservation_json TEXT,
                    statement_json TEXT,
                    lease_owner_id TEXT NOT NULL,
                    fencing_token INTEGER NOT NULL CHECK(fencing_token > 0),
                    heartbeat_wall REAL NOT NULL,
                    heartbeat_monotonic REAL NOT NULL,
                    heartbeat_boot_id TEXT NOT NULL,
                    lease_expires_wall REAL NOT NULL,
                    lease_expires_monotonic REAL NOT NULL,
                    version INTEGER NOT NULL DEFAULT 1 CHECK(version > 0)
                );
                CREATE TABLE IF NOT EXISTS broker_call (
                    call_id TEXT PRIMARY KEY,
                    claim_token TEXT NOT NULL REFERENCES broker_session(claim_token),
                    idempotency_key TEXT NOT NULL,
                    manifest_hash TEXT NOT NULL,
                    source TEXT NOT NULL,
                    operation TEXT NOT NULL,
                    call_seq INTEGER NOT NULL CHECK(call_seq > 0),
                    request_hash TEXT NOT NULL,
                    response_hash TEXT,
                    cost INTEGER NOT NULL CHECK(cost > 0),
                    state TEXT NOT NULL CHECK(
                        state IN (
                            'intent', 'intent_applied', 'dispatch_pending', 'dispatched',
                            'settling', 'recovering', 'success', 'failure', 'unknown',
                            'unknown_before_dispatch'
                        )
                    ),
                    outcome TEXT CHECK(
                        outcome IN (
                            'success', 'failure', 'unknown', 'unknown_before_dispatch'
                        )
                    ),
                    receipt_json TEXT,
                    receipt_hash TEXT,
                    lease_owner_id TEXT NOT NULL,
                    fencing_token INTEGER NOT NULL CHECK(fencing_token > 0),
                    heartbeat_wall REAL NOT NULL,
                    heartbeat_monotonic REAL NOT NULL,
                    heartbeat_boot_id TEXT NOT NULL,
                    lease_expires_wall REAL NOT NULL,
                    lease_expires_monotonic REAL NOT NULL,
                    version INTEGER NOT NULL DEFAULT 1 CHECK(version > 0),
                    UNIQUE(claim_token, call_seq),
                    UNIQUE(claim_token, idempotency_key)
                );
                CREATE TABLE IF NOT EXISTS broker_outbox (
                    operation_id TEXT PRIMARY KEY,
                    kind TEXT NOT NULL CHECK(
                        kind IN (
                            'replay', 'reserve', 'intent', 'dispatch',
                            'finalize', 'release', 'recover'
                        )
                    ),
                    aggregate_id TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    status TEXT NOT NULL CHECK(status IN ('pending', 'applied')),
                    result_json TEXT,
                    effect_receipt_json TEXT,
                    previous_envelope_json TEXT,
                    envelope_json TEXT NOT NULL,
                    version INTEGER NOT NULL DEFAULT 1 CHECK(version > 0)
                );
                CREATE INDEX IF NOT EXISTS broker_outbox_aggregate_idx
                ON broker_outbox(aggregate_id, status);
                """
            )
            connection.execute(
                "INSERT OR IGNORE INTO broker_meta(singleton, state_id) VALUES (1, ?)",
                (str(uuid4()),),
            )

    def _load_state_id(self) -> str:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT state_id FROM broker_meta WHERE singleton = 1"
            ).fetchone()
        if row is None:
            raise SourceBrokerError("broker state identity is unavailable")
        return cast(str, row["state_id"])

    def _fault(self, kind: str, phase: str, operation_id: str) -> None:
        if self._fault_injector is not None:
            self._fault_injector(kind, phase, operation_id)

    def _lease_values(self, reading: LeaseClockReading) -> tuple[object, ...]:
        return (
            self._instance_id,
            reading.wall_time,
            reading.monotonic_time,
            reading.boot_id,
            reading.wall_time + self._lease_ttl_seconds,
            reading.monotonic_time + self._lease_ttl_seconds,
        )

    def _lease_expired(self, row: sqlite3.Row, reading: LeaseClockReading) -> bool:
        if row["heartbeat_boot_id"] == reading.boot_id:
            return reading.monotonic_time >= float(row["lease_expires_monotonic"])
        return reading.wall_time >= float(row["lease_expires_wall"])

    def _guard_for(
        self,
        *,
        table: Literal["broker_session", "broker_call"],
        row: sqlite3.Row,
    ) -> _LeaseGuard:
        key_column: Literal["claim_token", "call_id"] = (
            "claim_token" if table == "broker_session" else "call_id"
        )
        return _LeaseGuard(
            table=table,
            key_column=key_column,
            aggregate_id=cast(str, row[key_column]),
            owner_id=cast(str, row["lease_owner_id"]),
            fencing_token=int(row["fencing_token"]),
        )

    def _assert_lease(self, connection: sqlite3.Connection, guard: _LeaseGuard) -> None:
        row = connection.execute(
            f"SELECT lease_owner_id, fencing_token FROM {guard.table} WHERE {guard.key_column} = ?",
            (guard.aggregate_id,),
        ).fetchone()
        if row is None or (
            row["lease_owner_id"] != guard.owner_id
            or int(row["fencing_token"]) != guard.fencing_token
            or guard.owner_id != self._instance_id
        ):
            raise SourceBrokerError("executor lease fence is stale")

    def _renew_lease(
        self,
        connection: sqlite3.Connection,
        guard: _LeaseGuard,
    ) -> None:
        reading = self._lease_clock()
        updated = connection.execute(
            f"""
            UPDATE {guard.table}
            SET heartbeat_wall = ?, heartbeat_monotonic = ?, heartbeat_boot_id = ?,
                lease_expires_wall = ?, lease_expires_monotonic = ?
            WHERE {guard.key_column} = ?
              AND lease_owner_id = ? AND fencing_token = ?
            """,
            (
                reading.wall_time,
                reading.monotonic_time,
                reading.boot_id,
                reading.wall_time + self._lease_ttl_seconds,
                reading.monotonic_time + self._lease_ttl_seconds,
                guard.aggregate_id,
                guard.owner_id,
                guard.fencing_token,
            ),
        ).rowcount
        if updated != 1:
            raise SourceBrokerError("executor lease heartbeat lost its fence")

    def _release_lease(
        self,
        connection: sqlite3.Connection,
        guard: _LeaseGuard,
    ) -> None:
        reading = self._lease_clock()
        updated = connection.execute(
            f"""
            UPDATE {guard.table}
            SET heartbeat_wall = ?, heartbeat_monotonic = ?, heartbeat_boot_id = ?,
                lease_expires_wall = ?, lease_expires_monotonic = ?
            WHERE {guard.key_column} = ?
              AND lease_owner_id = ? AND fencing_token = ?
            """,
            (
                reading.wall_time,
                reading.monotonic_time,
                reading.boot_id,
                reading.wall_time,
                reading.monotonic_time,
                guard.aggregate_id,
                guard.owner_id,
                guard.fencing_token,
            ),
        ).rowcount
        if updated != 1:
            raise SourceBrokerError("executor lease release lost its fence")

    def _acquire_lease(
        self,
        connection: sqlite3.Connection,
        *,
        table: Literal["broker_session", "broker_call"],
        row: sqlite3.Row,
    ) -> _LeaseGuard | None:
        existing = self._guard_for(table=table, row=row)
        if existing.owner_id == self._instance_id:
            self._renew_lease(connection, existing)
            return existing
        reading = self._lease_clock()
        if not self._lease_expired(row, reading):
            return None
        new_fence = existing.fencing_token + 1
        updated = connection.execute(
            f"""
            UPDATE {table}
            SET lease_owner_id = ?, fencing_token = ?, heartbeat_wall = ?,
                heartbeat_monotonic = ?, heartbeat_boot_id = ?, lease_expires_wall = ?,
                lease_expires_monotonic = ?
            WHERE {existing.key_column} = ? AND lease_owner_id = ?
              AND fencing_token = ? AND version = ?
            """,
            (
                self._instance_id,
                new_fence,
                reading.wall_time,
                reading.monotonic_time,
                reading.boot_id,
                reading.wall_time + self._lease_ttl_seconds,
                reading.monotonic_time + self._lease_ttl_seconds,
                existing.aggregate_id,
                existing.owner_id,
                existing.fencing_token,
                int(row["version"]),
            ),
        ).rowcount
        if updated == 0:
            return None
        if updated != 1:
            raise SourceBrokerError("executor lease takeover CAS updated multiple rows")
        return _LeaseGuard(
            table=table,
            key_column=existing.key_column,
            aggregate_id=existing.aggregate_id,
            owner_id=self._instance_id,
            fencing_token=new_fence,
        )

    def _replay_plan_anchor_in_transaction(
        self,
        connection: sqlite3.Connection,
        *,
        claim_token: str,
        session_status: str,
    ) -> tuple[str, str]:
        replay = connection.execute(
            """
            SELECT * FROM broker_outbox
            WHERE aggregate_id = ? AND kind = 'replay'
            ORDER BY rowid DESC LIMIT 1
            """,
            (claim_token,),
        ).fetchone()
        if replay is None:
            raise SourceBrokerError("persisted source plan replay anchor is missing")
        kind, payload, _, _, effect_receipt = self._validate_outbox_row(replay)
        if (
            kind != "replay"
            or set(payload) != {"nonce", "plan_hash", "claim_token", "broker_id"}
            or payload["claim_token"] != claim_token
            or payload["broker_id"] != self.broker_id
            or not isinstance(payload["plan_hash"], str)
            or not isinstance(payload["nonce"], str)
        ):
            raise SourceBrokerError("persisted source plan replay anchor binding is invalid")
        if (
            session_status in {"reserving", "active", "finalizing", "finalized"}
            and replay["status"] != "applied"
        ):
            raise SourceBrokerError("persisted source plan replay anchor is not applied")
        if replay["status"] == "applied":
            if effect_receipt is None:
                raise SourceBrokerError("persisted source plan replay receipt is missing")
            self._replay_authority.verify_claim_binding(
                operation_id=replay["operation_id"],
                nonce=cast(str, payload["nonce"]),
                plan_hash=cast(str, payload["plan_hash"]),
                claim_token=claim_token,
                broker_id=self.broker_id,
                receipt=effect_receipt,
            )
        return cast(str, payload["plan_hash"]), cast(str, payload["nonce"])

    def _validate_session_in_transaction(
        self,
        connection: sqlite3.Connection,
        claim_token: str,
    ) -> tuple[sqlite3.Row, SourceUsePlan]:
        row = connection.execute(
            "SELECT * FROM broker_session WHERE claim_token = ?",
            (claim_token,),
        ).fetchone()
        if row is None:
            raise SourceBrokerError("source session does not exist")
        anchor_plan_hash, anchor_nonce = self._replay_plan_anchor_in_transaction(
            connection,
            claim_token=claim_token,
            session_status=row["status"],
        )
        plan = self._validate_persisted_plan(
            claim_token=row["claim_token"],
            plan_hash=anchor_plan_hash,
            manifest_hash=row["manifest_hash"],
            plan_json=row["plan_json"],
        )
        if row["plan_hash"] != anchor_plan_hash or plan.nonce != anchor_nonce:
            raise SourceBrokerError("persisted source plan does not match replay anchor")
        self._fault("plan", "after_validation", claim_token)
        return row, plan

    def _validate_call_in_transaction(
        self,
        connection: sqlite3.Connection,
        call_id: str,
    ) -> tuple[sqlite3.Row, SourceUsePlan]:
        row = connection.execute(
            """
            SELECT call.*, session.reservation_json,
                   session.plan_hash AS session_plan_hash,
                   session.manifest_hash AS session_manifest_hash,
                   session.plan_json AS session_plan_json,
                   session.status AS session_status
            FROM broker_call AS call
            JOIN broker_session AS session ON session.claim_token = call.claim_token
            WHERE call.call_id = ?
            """,
            (call_id,),
        ).fetchone()
        if row is None:
            raise SourceBrokerError("source call does not exist")
        anchor_plan_hash, anchor_nonce = self._replay_plan_anchor_in_transaction(
            connection,
            claim_token=row["claim_token"],
            session_status=row["session_status"],
        )
        plan = self._validate_persisted_plan(
            claim_token=row["claim_token"],
            plan_hash=anchor_plan_hash,
            manifest_hash=row["session_manifest_hash"],
            plan_json=row["session_plan_json"],
        )
        if (
            row["session_plan_hash"] != anchor_plan_hash
            or plan.nonce != anchor_nonce
            or row["manifest_hash"] != plan.manifest_hash
        ):
            raise SourceBrokerError("persisted call manifest does not match source plan")
        self._fault("plan", "after_validation", row["claim_token"])
        return row, plan

    def _operation_id(self, *, kind: OutboxKind, aggregate_id: str, payload: object) -> str:
        return canonical_sha256(
            {
                "broker_state_id": self._state_id,
                "kind": kind,
                "aggregate_id": aggregate_id,
                "payload": payload,
            }
        )

    def _insert_outbox(
        self,
        connection: sqlite3.Connection,
        *,
        kind: OutboxKind,
        aggregate_id: str,
        payload: dict[str, object],
    ) -> str:
        operation_id = self._operation_id(
            kind=kind,
            aggregate_id=aggregate_id,
            payload=payload,
        )
        payload_json = _json_text(payload)
        envelope = self._sign_outbox_envelope(
            operation_id=operation_id,
            kind=kind,
            aggregate_id=aggregate_id,
            payload=payload,
            status="pending",
            result=None,
            effect_receipt=None,
            previous=None,
        )
        connection.execute(
            """
            INSERT OR IGNORE INTO broker_outbox(
                operation_id, kind, aggregate_id, payload_json, status, result_json,
                effect_receipt_json, previous_envelope_json, envelope_json, version
            ) VALUES (?, ?, ?, ?, 'pending', NULL, NULL, NULL, ?, 1)
            """,
            (operation_id, kind, aggregate_id, payload_json, envelope.model_dump_json()),
        )
        stored = connection.execute(
            "SELECT * FROM broker_outbox WHERE operation_id = ?",
            (operation_id,),
        ).fetchone()
        if stored is None:
            raise SourceBrokerError("outbox operation id conflicts")
        stored_kind, stored_payload, _, _, _ = self._validate_outbox_row(stored)
        if (
            stored_kind != kind
            or stored["aggregate_id"] != aggregate_id
            or stored_payload != payload
        ):
            raise SourceBrokerError("outbox operation id conflicts")
        return operation_id

    def _sign_outbox_envelope(
        self,
        *,
        operation_id: str,
        kind: OutboxKind,
        aggregate_id: str,
        payload: dict[str, object],
        status: Literal["pending", "applied"],
        result: object,
        effect_receipt: EffectReceipt | None,
        previous: BrokerOutboxEnvelope | None,
    ) -> BrokerOutboxEnvelope:
        unsigned = BrokerOutboxEnvelope(
            broker_id=self.broker_id,
            operation_id=operation_id,
            kind=kind,
            aggregate_id=aggregate_id,
            payload_hash=canonical_sha256(payload),
            status=status,
            result_hash=canonical_sha256(result),
            effect_receipt_hash=(
                effect_receipt.receipt_hash if effect_receipt is not None else None
            ),
            previous_envelope_hash=(previous.envelope_hash if previous is not None else None),
            transition_seq=1 if status == "pending" else 2,
            key_id=self._outbox_signer.key_id,
            signature="",
        )
        envelope = unsigned.model_copy(
            update={
                "signature": self._outbox_signer.sign(
                    namespace=BROKER_OUTBOX_NAMESPACE,
                    payload=unsigned.signing_bytes(),
                )
            }
        )
        if not self._verify_outbox_envelope(envelope):
            raise SourceBrokerError("outbox signer is not trusted by its keyring")
        return envelope

    def _verify_outbox_envelope(self, envelope: BrokerOutboxEnvelope) -> bool:
        return self._outbox_keyring.verify(
            issuer=envelope.broker_id,
            key_id=envelope.key_id,
            key_purpose="broker_outbox",
            namespace=BROKER_OUTBOX_NAMESPACE,
            payload=envelope.signing_bytes(),
            signature=envelope.signature,
        )

    def _validate_effect_receipt(
        self,
        *,
        receipt: EffectReceipt,
        operation_id: str,
        kind: OutboxKind,
        payload: dict[str, object],
        result: object,
    ) -> None:
        if kind == "reserve":
            try:
                QuotaReservation.model_validate(result)
            except ValidationError as exc:
                raise SourceBrokerError("quota reserve effect result is invalid") from exc
        elif result is not None:
            raise SourceBrokerError("non-reserve quota effect returned an unexpected result")
        expected = (
            operation_id,
            canonical_sha256(payload),
            kind,
            "applied",
            canonical_sha256(result),
        )
        actual = (
            receipt.operation_id,
            receipt.payload_hash,
            receipt.effect,
            receipt.outcome,
            receipt.result_hash,
        )
        if expected != actual:
            raise SourceBrokerError("effect receipt does not match the outbox operation")
        if kind == "replay":
            keyring = self._replay_claim_keyring
            namespace = REPLAY_CLAIM_NAMESPACE
            purpose = "replay_claim"
            if receipt.authority_id != self._replay_authority.authority_id:
                raise SourceBrokerError("replay claim receipt authority does not match")
        else:
            keyring = self._quota_effect_keyring
            namespace = QUOTA_EFFECT_NAMESPACE
            purpose = "quota_effect"
        if not keyring.verify(
            issuer=receipt.authority_id,
            key_id=receipt.key_id,
            key_purpose=purpose,
            namespace=namespace,
            payload=receipt.signing_bytes(),
            signature=receipt.signature,
        ):
            raise SourceBrokerError("effect receipt signature is invalid")

    def _validate_outbox_row(
        self, row: sqlite3.Row
    ) -> tuple[
        OutboxKind,
        dict[str, object],
        object,
        BrokerOutboxEnvelope,
        EffectReceipt | None,
    ]:
        try:
            payload_value = json.loads(row["payload_json"])
            if not isinstance(payload_value, dict):
                raise TypeError
            payload = cast(dict[str, object], payload_value)
            envelope = BrokerOutboxEnvelope.model_validate_json(row["envelope_json"])
        except (KeyError, TypeError, json.JSONDecodeError, ValidationError) as exc:
            raise SourceBrokerError("outbox integrity data is invalid") from exc
        kind = cast(OutboxKind, row["kind"])
        operation_id = cast(str, row["operation_id"])
        if (
            self._operation_id(
                kind=kind,
                aggregate_id=row["aggregate_id"],
                payload=payload,
            )
            != operation_id
        ):
            raise SourceBrokerError("outbox operation id does not match persisted payload")
        common = (
            self.broker_id,
            operation_id,
            kind,
            row["aggregate_id"],
            canonical_sha256(payload),
            row["status"],
        )
        observed_common = (
            envelope.broker_id,
            envelope.operation_id,
            envelope.kind,
            envelope.aggregate_id,
            envelope.payload_hash,
            envelope.status,
        )
        if common != observed_common or not self._verify_outbox_envelope(envelope):
            raise SourceBrokerError("outbox integrity envelope is invalid")
        if row["status"] == "pending":
            if (
                row["result_json"] is not None
                or row["effect_receipt_json"] is not None
                or row["previous_envelope_json"] is not None
                or envelope.result_hash != canonical_sha256(None)
                or envelope.transition_seq != 1
            ):
                raise SourceBrokerError("outbox integrity pending state was modified")
            return kind, payload, None, envelope, None
        if row["status"] != "applied":
            raise SourceBrokerError("outbox integrity status is invalid")
        try:
            result = json.loads(row["result_json"])
            receipt = EffectReceipt.model_validate_json(row["effect_receipt_json"])
            previous = BrokerOutboxEnvelope.model_validate_json(row["previous_envelope_json"])
        except (TypeError, json.JSONDecodeError, ValidationError) as exc:
            raise SourceBrokerError("outbox integrity applied state is invalid") from exc
        expected_previous = (
            self.broker_id,
            operation_id,
            kind,
            row["aggregate_id"],
            canonical_sha256(payload),
            "pending",
            canonical_sha256(None),
            None,
            None,
            1,
        )
        actual_previous = (
            previous.broker_id,
            previous.operation_id,
            previous.kind,
            previous.aggregate_id,
            previous.payload_hash,
            previous.status,
            previous.result_hash,
            previous.effect_receipt_hash,
            previous.previous_envelope_hash,
            previous.transition_seq,
        )
        if actual_previous != expected_previous or not self._verify_outbox_envelope(previous):
            raise SourceBrokerError("outbox integrity previous envelope is invalid")
        if (
            envelope.result_hash != canonical_sha256(result)
            or envelope.effect_receipt_hash != receipt.receipt_hash
            or envelope.previous_envelope_hash != previous.envelope_hash
            or envelope.transition_seq != 2
        ):
            raise SourceBrokerError("outbox integrity applied envelope does not match columns")
        self._validate_effect_receipt(
            receipt=receipt,
            operation_id=operation_id,
            kind=kind,
            payload=payload,
            result=result,
        )
        return kind, payload, result, envelope, receipt

    def _outbox_for(self, *, aggregate_id: str, kind: OutboxKind) -> sqlite3.Row:
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT * FROM broker_outbox
                WHERE aggregate_id = ? AND kind = ?
                ORDER BY rowid DESC LIMIT 1
                """,
                (aggregate_id, kind),
            ).fetchone()
        if row is None:
            raise SourceBrokerError(f"{kind} outbox operation is missing")
        return row

    def _apply_outbox(
        self,
        operation_id: str,
        *,
        guard: _LeaseGuard | None = None,
    ) -> object:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM broker_outbox WHERE operation_id = ?",
                (operation_id,),
            ).fetchone()
        if row is None:
            raise SourceBrokerError("outbox operation does not exist")
        kind, _, _, _, _ = self._validate_outbox_row(row)
        self._fault(kind, "before_effect_check", operation_id)
        with self._immediate() as connection:
            current = connection.execute(
                "SELECT * FROM broker_outbox WHERE operation_id = ?",
                (operation_id,),
            ).fetchone()
            if current is None:
                raise SourceBrokerError("outbox operation disappeared during apply")
            kind, payload, current_result, current_envelope, _ = self._validate_outbox_row(current)
            expected_table: Literal["broker_session", "broker_call"]
            if kind in {"replay", "reserve", "release"}:
                expected_table = "broker_session"
                self._validate_session_in_transaction(connection, current["aggregate_id"])
            else:
                expected_table = "broker_call"
                self._validate_call_in_transaction(connection, current["aggregate_id"])
            if current["status"] == "applied":
                return current_result
            if (
                guard is None
                or guard.table != expected_table
                or guard.aggregate_id != current["aggregate_id"]
            ):
                raise SourceBrokerError("pending outbox effect requires its executor lease fence")
            self._assert_lease(connection, guard)
            self._renew_lease(connection, guard)
            result, effect_receipt = self._invoke_outbox_effect(
                operation_id=operation_id,
                kind=kind,
                payload=payload,
            )
            self._validate_effect_receipt(
                receipt=effect_receipt,
                operation_id=operation_id,
                kind=kind,
                payload=payload,
                result=result,
            )
            self._fault(kind, "after_effect", operation_id)
            applied_envelope = self._sign_outbox_envelope(
                operation_id=operation_id,
                kind=kind,
                aggregate_id=current["aggregate_id"],
                payload=payload,
                status="applied",
                result=result,
                effect_receipt=effect_receipt,
                previous=current_envelope,
            )
            updated = connection.execute(
                """
                UPDATE broker_outbox
                SET status = 'applied', result_json = ?, effect_receipt_json = ?,
                    previous_envelope_json = ?, envelope_json = ?, version = version + 1
                WHERE operation_id = ? AND status = 'pending' AND envelope_json = ?
                """,
                (
                    _json_text(result),
                    effect_receipt.model_dump_json(),
                    current_envelope.model_dump_json(),
                    applied_envelope.model_dump_json(),
                    operation_id,
                    current["envelope_json"],
                ),
            ).rowcount
            if updated != 1:
                observed = connection.execute(
                    "SELECT * FROM broker_outbox WHERE operation_id = ?",
                    (operation_id,),
                ).fetchone()
                if observed is None:
                    raise SourceBrokerError("outbox apply CAS failed")
                _, _, observed_result, _, _ = self._validate_outbox_row(observed)
                if observed["status"] != "applied":
                    raise SourceBrokerError("outbox apply CAS failed")
                result = observed_result
        self._fault(kind, "after_apply", operation_id)
        return result

    def _invoke_outbox_effect(
        self,
        *,
        operation_id: str,
        kind: OutboxKind,
        payload: dict[str, object],
    ) -> tuple[object, EffectReceipt]:
        if kind == "replay":
            return None, self._replay_authority.consume_once(
                operation_id=operation_id,
                nonce=cast(str, payload["nonce"]),
                plan_hash=cast(str, payload["plan_hash"]),
                claim_token=cast(str, payload["claim_token"]),
                broker_id=cast(str, payload["broker_id"]),
            )
        if kind == "reserve":
            response = self._quota_ledger.reserve(
                operation_id=operation_id,
                claim_token=cast(str, payload["claim_token"]),
                source=cast(str, payload["source"]),
                units=cast(int, payload["units"]),
            )
            result = (
                response.result.model_dump(mode="json") if response.result is not None else None
            )
            return result, response.receipt
        if kind == "intent":
            response = self._quota_ledger.record_intent(
                operation_id=operation_id,
                reservation_id=cast(str, payload["reservation_id"]),
                call_id=cast(str, payload["call_id"]),
                claim_token=cast(str, payload["claim_token"]),
                idempotency_key=cast(str, payload["idempotency_key"]),
                manifest_hash=cast(str, payload["manifest_hash"]),
                source=cast(str, payload["source"]),
                operation=cast(str, payload["operation"]),
                request_hash=cast(str, payload["request_hash"]),
                cost=cast(int, payload["cost"]),
            )
        elif kind == "dispatch":
            response = self._quota_ledger.mark_dispatched(
                operation_id=operation_id,
                reservation_id=cast(str, payload["reservation_id"]),
                call_id=cast(str, payload["call_id"]),
            )
        elif kind == "finalize":
            response = self._quota_ledger.finalize(
                operation_id=operation_id,
                reservation_id=cast(str, payload["reservation_id"]),
                call_id=cast(str, payload["call_id"]),
                outcome=cast(str, payload["outcome"]),
            )
        elif kind == "recover":
            response = self._quota_ledger.recover(
                operation_id=operation_id,
                reservation_id=cast(str, payload["reservation_id"]),
                call_id=cast(str, payload["call_id"]),
            )
        else:
            response = self._quota_ledger.release_unused(
                operation_id=operation_id,
                reservation_id=cast(str, payload["reservation_id"]),
            )
        return response.result, response.receipt

    def _validate_plan(self, plan: SourceUsePlan, *, require_current: bool) -> SourceUsePlan:
        try:
            validated = SourceUsePlan.model_validate(plan.model_dump(mode="python"))
        except ValidationError as exc:
            raise SourceBrokerError("source use plan is structurally invalid") from exc
        if not validated.verify(self._authorization_keyring):
            raise SourceBrokerError("source use plan is not authorized")
        if validated.audience != self.broker_id:
            raise SourceBrokerError("source use plan audience does not match broker")
        if validated.single_use_authority_id != self._replay_authority.authority_id:
            raise SourceBrokerError("source use plan replay authority does not match")
        observed = self._now()
        if observed.tzinfo is None or observed.utcoffset() is None:
            raise SourceBrokerError("broker clock must be timezone-aware")
        if observed < validated.not_before:
            raise SourceBrokerError("source use plan is not yet valid")
        if require_current and observed >= validated.expires_at:
            raise SourceBrokerError("source use plan is expired")
        if (
            validated.network != "provider"
            or validated.source is None
            or validated.operation is None
        ):
            raise SourceBrokerError("source broker only accepts provider plans")
        binding = self._provider_registry.resolve(
            source=validated.source,
            operation=validated.operation,
        )
        if (
            binding.request_schema != validated.request_schema
            or binding.response_schema != validated.response_schema
        ):
            raise SourceBrokerError("provider schemas do not match signed plan")
        return validated

    def _validate_persisted_plan(
        self,
        *,
        claim_token: str,
        plan_hash: str,
        manifest_hash: str,
        plan_json: str,
    ) -> SourceUsePlan:
        try:
            stored = SourceUsePlan.model_validate_json(plan_json)
        except ValidationError as exc:
            raise SourceBrokerError("persisted source plan is structurally invalid") from exc
        validated = self._validate_plan(stored, require_current=False)
        if (
            validated.plan_hash != plan_hash
            or validated.manifest_hash != manifest_hash
            or validated.claim_token != claim_token
            or validated.audience != self.broker_id
        ):
            raise SourceBrokerError("persisted source plan integrity binding is invalid")
        return validated

    def _preflight_persisted_plans(self) -> None:
        with self._immediate() as connection:
            claims = connection.execute(
                "SELECT claim_token FROM broker_session ORDER BY claim_token"
            ).fetchall()
            for row in claims:
                self._validate_session_in_transaction(connection, row["claim_token"])

    def start(self, plan: SourceUsePlan) -> QuotaReservation:
        plan = self._validate_plan(plan, require_current=True)
        with self._immediate() as connection:
            plan = self._validate_plan(plan, require_current=True)
            row = connection.execute(
                "SELECT * FROM broker_session WHERE claim_token = ?",
                (plan.claim_token,),
            ).fetchone()
            if row is None:
                conflict = connection.execute(
                    "SELECT claim_token FROM broker_session WHERE plan_hash = ?",
                    (plan.plan_hash,),
                ).fetchone()
                if conflict is not None:
                    raise SourceBrokerError("source use plan replay was rejected")
                replay_payload: dict[str, object] = {
                    "nonce": plan.nonce,
                    "plan_hash": plan.plan_hash,
                    "claim_token": plan.claim_token,
                    "broker_id": self.broker_id,
                }
                lease = self._lease_values(self._lease_clock())
                inserted = connection.execute(
                    """
                    INSERT INTO broker_session(
                        claim_token, plan_hash, manifest_hash, plan_json, status,
                        reservation_json, statement_json, lease_owner_id, fencing_token,
                        heartbeat_wall, heartbeat_monotonic, heartbeat_boot_id,
                        lease_expires_wall, lease_expires_monotonic, version
                    ) VALUES (?, ?, ?, ?, 'claiming', NULL, NULL, ?, 1, ?, ?, ?, ?, ?, 1)
                    """,
                    (
                        plan.claim_token,
                        plan.plan_hash,
                        plan.manifest_hash,
                        plan.model_dump_json(),
                        *lease,
                    ),
                ).rowcount
                if inserted != 1:
                    raise SourceBrokerError("source session insert CAS failed")
                replay_operation = self._insert_outbox(
                    connection,
                    kind="replay",
                    aggregate_id=plan.claim_token,
                    payload=replay_payload,
                )
            else:
                if row["plan_hash"] != plan.plan_hash:
                    raise SourceBrokerError("claim token is bound to a different source plan")
                replay_operation = None
        if replay_operation is not None:
            self._fault("replay", "after_persist", replay_operation)
        reservation = self._resume_start(plan.claim_token, wait_for_lease=True)
        if reservation is None:
            raise SourceBrokerError("source session executor lease was not acquired")
        return reservation

    def _resume_start(
        self,
        claim_token: str,
        *,
        wait_for_lease: bool,
    ) -> QuotaReservation | None:
        while True:
            with self._immediate() as connection:
                session, persisted_plan = self._validate_session_in_transaction(
                    connection, claim_token
                )
                status = cast(str, session["status"])
                if status in {"active", "finalizing", "finalized"}:
                    if session["reservation_json"] is None:
                        raise SourceBrokerError("source session reservation is missing")
                    reservation = QuotaReservation.model_validate_json(session["reservation_json"])
                    operation = connection.execute(
                        """
                        SELECT * FROM broker_outbox
                        WHERE aggregate_id = ? AND kind = 'reserve'
                        ORDER BY rowid DESC LIMIT 1
                        """,
                        (claim_token,),
                    ).fetchone()
                    guard = None
                elif status == "rejected":
                    raise SourceBrokerError("source use plan replay was rejected")
                else:
                    guard = self._acquire_lease(
                        connection,
                        table="broker_session",
                        row=session,
                    )
                    if guard is None:
                        operation = None
                        reservation = None
                    else:
                        operation = connection.execute(
                            """
                            SELECT * FROM broker_outbox
                            WHERE aggregate_id = ? AND kind = ?
                            ORDER BY rowid DESC LIMIT 1
                            """,
                            (claim_token, "replay" if status == "claiming" else "reserve"),
                        ).fetchone()
                        reservation = None
            if guard is None and status not in {"active", "finalizing", "finalized"}:
                if not wait_for_lease:
                    return None
                time.sleep(self._lease_wait_seconds)
                continue
            if operation is None:
                raise SourceBrokerError(f"{status} source session outbox operation is missing")
            if status in {"active", "finalizing", "finalized"}:
                if reservation is None:
                    raise SourceBrokerError("source session reservation is missing")
                effect_result = QuotaReservation.model_validate(
                    self._apply_outbox(operation["operation_id"])
                )
                if effect_result != reservation:
                    raise SourceBrokerError(
                        "source session reservation does not match signed quota effect"
                    )
                return reservation
            if guard is None:
                raise SourceBrokerError("source session executor lease is missing")
            if status == "claiming":
                try:
                    self._apply_outbox(operation["operation_id"], guard=guard)
                except SourceBrokerError as exc:
                    with self._immediate() as connection:
                        current, _ = self._validate_session_in_transaction(connection, claim_token)
                        self._assert_lease(connection, guard)
                        updated = connection.execute(
                            """
                            UPDATE broker_session SET status = 'rejected', version = version + 1
                            WHERE claim_token = ? AND status = 'claiming'
                              AND lease_owner_id = ? AND fencing_token = ?
                            """,
                            (claim_token, guard.owner_id, guard.fencing_token),
                        ).rowcount
                        if updated != 1:
                            if current["status"] != "rejected":
                                raise SourceBrokerError(
                                    "source session rejection CAS failed"
                                ) from exc
                        else:
                            self._release_lease(connection, guard)
                    raise
                with self._immediate() as connection:
                    current, current_plan = self._validate_session_in_transaction(
                        connection, claim_token
                    )
                    self._assert_lease(connection, guard)
                    if current["status"] != "claiming":
                        continue
                    reserve_payload: dict[str, object] = {
                        "claim_token": claim_token,
                        "source": current_plan.source,
                        "units": current_plan.cost_per_call * current_plan.max_calls,
                    }
                    updated = connection.execute(
                        """
                        UPDATE broker_session SET status = 'reserving', version = version + 1
                        WHERE claim_token = ? AND status = 'claiming'
                          AND lease_owner_id = ? AND fencing_token = ?
                        """,
                        (claim_token, guard.owner_id, guard.fencing_token),
                    ).rowcount
                    if updated == 1:
                        reserve_operation = self._insert_outbox(
                            connection,
                            kind="reserve",
                            aggregate_id=claim_token,
                            payload=reserve_payload,
                        )
                    else:
                        raise SourceBrokerError("source claim CAS failed")
                if reserve_operation is not None:
                    self._fault("reserve", "after_persist", reserve_operation)
                continue
            if status == "reserving":
                result = self._apply_outbox(operation["operation_id"], guard=guard)
                reservation = QuotaReservation.model_validate(result)
                reservation_json = reservation.model_dump_json()
                with self._immediate() as connection:
                    current, current_plan = self._validate_session_in_transaction(
                        connection, claim_token
                    )
                    self._assert_lease(connection, guard)
                    required = current_plan.cost_per_call * current_plan.max_calls
                    if (
                        reservation.claim_token != claim_token
                        or reservation.source != current_plan.source
                        or reservation.reserved_units < required
                    ):
                        raise SourceBrokerError("quota reservation does not match source claim")
                    if current["status"] != "reserving":
                        continue
                    updated = connection.execute(
                        """
                        UPDATE broker_session
                        SET status = 'active', reservation_json = ?, version = version + 1
                        WHERE claim_token = ? AND status = 'reserving'
                          AND lease_owner_id = ? AND fencing_token = ?
                        """,
                        (
                            reservation_json,
                            claim_token,
                            guard.owner_id,
                            guard.fencing_token,
                        ),
                    ).rowcount
                    if updated != 1:
                        raise SourceBrokerError("reservation claim CAS failed")
                    self._release_lease(connection, guard)
                continue
            raise SourceBrokerError("source session has an invalid state")

    def call(
        self,
        plan: SourceUsePlan,
        request: BaseModel,
        *,
        idempotency_key: str,
    ) -> SourceCallReceipt:
        idempotency_key = _validate_idempotency_key(idempotency_key)
        plan = self._validate_plan(plan, require_current=True)
        binding = self._provider_registry.resolve(source=plan.source, operation=plan.operation)
        if not isinstance(request, binding.request_model):
            raise SourceBrokerError("provider request must use the registered Pydantic model")
        try:
            validated_request = binding.request_model.model_validate(
                request.model_dump(mode="python")
            )
        except ValidationError as exc:
            raise SourceBrokerError("provider request is invalid") from exc
        capability = ProviderCapability(
            operation=plan.operation,
            request=validated_request.model_copy(deep=True),
        )
        request_hash = canonical_sha256(capability.request)
        reservation = self.start(plan)
        self._fault("call", "after_snapshot", plan.claim_token)
        source = cast(str, plan.source)
        operation = cast(str, plan.operation)
        with self._immediate() as connection:
            session, persisted_plan = self._validate_session_in_transaction(
                connection, plan.claim_token
            )
            if persisted_plan.plan_hash != plan.plan_hash:
                raise SourceBrokerError("source session does not match call plan")
            existing = connection.execute(
                """
                SELECT * FROM broker_call
                WHERE claim_token = ? AND idempotency_key = ?
                """,
                (plan.claim_token, idempotency_key),
            ).fetchone()
            if existing is not None:
                self._validate_call_binding(
                    row=existing,
                    plan=plan,
                    idempotency_key=idempotency_key,
                    request_hash=request_hash,
                    source=source,
                    operation=operation,
                )
                created = False
                call_id = cast(str, existing["call_id"])
                call_seq = int(existing["call_seq"])
                intent_operation = None
                call_guard = None
            else:
                if session["status"] != "active":
                    raise SourceBrokerError("source session is not active or is finalized")
                latest = connection.execute(
                    """
                    SELECT COALESCE(MAX(call_seq), 0) AS latest
                    FROM broker_call WHERE claim_token = ?
                    """,
                    (plan.claim_token,),
                ).fetchone()
                call_seq = int(latest["latest"]) + 1
                if call_seq > plan.max_calls:
                    raise SourceBrokerError("source use plan maximum call count is exhausted")
                call_id = canonical_sha256(
                    {
                        "claim_token": plan.claim_token,
                        "plan_hash": plan.plan_hash,
                        "idempotency_key": idempotency_key,
                    }
                )
                lease = self._lease_values(self._lease_clock())
                inserted = connection.execute(
                    """
                    INSERT INTO broker_call(
                        call_id, claim_token, idempotency_key, manifest_hash,
                        source, operation, call_seq, request_hash, response_hash,
                        cost, state, outcome, receipt_json, receipt_hash,
                        lease_owner_id, fencing_token, heartbeat_wall,
                        heartbeat_monotonic, heartbeat_boot_id, lease_expires_wall,
                        lease_expires_monotonic, version
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, NULL, ?, 'intent',
                              NULL, NULL, NULL, ?, 1, ?, ?, ?, ?, ?, 1)
                    """,
                    (
                        call_id,
                        plan.claim_token,
                        idempotency_key,
                        plan.manifest_hash,
                        source,
                        operation,
                        call_seq,
                        request_hash,
                        plan.cost_per_call,
                        *lease,
                    ),
                ).rowcount
                if inserted != 1:
                    raise SourceBrokerError("source call idempotency CAS failed")
                intent_payload: dict[str, object] = {
                    "reservation_id": reservation.reservation_id,
                    "call_id": call_id,
                    "claim_token": plan.claim_token,
                    "idempotency_key": idempotency_key,
                    "manifest_hash": plan.manifest_hash,
                    "source": source,
                    "operation": operation,
                    "request_hash": request_hash,
                    "cost": plan.cost_per_call,
                }
                intent_operation = self._insert_outbox(
                    connection,
                    kind="intent",
                    aggregate_id=call_id,
                    payload=intent_payload,
                )
                created = True
                call_guard = _LeaseGuard(
                    table="broker_call",
                    key_column="call_id",
                    aggregate_id=call_id,
                    owner_id=self._instance_id,
                    fencing_token=1,
                )
                with self._active_calls_condition:
                    self._active_call_ids.add(call_id)
        if not created:
            return self._resume_idempotent_call(
                plan=plan,
                call_id=call_id,
                idempotency_key=idempotency_key,
                request_hash=request_hash,
                source=source,
                operation=operation,
            )
        if intent_operation is None:
            raise SourceBrokerError("new source call intent operation is missing")
        if call_guard is None:
            raise SourceBrokerError("new source call executor lease is missing")
        try:
            return self._execute_new_call(
                plan=plan,
                reservation=reservation,
                binding=binding,
                capability=capability,
                call_id=call_id,
                call_seq=call_seq,
                idempotency_key=idempotency_key,
                source=source,
                operation=operation,
                request_hash=request_hash,
                intent_operation=intent_operation,
                guard=call_guard,
            )
        finally:
            with self._active_calls_condition:
                self._active_call_ids.discard(call_id)
                self._active_calls_condition.notify_all()

    def _execute_new_call(
        self,
        *,
        plan: SourceUsePlan,
        reservation: QuotaReservation,
        binding: _ProviderBinding,
        capability: ProviderCapability,
        call_id: str,
        call_seq: int,
        idempotency_key: str,
        source: str,
        operation: str,
        request_hash: str,
        intent_operation: str,
        guard: _LeaseGuard,
    ) -> SourceCallReceipt:
        self._fault("intent", "after_persist", intent_operation)
        self._apply_outbox(intent_operation, guard=guard)
        with self._immediate() as connection:
            row, persisted_plan = self._validate_call_in_transaction(connection, call_id)
            self._assert_lease(connection, guard)
            if persisted_plan.plan_hash != plan.plan_hash:
                raise SourceBrokerError("source session does not match call plan")
            updated = connection.execute(
                """
                UPDATE broker_call SET state = 'intent_applied', version = version + 1
                WHERE call_id = ? AND state = 'intent'
                  AND lease_owner_id = ? AND fencing_token = ?
                """,
                (call_id, guard.owner_id, guard.fencing_token),
            ).rowcount
            if updated != 1:
                raise SourceBrokerError("call intent CAS failed")
            if row["session_status"] != "active":
                raise SourceBrokerError("source session stopped before dispatch")
            dispatch_payload: dict[str, object] = {
                "reservation_id": reservation.reservation_id,
                "call_id": call_id,
            }
            dispatch_operation = self._insert_outbox(
                connection,
                kind="dispatch",
                aggregate_id=call_id,
                payload=dispatch_payload,
            )
            updated = connection.execute(
                """
                UPDATE broker_call SET state = 'dispatch_pending', version = version + 1
                WHERE call_id = ? AND state = 'intent_applied'
                  AND lease_owner_id = ? AND fencing_token = ?
                """,
                (call_id, guard.owner_id, guard.fencing_token),
            ).rowcount
            if updated != 1:
                raise SourceBrokerError("call dispatch preparation CAS failed")
        self._fault("dispatch", "after_persist", dispatch_operation)
        self._apply_outbox(dispatch_operation, guard=guard)
        with self._immediate() as connection:
            row, persisted_plan = self._validate_call_in_transaction(connection, call_id)
            self._assert_lease(connection, guard)
            if persisted_plan.plan_hash != plan.plan_hash:
                raise SourceBrokerError("source session does not match call plan")
            if row["session_status"] != "active":
                raise SourceBrokerError("source session stopped before provider dispatch")
            updated = connection.execute(
                """
                UPDATE broker_call SET state = 'dispatched', version = version + 1
                WHERE call_id = ? AND state = 'dispatch_pending'
                  AND lease_owner_id = ? AND fencing_token = ?
                """,
                (call_id, guard.owner_id, guard.fencing_token),
            ).rowcount
            if updated != 1:
                raise SourceBrokerError("provider dispatch CAS failed")
            self._renew_lease(connection, guard)
        with self._provider_heartbeat(guard) as lease_lost:
            try:
                response = binding.provider.dispatch(capability)
                if not isinstance(response, binding.response_model):
                    raise ValueError("provider response type mismatch")
                response_model = binding.response_model.model_validate(
                    response.model_dump(mode="python")
                )
                response_hash = canonical_sha256(response_model)
                outcome: CallOutcome = "success"
            except Exception:
                response_hash = canonical_sha256(None)
                outcome = "failure"
        if lease_lost.is_set():
            raise SourceBrokerError("provider executor lease fence was taken over")
        return self._settle_call(
            reservation=reservation,
            call_id=call_id,
            claim_token=plan.claim_token,
            idempotency_key=idempotency_key,
            manifest_hash=plan.manifest_hash,
            source=source,
            operation=operation,
            call_seq=call_seq,
            request_hash=request_hash,
            response_hash=response_hash,
            cost=plan.cost_per_call,
            outcome=outcome,
            guard=guard,
        )

    @contextmanager
    def _provider_heartbeat(self, guard: _LeaseGuard) -> Iterator[threading.Event]:
        stop = threading.Event()
        lease_lost = threading.Event()

        def heartbeat() -> None:
            while not stop.wait(self._heartbeat_interval_seconds):
                try:
                    with self._immediate() as connection:
                        self._renew_lease(connection, guard)
                except (sqlite3.Error, SourceBrokerError):
                    lease_lost.set()
                    return

        thread = threading.Thread(target=heartbeat, daemon=True)
        thread.start()
        try:
            yield lease_lost
        finally:
            stop.set()
            thread.join(timeout=max(1.0, self._heartbeat_interval_seconds * 2))

    def _validate_call_binding(
        self,
        *,
        row: sqlite3.Row,
        plan: SourceUsePlan,
        idempotency_key: str,
        request_hash: str,
        source: str,
        operation: str,
    ) -> None:
        expected_call_id = canonical_sha256(
            {
                "claim_token": plan.claim_token,
                "plan_hash": plan.plan_hash,
                "idempotency_key": idempotency_key,
            }
        )
        expected = (
            expected_call_id,
            plan.claim_token,
            idempotency_key,
            plan.manifest_hash,
            source,
            operation,
            request_hash,
            plan.cost_per_call,
        )
        actual = (
            row["call_id"],
            row["claim_token"],
            row["idempotency_key"],
            row["manifest_hash"],
            row["source"],
            row["operation"],
            row["request_hash"],
            int(row["cost"]),
        )
        if actual != expected:
            raise SourceBrokerError(
                "idempotency key binding conflicts with a different request or operation"
            )

    def _resume_idempotent_call(
        self,
        *,
        plan: SourceUsePlan,
        call_id: str,
        idempotency_key: str,
        request_hash: str,
        source: str,
        operation: str,
    ) -> SourceCallReceipt:
        terminal = {"success", "failure", "unknown", "unknown_before_dispatch"}
        while True:
            with self._immediate() as connection:
                row, persisted_plan = self._validate_call_in_transaction(connection, call_id)
                if persisted_plan.plan_hash != plan.plan_hash:
                    raise SourceBrokerError("source session does not match call plan")
                self._validate_call_binding(
                    row=row,
                    plan=plan,
                    idempotency_key=idempotency_key,
                    request_hash=request_hash,
                    source=source,
                    operation=operation,
                )
                if row["state"] in terminal:
                    return self._validate_receipt_row(plan=plan, row=row)
                with self._active_calls_condition:
                    locally_active = call_id in self._active_call_ids
                if row["lease_owner_id"] == self._instance_id and locally_active:
                    guard = None
                else:
                    guard = self._acquire_lease(
                        connection,
                        table="broker_call",
                        row=row,
                    )
            if guard is None:
                with self._active_calls_condition:
                    self._active_calls_condition.wait(timeout=self._lease_wait_seconds)
                continue
            self._recover_call(call_id, guard=guard)

    def _validate_call_intent_outbox(
        self,
        *,
        row: sqlite3.Row,
        reservation_id: str,
        connection: sqlite3.Connection | None = None,
    ) -> None:
        if connection is None:
            intent = self._outbox_for(aggregate_id=row["call_id"], kind="intent")
        else:
            intent = connection.execute(
                """
                SELECT * FROM broker_outbox
                WHERE aggregate_id = ? AND kind = 'intent'
                ORDER BY rowid DESC LIMIT 1
                """,
                (row["call_id"],),
            ).fetchone()
            if intent is None:
                raise SourceBrokerError("intent outbox operation is missing")
        kind, payload, _, _, _ = self._validate_outbox_row(intent)
        expected = {
            "reservation_id": reservation_id,
            "call_id": row["call_id"],
            "claim_token": row["claim_token"],
            "idempotency_key": row["idempotency_key"],
            "manifest_hash": row["manifest_hash"],
            "source": row["source"],
            "operation": row["operation"],
            "request_hash": row["request_hash"],
            "cost": int(row["cost"]),
        }
        if kind != "intent" or payload != expected:
            raise SourceBrokerError(
                "idempotent call binding does not match its signed intent outbox"
            )

    def _settle_call(
        self,
        *,
        reservation: QuotaReservation,
        call_id: str,
        claim_token: str,
        idempotency_key: str,
        manifest_hash: str,
        source: str,
        operation: str,
        call_seq: int,
        request_hash: str,
        response_hash: str,
        cost: int,
        outcome: Literal["success", "failure"],
        guard: _LeaseGuard,
    ) -> SourceCallReceipt:
        receipt = self._sign_receipt(
            call_id=call_id,
            claim_token=claim_token,
            idempotency_key=idempotency_key,
            manifest_hash=manifest_hash,
            source=source,
            operation=operation,
            call_seq=call_seq,
            request_hash=request_hash,
            response_hash=response_hash,
            cost=cost,
            outcome=outcome,
        )
        payload: dict[str, object] = {
            "reservation_id": reservation.reservation_id,
            "call_id": call_id,
            "outcome": outcome,
        }
        with self._immediate() as connection:
            self._validate_call_in_transaction(connection, call_id)
            self._assert_lease(connection, guard)
            updated = connection.execute(
                """
                UPDATE broker_call
                SET response_hash = ?, state = 'settling', outcome = ?,
                    receipt_json = ?, receipt_hash = ?, version = version + 1
                WHERE call_id = ? AND state = 'dispatched'
                  AND lease_owner_id = ? AND fencing_token = ?
                """,
                (
                    response_hash,
                    outcome,
                    receipt.model_dump_json(),
                    receipt.receipt_hash,
                    call_id,
                    guard.owner_id,
                    guard.fencing_token,
                ),
            ).rowcount
            if updated != 1:
                raise SourceBrokerError("call settlement CAS failed")
            operation_id = self._insert_outbox(
                connection,
                kind="finalize",
                aggregate_id=call_id,
                payload=payload,
            )
        self._fault("finalize", "after_persist", operation_id)
        self._apply_outbox(operation_id, guard=guard)
        self._finish_call(
            call_id=call_id,
            expected="settling",
            outcome=outcome,
            guard=guard,
        )
        return receipt

    def _finish_call(
        self,
        *,
        call_id: str,
        expected: str,
        outcome: CallOutcome,
        guard: _LeaseGuard,
    ) -> None:
        with self._immediate() as connection:
            self._validate_call_in_transaction(connection, call_id)
            self._assert_lease(connection, guard)
            updated = connection.execute(
                """
                UPDATE broker_call SET state = ?, version = version + 1
                WHERE call_id = ? AND state = ? AND outcome = ?
                  AND lease_owner_id = ? AND fencing_token = ?
                """,
                (
                    outcome,
                    call_id,
                    expected,
                    outcome,
                    guard.owner_id,
                    guard.fencing_token,
                ),
            ).rowcount
            if updated != 1:
                observed = connection.execute(
                    "SELECT state FROM broker_call WHERE call_id = ?",
                    (call_id,),
                ).fetchone()
                if observed is None or observed["state"] != outcome:
                    raise SourceBrokerError("call completion CAS failed")
            else:
                self._release_lease(connection, guard)

    def _sign_receipt(
        self,
        *,
        call_id: str,
        claim_token: str,
        idempotency_key: str,
        manifest_hash: str,
        source: str,
        operation: str,
        call_seq: int,
        request_hash: str,
        response_hash: str,
        cost: int,
        outcome: CallOutcome,
    ) -> SourceCallReceipt:
        unsigned = SourceCallReceipt(
            broker_id=self.broker_id,
            call_id=call_id,
            claim_token=claim_token,
            idempotency_key=idempotency_key,
            manifest_hash=manifest_hash,
            source=source,
            operation=operation,
            call_seq=call_seq,
            request_hash=request_hash,
            response_hash=response_hash,
            cost=cost,
            outcome=outcome,
            key_id=self._receipt_signer.key_id,
            signature="",
        )
        receipt = unsigned.model_copy(
            update={
                "signature": self._receipt_signer.sign(
                    namespace=BROKER_RECEIPT_NAMESPACE,
                    payload=unsigned.signing_bytes(),
                )
            }
        )
        if not self.verify_receipt(receipt):
            raise SourceBrokerError("broker receipt signer is not trusted by receipt keyring")
        return receipt

    def verify_receipt(self, receipt: SourceCallReceipt) -> bool:
        return self._receipt_keyring.verify(
            issuer=receipt.broker_id,
            key_id=receipt.key_id,
            key_purpose="broker_receipt",
            namespace=BROKER_RECEIPT_NAMESPACE,
            payload=receipt.signing_bytes(),
            signature=receipt.signature,
        )

    def _recover_open_sessions(self) -> None:
        with self._connect() as connection:
            sessions = tuple(
                row["claim_token"]
                for row in connection.execute(
                    """
                    SELECT claim_token FROM broker_session
                    WHERE status IN ('claiming', 'reserving')
                    """
                ).fetchall()
            )
        for claim_token in sessions:
            self._resume_start(claim_token, wait_for_lease=False)

    def _recover_calls(self) -> None:
        with self._connect() as connection:
            call_ids = tuple(
                row["call_id"]
                for row in connection.execute(
                    """
                    SELECT call_id FROM broker_call
                    WHERE state NOT IN (
                        'success', 'failure', 'unknown', 'unknown_before_dispatch'
                    )
                    ORDER BY claim_token, call_seq
                    """
                ).fetchall()
            )
        for call_id in call_ids:
            self._fault("recover", "after_snapshot", call_id)
            with self._immediate() as connection:
                row, _ = self._validate_call_in_transaction(connection, call_id)
                if row["state"] in {
                    "success",
                    "failure",
                    "unknown",
                    "unknown_before_dispatch",
                }:
                    continue
                guard = self._acquire_lease(
                    connection,
                    table="broker_call",
                    row=row,
                )
            if guard is not None:
                self._recover_call(call_id, guard=guard)

    def _recover_call(self, call_id: str, *, guard: _LeaseGuard) -> None:
        terminal = {"success", "failure", "unknown", "unknown_before_dispatch"}
        while True:
            with self._immediate() as connection:
                row, persisted_plan = self._validate_call_in_transaction(connection, call_id)
                self._assert_lease(connection, guard)
                self._validate_call_binding(
                    row=row,
                    plan=persisted_plan,
                    idempotency_key=row["idempotency_key"],
                    request_hash=row["request_hash"],
                    source=row["source"],
                    operation=row["operation"],
                )
                reservation = QuotaReservation.model_validate_json(row["reservation_json"])
                self._validate_call_intent_outbox(
                    row=row,
                    reservation_id=reservation.reservation_id,
                    connection=connection,
                )
                state = cast(str, row["state"])
                if state in terminal:
                    return
                if state in {"settling", "recovering"}:
                    kind: OutboxKind = "finalize" if state == "settling" else "recover"
                    operation = connection.execute(
                        """
                        SELECT operation_id FROM broker_outbox
                        WHERE aggregate_id = ? AND kind = ?
                        ORDER BY rowid DESC LIMIT 1
                        """,
                        (call_id, kind),
                    ).fetchone()
                    effects: list[sqlite3.Row] = []
                elif state in {"intent", "intent_applied", "dispatch_pending", "dispatched"}:
                    operation = None
                    effects = connection.execute(
                        """
                        SELECT operation_id FROM broker_outbox
                        WHERE aggregate_id = ? AND kind IN ('intent', 'dispatch')
                    ORDER BY rowid
                        """,
                        (call_id,),
                    ).fetchall()
                else:
                    raise SourceBrokerError("source call has an invalid recovery state")
            if operation is not None:
                self._apply_outbox(operation["operation_id"], guard=guard)
                self._finish_call(
                    call_id=call_id,
                    expected=state,
                    outcome=cast(CallOutcome, row["outcome"]),
                    guard=guard,
                )
                return
            for operation in effects:
                self._apply_outbox(operation["operation_id"], guard=guard)
            with self._immediate() as connection:
                row, persisted_plan = self._validate_call_in_transaction(connection, call_id)
                self._assert_lease(connection, guard)
                state = cast(str, row["state"])
                if state in terminal:
                    return
                if state not in {"intent", "intent_applied", "dispatch_pending", "dispatched"}:
                    continue
                reservation = QuotaReservation.model_validate_json(row["reservation_json"])
                outcome: CallOutcome = (
                    "unknown" if state == "dispatched" else "unknown_before_dispatch"
                )
                response_hash = canonical_sha256(None)
                receipt = self._sign_receipt(
                    call_id=call_id,
                    claim_token=row["claim_token"],
                    idempotency_key=row["idempotency_key"],
                    manifest_hash=row["manifest_hash"],
                    source=row["source"],
                    operation=row["operation"],
                    call_seq=int(row["call_seq"]),
                    request_hash=row["request_hash"],
                    response_hash=response_hash,
                    cost=int(row["cost"]),
                    outcome=outcome,
                )
                payload: dict[str, object] = {
                    "reservation_id": reservation.reservation_id,
                    "call_id": call_id,
                }
                updated = connection.execute(
                    """
                    UPDATE broker_call
                    SET response_hash = ?, state = 'recovering', outcome = ?,
                        receipt_json = ?, receipt_hash = ?, version = version + 1
                    WHERE call_id = ? AND state = ? AND version = ?
                      AND lease_owner_id = ? AND fencing_token = ?
                    """,
                    (
                        response_hash,
                        outcome,
                        receipt.model_dump_json(),
                        receipt.receipt_hash,
                        call_id,
                        state,
                        int(row["version"]),
                        guard.owner_id,
                        guard.fencing_token,
                    ),
                ).rowcount
                if updated == 0:
                    continue
                if updated != 1:
                    raise SourceBrokerError("call recovery CAS updated multiple rows")
                operation_id = self._insert_outbox(
                    connection,
                    kind="recover",
                    aggregate_id=call_id,
                    payload=payload,
                )
            self._fault("recover", "after_persist", operation_id)
            self._apply_outbox(operation_id, guard=guard)
            self._finish_call(
                call_id=call_id,
                expected="recovering",
                outcome=outcome,
                guard=guard,
            )
            return

    def finalize(self, plan: SourceUsePlan) -> SourceUseStatement:
        plan = self._validate_plan(plan, require_current=False)
        while True:
            with self._immediate() as connection:
                session, persisted_plan = self._validate_session_in_transaction(
                    connection, plan.claim_token
                )
                if persisted_plan.plan_hash != plan.plan_hash:
                    raise SourceBrokerError("source session does not match finalization plan")
                if session["status"] == "finalized":
                    return self._validated_statement_in_transaction(
                        connection=connection,
                        session=session,
                        plan=persisted_plan,
                    )
                if session["status"] not in {"active", "finalizing"}:
                    raise SourceBrokerError("source session is not active for finalization")
                guard = self._acquire_lease(
                    connection,
                    table="broker_session",
                    row=session,
                )
                if guard is None or session["status"] == "finalizing":
                    release_operation = None
                else:
                    calls = connection.execute(
                        "SELECT * FROM broker_call WHERE claim_token = ? ORDER BY call_seq",
                        (plan.claim_token,),
                    ).fetchall()
                    if any(
                        row["state"]
                        not in {"success", "failure", "unknown", "unknown_before_dispatch"}
                        for row in calls
                    ):
                        raise SourceBrokerError("source session has in-flight calls")
                    receipts = self._validate_receipts(plan=persisted_plan, rows=calls)
                    reservation = QuotaReservation.model_validate_json(session["reservation_json"])
                    cost_reserved = persisted_plan.cost_per_call * persisted_plan.max_calls
                    cost_consumed = sum(receipt.cost for receipt in receipts)
                    cost_released = cost_reserved - cost_consumed
                    if cost_released < 0:
                        raise SourceBrokerError("source use exceeds reserved quota")
                    statement = self._sign_statement(
                        claim_token=persisted_plan.claim_token,
                        plan_hash=persisted_plan.plan_hash,
                        manifest_hash=persisted_plan.manifest_hash,
                        reservation_id=reservation.reservation_id,
                        calls_dispatched=sum(
                            receipt.outcome != "unknown_before_dispatch" for receipt in receipts
                        ),
                        calls_unknown=sum(receipt.outcome == "unknown" for receipt in receipts),
                        calls_consumed_unknown=sum(
                            receipt.outcome == "unknown_before_dispatch" for receipt in receipts
                        ),
                        cost_reserved=cost_reserved,
                        cost_consumed=cost_consumed,
                        cost_released=cost_released,
                        receipt_hashes=tuple(receipt.receipt_hash for receipt in receipts),
                    )
                    updated = connection.execute(
                        """
                        UPDATE broker_session
                        SET status = 'finalizing', statement_json = ?, version = version + 1
                        WHERE claim_token = ? AND status = 'active'
                          AND lease_owner_id = ? AND fencing_token = ?
                        """,
                        (
                            statement.model_dump_json(),
                            plan.claim_token,
                            guard.owner_id,
                            guard.fencing_token,
                        ),
                    ).rowcount
                    if updated != 1:
                        raise SourceBrokerError("source finalization CAS failed")
                    release_operation = self._insert_outbox(
                        connection,
                        kind="release",
                        aggregate_id=plan.claim_token,
                        payload={"reservation_id": reservation.reservation_id},
                    )
            if guard is None:
                time.sleep(self._lease_wait_seconds)
                continue
            if release_operation is not None:
                self._fault("release", "after_persist", release_operation)
            return self._resume_finalize(
                plan.claim_token,
                guard=guard,
                expected_plan_hash=plan.plan_hash,
            )

    def _resume_finalize(
        self,
        claim_token: str,
        *,
        guard: _LeaseGuard,
        expected_plan_hash: str | None = None,
    ) -> SourceUseStatement:
        with self._immediate() as connection:
            session, persisted_plan = self._validate_session_in_transaction(connection, claim_token)
            self._assert_lease(connection, guard)
            if expected_plan_hash is not None and persisted_plan.plan_hash != expected_plan_hash:
                raise SourceBrokerError("source session does not match finalization plan")
            if session["status"] != "finalizing":
                raise SourceBrokerError("source session is not finalizing")
            operation = connection.execute(
                """
                SELECT operation_id FROM broker_outbox
                WHERE aggregate_id = ? AND kind = 'release'
                ORDER BY rowid DESC LIMIT 1
                """,
                (claim_token,),
            ).fetchone()
        if operation is None:
            raise SourceBrokerError("release outbox operation is missing")
        self._apply_outbox(operation["operation_id"], guard=guard)
        with self._immediate() as connection:
            observed, persisted_plan = self._validate_session_in_transaction(
                connection, claim_token
            )
            self._assert_lease(connection, guard)
            if expected_plan_hash is not None and persisted_plan.plan_hash != expected_plan_hash:
                raise SourceBrokerError("source session does not match finalization plan")
            updated = connection.execute(
                """
                UPDATE broker_session SET status = 'finalized', version = version + 1
                WHERE claim_token = ? AND status = 'finalizing'
                  AND lease_owner_id = ? AND fencing_token = ?
                """,
                (claim_token, guard.owner_id, guard.fencing_token),
            ).rowcount
            if updated != 1:
                observed, persisted_plan = self._validate_session_in_transaction(
                    connection, claim_token
                )
                self._assert_lease(connection, guard)
                if observed["status"] != "finalized" or (
                    expected_plan_hash is not None
                    and persisted_plan.plan_hash != expected_plan_hash
                ):
                    raise SourceBrokerError("source release CAS failed")
                return self._validated_statement_in_transaction(
                    connection=connection,
                    session=observed,
                    plan=persisted_plan,
                )
            self._release_lease(connection, guard)
            observed = connection.execute(
                "SELECT * FROM broker_session WHERE claim_token = ?",
                (claim_token,),
            ).fetchone()
            if observed is None:
                raise SourceBrokerError("source final session is missing")
            return self._validated_statement_in_transaction(
                connection=connection,
                session=observed,
                plan=persisted_plan,
            )

    def _validated_statement_in_transaction(
        self,
        *,
        connection: sqlite3.Connection,
        session: sqlite3.Row,
        plan: SourceUsePlan,
    ) -> SourceUseStatement:
        if session["statement_json"] is None:
            raise SourceBrokerError("source final statement is missing")
        try:
            statement = SourceUseStatement.model_validate_json(session["statement_json"])
        except ValidationError as exc:
            raise SourceBrokerError("source final statement schema is invalid") from exc
        if not self.verify_statement(statement):
            raise SourceBrokerError("source final statement cannot be verified")
        calls = connection.execute(
            "SELECT * FROM broker_call WHERE claim_token = ? ORDER BY call_seq",
            (plan.claim_token,),
        ).fetchall()
        receipts = self._validate_receipts(plan=plan, rows=calls)
        self._validate_statement_integrity(
            statement=statement,
            plan=plan,
            session=session,
            receipts=receipts,
        )
        return statement

    def _recover_finalizing_sessions(self) -> None:
        with self._connect() as connection:
            claims = tuple(
                row["claim_token"]
                for row in connection.execute(
                    "SELECT claim_token FROM broker_session WHERE status = 'finalizing'"
                ).fetchall()
            )
        for claim_token in claims:
            with self._immediate() as connection:
                session, _ = self._validate_session_in_transaction(connection, claim_token)
                guard = self._acquire_lease(
                    connection,
                    table="broker_session",
                    row=session,
                )
            if guard is not None:
                self._resume_finalize(claim_token, guard=guard)

    def _validate_receipts(
        self,
        *,
        plan: SourceUsePlan,
        rows: list[sqlite3.Row],
    ) -> tuple[SourceCallReceipt, ...]:
        expected_sequences = list(range(1, len(rows) + 1))
        if [int(row["call_seq"]) for row in rows] != expected_sequences:
            raise SourceBrokerError("receipt integrity requires continuous call sequence")
        receipts: list[SourceCallReceipt] = []
        receipt_hashes: set[str] = set()
        for row in rows:
            receipt = self._validate_receipt_row(plan=plan, row=row)
            if receipt.receipt_hash in receipt_hashes:
                raise SourceBrokerError("receipt integrity hash is duplicated")
            receipt_hashes.add(receipt.receipt_hash)
            receipts.append(receipt)
        return tuple(receipts)

    def _validate_receipt_row(
        self,
        *,
        plan: SourceUsePlan,
        row: sqlite3.Row,
    ) -> SourceCallReceipt:
        terminal = {"success", "failure", "unknown", "unknown_before_dispatch"}
        if row["receipt_json"] is None or row["receipt_hash"] is None:
            raise SourceBrokerError("receipt integrity data is missing")
        expected_call_id = canonical_sha256(
            {
                "claim_token": plan.claim_token,
                "plan_hash": plan.plan_hash,
                "idempotency_key": row["idempotency_key"],
            }
        )
        if (
            row["claim_token"] != plan.claim_token
            or row["manifest_hash"] != plan.manifest_hash
            or row["source"] != plan.source
            or row["operation"] != plan.operation
            or row["call_id"] != expected_call_id
            or row["state"] not in terminal
            or row["state"] != row["outcome"]
            or row["response_hash"] is None
            or int(row["cost"]) != plan.cost_per_call
        ):
            raise SourceBrokerError("receipt integrity call facts do not match plan")
        try:
            receipt = SourceCallReceipt.model_validate_json(row["receipt_json"])
        except ValidationError as exc:
            raise SourceBrokerError("receipt integrity model is invalid") from exc
        expected = (
            self.broker_id,
            row["call_id"],
            plan.claim_token,
            row["idempotency_key"],
            plan.manifest_hash,
            row["source"],
            row["operation"],
            int(row["call_seq"]),
            row["request_hash"],
            row["response_hash"],
            int(row["cost"]),
            row["outcome"],
        )
        actual = (
            receipt.broker_id,
            receipt.call_id,
            receipt.claim_token,
            receipt.idempotency_key,
            receipt.manifest_hash,
            receipt.source,
            receipt.operation,
            receipt.call_seq,
            receipt.request_hash,
            receipt.response_hash,
            receipt.cost,
            receipt.outcome,
        )
        if actual != expected or receipt.receipt_hash != row["receipt_hash"]:
            raise SourceBrokerError("receipt integrity does not match call row")
        if not self.verify_receipt(receipt):
            raise SourceBrokerError("receipt integrity signature is invalid")
        return receipt

    def _validate_statement_integrity(
        self,
        *,
        statement: SourceUseStatement,
        plan: SourceUsePlan,
        session: sqlite3.Row,
        receipts: tuple[SourceCallReceipt, ...],
    ) -> None:
        if session["reservation_json"] is None:
            raise SourceBrokerError("statement integrity reservation is missing")
        reservation = QuotaReservation.model_validate_json(session["reservation_json"])
        cost_reserved = plan.cost_per_call * plan.max_calls
        cost_consumed = sum(receipt.cost for receipt in receipts)
        expected = (
            2,
            self.broker_id,
            plan.claim_token,
            plan.plan_hash,
            plan.manifest_hash,
            reservation.reservation_id,
            sum(receipt.outcome != "unknown_before_dispatch" for receipt in receipts),
            sum(receipt.outcome == "unknown" for receipt in receipts),
            sum(receipt.outcome == "unknown_before_dispatch" for receipt in receipts),
            cost_reserved,
            cost_consumed,
            cost_reserved - cost_consumed,
            tuple(receipt.receipt_hash for receipt in receipts),
        )
        actual = (
            statement.schema_version,
            statement.broker_id,
            statement.claim_token,
            statement.plan_hash,
            statement.manifest_hash,
            statement.reservation_id,
            statement.calls_dispatched,
            statement.calls_unknown,
            statement.calls_consumed_unknown,
            statement.cost_reserved,
            statement.cost_consumed,
            statement.cost_released,
            statement.receipt_hashes,
        )
        if actual != expected:
            raise SourceBrokerError("statement integrity does not match source session")

    def _sign_statement(
        self,
        *,
        claim_token: str,
        plan_hash: str,
        manifest_hash: str,
        reservation_id: str,
        calls_dispatched: int,
        calls_unknown: int,
        calls_consumed_unknown: int,
        cost_reserved: int,
        cost_consumed: int,
        cost_released: int,
        receipt_hashes: tuple[str, ...],
    ) -> SourceUseStatement:
        unsigned = SourceUseStatement(
            schema_version=2,
            broker_id=self.broker_id,
            claim_token=claim_token,
            plan_hash=plan_hash,
            manifest_hash=manifest_hash,
            reservation_id=reservation_id,
            calls_dispatched=calls_dispatched,
            calls_unknown=calls_unknown,
            calls_consumed_unknown=calls_consumed_unknown,
            cost_reserved=cost_reserved,
            cost_consumed=cost_consumed,
            cost_released=cost_released,
            receipt_hashes=receipt_hashes,
            key_id=self._receipt_signer.key_id,
            signature="",
        )
        statement = unsigned.model_copy(
            update={
                "signature": self._receipt_signer.sign(
                    namespace=BROKER_STATEMENT_NAMESPACE,
                    payload=unsigned.signing_bytes(),
                )
            }
        )
        if not self.verify_statement(statement):
            raise SourceBrokerError("broker statement signer is not trusted")
        return statement

    def verify_statement(self, statement: SourceUseStatement) -> bool:
        return self._receipt_keyring.verify(
            issuer=statement.broker_id,
            key_id=statement.key_id,
            key_purpose="broker_receipt",
            namespace=BROKER_STATEMENT_NAMESPACE,
            payload=statement.signing_bytes(),
            signature=statement.signature,
        )
