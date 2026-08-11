"""Authenticated cache/outbox for an external resource-journal anti-rollback root."""

from __future__ import annotations

import json
import re
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Literal, Protocol

from pydantic import Field, ValidationError, model_validator

from rquant.runtime_contracts import RuntimeContractModel, canonical_sha256

RESOURCE_JOURNAL_SIGNING_PURPOSE = "resource-admission-effect"
RESOURCE_JOURNAL_HIGH_WATER_PURPOSE = "resource-journal-high-water"
RESOURCE_JOURNAL_HEAD_NAMESPACE = "rquant-resource-admission-journal-head/v1"
RESOURCE_JOURNAL_ANTI_ROLLBACK_RECEIPT_NAMESPACE = (
    "rquant-resource-journal-anti-rollback-root-receipt/v1"
)
RESOURCE_JOURNAL_HIGH_WATER_ZERO_HASH = "0" * 64
RESOURCE_JOURNAL_SIGNATURE_ALGORITHM = "ed25519"
TRUSTED_RESOURCE_ROLE_PURPOSES = frozenset(
    {
        "adapter_manifest",
        "broker_outbox",
        "broker_receipt",
        "quota_effect",
        "replay_claim",
        RESOURCE_JOURNAL_HIGH_WATER_PURPOSE,
        RESOURCE_JOURNAL_SIGNING_PURPOSE,
        "source_use_plan",
        "source_use_plan_v2",
    }
)

_SCHEMA_VERSION = 2
_APPLICATION_ID = 0x52524A48
_META_TABLE = "resource_journal_high_water_meta"
_STATE_TABLE = "resource_journal_high_water_state"
_OPERATION_TABLE = "resource_journal_high_water_operation"
_PENDING_TABLE = "resource_journal_high_water_pending"
_TABLE_SQL = {
    _META_TABLE: """
        CREATE TABLE resource_journal_high_water_meta (
            singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
            schema_version INTEGER NOT NULL CHECK (schema_version = 2),
            authority_id TEXT NOT NULL,
            mode TEXT NOT NULL CHECK (mode IN ('production', 'test-standalone')),
            trusted_role_inventory_hash TEXT NOT NULL,
            journal_issuer TEXT NOT NULL,
            journal_verifier_fingerprints_json TEXT NOT NULL,
            root_authority_id TEXT,
            root_issuer TEXT,
            root_verifier_fingerprints_json TEXT NOT NULL,
            operation_count INTEGER NOT NULL CHECK (operation_count >= 0),
            journal_root TEXT NOT NULL
        ) STRICT
    """,
    _STATE_TABLE: """
        CREATE TABLE resource_journal_high_water_state (
            journal_authority_id TEXT PRIMARY KEY,
            receipt_json TEXT NOT NULL
        ) STRICT
    """,
    _OPERATION_TABLE: """
        CREATE TABLE resource_journal_high_water_operation (
            operation_id TEXT PRIMARY KEY,
            operation_index INTEGER NOT NULL UNIQUE CHECK (operation_index > 0),
            request_json TEXT NOT NULL,
            request_hash TEXT NOT NULL,
            receipt_json TEXT NOT NULL,
            previous_operation_hash TEXT NOT NULL,
            operation_hash TEXT NOT NULL
        ) STRICT
    """,
    _PENDING_TABLE: """
        CREATE TABLE resource_journal_high_water_pending (
            operation_id TEXT PRIMARY KEY,
            journal_authority_id TEXT NOT NULL UNIQUE,
            request_json TEXT NOT NULL,
            request_hash TEXT NOT NULL
        ) STRICT
    """,
}

_SIGNED_HEAD_FIELDS = frozenset(
    {
        "authority_id",
        "lineage_id",
        "genesis_hash",
        "keyring_policy_hash",
        "sequence",
        "entry_hash",
        "previous_head_hash",
        "materialized_state_root",
        "issuer",
        "key_id",
        "key_purpose",
        "namespace",
        "signature_algorithm",
        "public_key_fingerprint",
        "signature",
    }
)


class ResourceJournalHighWaterError(RuntimeError):
    """The cache, candidate head, or external anti-rollback proof is untrusted."""


class TrustedRoleInventory:
    """Closed, complete fingerprint ownership map across every trusted role."""

    def __init__(
        self,
        *,
        role_fingerprints: dict[str, frozenset[str]] | None,
    ) -> None:
        if not isinstance(role_fingerprints, dict):
            raise ValueError("trusted role inventory is required")
        purposes = frozenset(role_fingerprints)
        if purposes != TRUSTED_RESOURCE_ROLE_PURPOSES:
            raise ValueError("trusted role inventory must contain the complete fixed role set")
        normalized: dict[str, frozenset[str]] = {}
        owner_by_fingerprint: dict[str, str] = {}
        for purpose in sorted(TRUSTED_RESOURCE_ROLE_PURPOSES):
            values = role_fingerprints[purpose]
            if not isinstance(values, frozenset) or not values:
                raise ValueError(
                    f"trusted role inventory purpose {purpose!r} must be closed and nonempty"
                )
            fingerprints = frozenset(value.strip().lower() for value in values)
            if any(not _is_sha256(value) for value in fingerprints):
                raise ValueError("trusted role inventory contains an invalid fingerprint")
            for fingerprint in fingerprints:
                owner = owner_by_fingerprint.setdefault(fingerprint, purpose)
                if owner != purpose:
                    raise ValueError(
                        "trusted role inventory fingerprint cannot be reused across purposes"
                    )
            normalized[purpose] = fingerprints
        self._role_fingerprints = normalized
        self.policy_hash = canonical_sha256(
            {
                "contract": "rquant-trusted-role-inventory/v1",
                "roles": {
                    purpose: sorted(fingerprints)
                    for purpose, fingerprints in sorted(normalized.items())
                },
            }
        )

    def fingerprints_for_purpose(self, purpose: str) -> frozenset[str]:
        try:
            return self._role_fingerprints[purpose]
        except KeyError as exc:
            raise ValueError("trusted role inventory purpose is unknown") from exc

    def as_json_value(self) -> dict[str, list[str]]:
        return {
            purpose: sorted(fingerprints)
            for purpose, fingerprints in sorted(self._role_fingerprints.items())
        }


class ResourceJournalHighWaterCheckpoint(RuntimeContractModel):
    """One signed resource journal head submitted to the external root."""

    schema_version: Literal[1]
    contract: Literal["rquant-resource-journal-high-water-checkpoint/v1"]
    journal_authority_id: str = Field(min_length=1, max_length=200)
    lineage_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    sequence: int = Field(strict=True, ge=0)
    previous_head_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    head_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    materialized_state_root: str = Field(pattern=r"^[0-9a-f]{64}$")
    signed_head_json: str = Field(min_length=2)

    @model_validator(mode="after")
    def validate_signed_head_bindings(self) -> ResourceJournalHighWaterCheckpoint:
        try:
            payload = json.loads(self.signed_head_json)
        except (TypeError, ValueError) as exc:
            raise ValueError("signed resource journal head is malformed") from exc
        if (
            not isinstance(payload, dict)
            or frozenset(payload) != _SIGNED_HEAD_FIELDS
            or _canonical_json(payload) != self.signed_head_json
            or canonical_sha256(payload) != self.head_hash
        ):
            raise ValueError("signed resource journal head hash or shape is invalid")
        if (
            payload.get("authority_id") != self.journal_authority_id
            or payload.get("lineage_id") != self.lineage_id
            or payload.get("sequence") != self.sequence
            or payload.get("previous_head_hash") != self.previous_head_hash
            or payload.get("materialized_state_root") != self.materialized_state_root
        ):
            raise ValueError("signed resource journal head fields conflict with the checkpoint")
        return self

    @property
    def checkpoint_hash(self) -> str:
        return canonical_sha256(self.model_dump(mode="python"))


class ResourceJournalAntiRollbackReceipt(RuntimeContractModel):
    """Signed closed receipt issued by an independent anti-rollback authority."""

    schema_version: Literal[1]
    contract: Literal["rquant-resource-journal-anti-rollback-receipt/v1"]
    root_authority_id: str = Field(min_length=1, max_length=200)
    high_water_authority_id: str = Field(min_length=1, max_length=200)
    journal_authority_id: str = Field(min_length=1, max_length=200)
    operation_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    previous_checkpoint_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    checkpoint: ResourceJournalHighWaterCheckpoint
    closed: Literal[True] = True
    issuer: str = Field(min_length=1, max_length=200)
    key_id: str = Field(min_length=1, max_length=200)
    key_purpose: str = Field(min_length=1, max_length=200)
    namespace: str = Field(min_length=1, max_length=200)
    signature_algorithm: str = Field(min_length=1, max_length=100)
    public_key_fingerprint: str = Field(pattern=r"^[0-9a-f]{64}$")
    signature: str = Field(min_length=1)

    def signing_bytes(self) -> bytes:
        return _canonical_json_bytes(self.model_dump(mode="json", exclude={"signature"}))

    @property
    def receipt_hash(self) -> str:
        return canonical_sha256(self.model_dump(mode="python"))


class ResourceJournalSignatureVerifier(Protocol):
    key_id: str
    issuer: str
    key_purpose: str
    signature_algorithm: str
    public_key_fingerprint: str

    def verify(self, *, namespace: str, payload: bytes, signature: str) -> bool: ...


class AntiRollbackRootProtocol(Protocol):
    """Authenticated monotonic authority external to the local SQLite cache."""

    @property
    def authority_id(self) -> str: ...

    @property
    def verifier_fingerprints(self) -> frozenset[str]: ...

    def pin(
        self,
        *,
        operation_id: str,
        high_water_authority_id: str,
        journal_authority_id: str,
        checkpoint: ResourceJournalHighWaterCheckpoint,
    ) -> ResourceJournalAntiRollbackReceipt: ...

    def current(
        self,
        *,
        journal_authority_id: str,
    ) -> ResourceJournalAntiRollbackReceipt | None: ...

    def compare_and_advance(
        self,
        *,
        operation_id: str,
        high_water_authority_id: str,
        journal_authority_id: str,
        previous_checkpoint_hash: str,
        checkpoint: ResourceJournalHighWaterCheckpoint,
    ) -> ResourceJournalAntiRollbackReceipt: ...


class ResourceJournalHighWaterAuthority(Protocol):
    """Local cache/outbox boundary consumed by resource admission."""

    @property
    def authority_id(self) -> str: ...

    @property
    def storage_path(self) -> Path: ...

    @property
    def mode(self) -> Literal["production", "test-standalone"]: ...

    @property
    def anti_rollback_root_authority_id(self) -> str | None: ...

    @property
    def verifier_fingerprints(self) -> frozenset[str]: ...

    @property
    def journal_verifier_fingerprints(self) -> frozenset[str]: ...

    def pin(
        self,
        *,
        operation_id: str,
        journal_authority_id: str,
        checkpoint: ResourceJournalHighWaterCheckpoint,
    ) -> ResourceJournalAntiRollbackReceipt: ...

    def current(
        self,
        *,
        journal_authority_id: str,
    ) -> ResourceJournalAntiRollbackReceipt | None: ...

    def compare_and_advance(
        self,
        *,
        operation_id: str,
        journal_authority_id: str,
        previous_checkpoint_hash: str,
        checkpoint: ResourceJournalHighWaterCheckpoint,
    ) -> ResourceJournalAntiRollbackReceipt: ...


class _HighWaterRequest(RuntimeContractModel):
    schema_version: Literal[1]
    contract: Literal["rquant-resource-journal-high-water-request/v1"]
    kind: Literal["pin", "advance"]
    operation_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    high_water_authority_id: str = Field(min_length=1, max_length=200)
    journal_authority_id: str = Field(min_length=1, max_length=200)
    previous_checkpoint_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    checkpoint: ResourceJournalHighWaterCheckpoint

    @property
    def request_hash(self) -> str:
        return canonical_sha256(self.model_dump(mode="python"))


@dataclass(frozen=True)
class _VerifierRecord:
    verifier: ResourceJournalSignatureVerifier
    key_id: str
    fingerprint: str


@dataclass(frozen=True)
class _CacheAudit:
    states: dict[str, ResourceJournalAntiRollbackReceipt]
    pending: dict[str, _HighWaterRequest]


def _canonical_json(value: object) -> str:
    return json.dumps(
        value,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def _canonical_json_bytes(value: object) -> bytes:
    return _canonical_json(value).encode("utf-8")


def _model_json(value: RuntimeContractModel) -> str:
    return _canonical_json(value.model_dump(mode="json"))


def _normalized_sql(value: str) -> str:
    return " ".join(value.split())


def _is_sha256(value: object) -> bool:
    return isinstance(value, str) and re.fullmatch(r"[0-9a-f]{64}", value) is not None


class SQLiteResourceJournalHighWaterAuthority:
    """Durable cache/outbox; production truth remains the injected external root."""

    def __init__(
        self,
        path: Path,
        *,
        authority_id: str,
        trusted_role_inventory: TrustedRoleInventory,
        journal_verifiers: tuple[ResourceJournalSignatureVerifier, ...],
        trusted_journal_issuer: str,
        anti_rollback_root: AntiRollbackRootProtocol | None,
        root_verifiers: tuple[ResourceJournalSignatureVerifier, ...],
        trusted_root_issuer: str | None,
        mode: Literal["production", "test-standalone"] = "production",
        busy_timeout_ms: int = 5_000,
    ) -> None:
        normalized_authority_id = authority_id.strip()
        if not normalized_authority_id:
            raise ResourceJournalHighWaterError("high-water cache authority_id is required")
        if mode not in {"production", "test-standalone"}:
            raise ResourceJournalHighWaterError("high-water cache mode is invalid")
        if type(busy_timeout_ms) is not int or busy_timeout_ms < 1:
            raise ResourceJournalHighWaterError("high-water cache busy timeout is invalid")
        self._authority_id = normalized_authority_id
        self._mode = mode
        if not isinstance(trusted_role_inventory, TrustedRoleInventory):
            raise ResourceJournalHighWaterError(
                "high-water cache requires the closed TrustedRoleInventory"
            )
        self._inventory_hash = trusted_role_inventory.policy_hash
        self._journal_issuer = trusted_journal_issuer.strip()
        self._journal_records = self._build_verifier_records(
            verifiers=journal_verifiers,
            issuer=self._journal_issuer,
            purpose=RESOURCE_JOURNAL_SIGNING_PURPOSE,
            inventory=trusted_role_inventory,
            label="resource journal",
        )
        self._journal_fingerprints = frozenset(
            record.fingerprint for record in self._journal_records.values()
        )
        self._anti_rollback_root = anti_rollback_root
        self._root_records: dict[str, _VerifierRecord] = {}
        self._root_fingerprints: frozenset[str] = frozenset()
        self._root_issuer = "" if trusted_root_issuer is None else trusted_root_issuer.strip()
        if mode == "production":
            if anti_rollback_root is None:
                raise ResourceJournalHighWaterError(
                    "production high-water cache requires an authenticated external root"
                )
            root_authority_id = anti_rollback_root.authority_id.strip()
            if not root_authority_id or root_authority_id == self._authority_id:
                raise ResourceJournalHighWaterError(
                    "external anti-rollback root authority must be independent"
                )
            self._root_records = self._build_verifier_records(
                verifiers=root_verifiers,
                issuer=self._root_issuer,
                purpose=RESOURCE_JOURNAL_HIGH_WATER_PURPOSE,
                inventory=trusted_role_inventory,
                label="external root",
            )
            self._root_fingerprints = frozenset(
                record.fingerprint for record in self._root_records.values()
            )
            if anti_rollback_root.verifier_fingerprints != self._root_fingerprints:
                raise ResourceJournalHighWaterError(
                    "external root verifier set conflicts with the trusted inventory"
                )
        elif anti_rollback_root is not None or root_verifiers or trusted_root_issuer is not None:
            raise ResourceJournalHighWaterError(
                "test-standalone cache cannot claim an external anti-rollback root"
            )
        self._path = Path(path).expanduser().resolve()
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._busy_timeout_ms = busy_timeout_ms
        self._initialize()

    @staticmethod
    def _build_verifier_records(
        *,
        verifiers: tuple[ResourceJournalSignatureVerifier, ...],
        issuer: str,
        purpose: str,
        inventory: TrustedRoleInventory,
        label: str,
    ) -> dict[str, _VerifierRecord]:
        if not issuer or not verifiers:
            raise ResourceJournalHighWaterError(
                f"{label} verifier inventory and issuer are required"
            )
        records: dict[str, _VerifierRecord] = {}
        fingerprints: set[str] = set()
        for verifier in verifiers:
            key_id = verifier.key_id.strip()
            fingerprint = verifier.public_key_fingerprint.strip().lower()
            if (
                not key_id
                or key_id in records
                or fingerprint in fingerprints
                or verifier.issuer.strip() != issuer
                or verifier.key_purpose.strip() != purpose
                or verifier.signature_algorithm.strip() != RESOURCE_JOURNAL_SIGNATURE_ALGORITHM
                or not _is_sha256(fingerprint)
                or not callable(verifier.verify)
            ):
                raise ResourceJournalHighWaterError(f"{label} verifier identity is invalid")
            records[key_id] = _VerifierRecord(
                verifier=verifier,
                key_id=key_id,
                fingerprint=fingerprint,
            )
            fingerprints.add(fingerprint)
        try:
            expected = inventory.fingerprints_for_purpose(purpose)
        except Exception as exc:
            raise ResourceJournalHighWaterError(
                f"{label} verifier purpose is absent from the trusted inventory"
            ) from exc
        if expected != frozenset(fingerprints):
            raise ResourceJournalHighWaterError(
                f"{label} verifier set conflicts with the trusted inventory"
            )
        return records

    @property
    def authority_id(self) -> str:
        return self._authority_id

    @property
    def storage_path(self) -> Path:
        return self._path

    @property
    def mode(self) -> Literal["production", "test-standalone"]:
        return self._mode

    @property
    def anti_rollback_root_authority_id(self) -> str | None:
        root = self._anti_rollback_root
        return None if root is None else root.authority_id

    @property
    def verifier_fingerprints(self) -> frozenset[str]:
        return self._root_fingerprints

    @property
    def journal_verifier_fingerprints(self) -> frozenset[str]:
        return self._journal_fingerprints

    def pin(
        self,
        *,
        operation_id: str,
        journal_authority_id: str,
        checkpoint: ResourceJournalHighWaterCheckpoint,
    ) -> ResourceJournalAntiRollbackReceipt:
        return self._write(
            kind="pin",
            operation_id=operation_id,
            journal_authority_id=journal_authority_id,
            previous_checkpoint_hash=RESOURCE_JOURNAL_HIGH_WATER_ZERO_HASH,
            checkpoint=checkpoint,
        )

    def current(
        self,
        *,
        journal_authority_id: str,
    ) -> ResourceJournalAntiRollbackReceipt | None:
        identifier = journal_authority_id.strip()
        if not identifier:
            raise ResourceJournalHighWaterError("journal authority identity is required")
        self._require_production_root()
        self._synchronize(identifier)
        try:
            with self._connect() as connection:
                connection.execute("BEGIN IMMEDIATE")
                audit = self._audit(connection)
                result = audit.states.get(identifier)
                connection.commit()
                return result
        except ResourceJournalHighWaterError:
            raise
        except (OSError, sqlite3.Error) as exc:
            raise ResourceJournalHighWaterError("high-water cache current read failed") from exc

    def compare_and_advance(
        self,
        *,
        operation_id: str,
        journal_authority_id: str,
        previous_checkpoint_hash: str,
        checkpoint: ResourceJournalHighWaterCheckpoint,
    ) -> ResourceJournalAntiRollbackReceipt:
        return self._write(
            kind="advance",
            operation_id=operation_id,
            journal_authority_id=journal_authority_id,
            previous_checkpoint_hash=previous_checkpoint_hash,
            checkpoint=checkpoint,
        )

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(
            self._path,
            timeout=self._busy_timeout_ms / 1_000,
            isolation_level=None,
        )
        connection.row_factory = sqlite3.Row
        connection.execute(f"PRAGMA busy_timeout = {self._busy_timeout_ms}")
        connection.execute("PRAGMA synchronous = FULL")
        connection.execute("PRAGMA trusted_schema = OFF")
        return connection

    def _initialize(self) -> None:
        identifiers: set[str]
        try:
            with self._connect() as connection:
                connection.execute("BEGIN IMMEDIATE")
                objects = connection.execute(
                    "SELECT 1 FROM sqlite_schema WHERE name NOT LIKE 'sqlite_%' LIMIT 1"
                ).fetchone()
                if objects is None:
                    for sql in _TABLE_SQL.values():
                        connection.execute(sql)
                    connection.execute(f"PRAGMA application_id = {_APPLICATION_ID}")
                    connection.execute(f"PRAGMA user_version = {_SCHEMA_VERSION}")
                    connection.execute(
                        f"""
                        INSERT INTO {_META_TABLE}(
                            singleton, schema_version, authority_id, mode,
                            trusted_role_inventory_hash,
                            journal_issuer, journal_verifier_fingerprints_json,
                            root_authority_id, root_issuer,
                            root_verifier_fingerprints_json,
                            operation_count, journal_root
                        ) VALUES (1, 2, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?)
                        """,
                        (
                            self._authority_id,
                            self._mode,
                            self._inventory_hash,
                            self._journal_issuer,
                            _canonical_json(sorted(self._journal_fingerprints)),
                            self.anti_rollback_root_authority_id,
                            None if self._mode == "test-standalone" else self._root_issuer,
                            _canonical_json(sorted(self._root_fingerprints)),
                            RESOURCE_JOURNAL_HIGH_WATER_ZERO_HASH,
                        ),
                    )
                audit = self._audit(connection)
                identifiers = set(audit.states) | set(audit.pending)
                connection.commit()
        except ResourceJournalHighWaterError:
            raise
        except (OSError, sqlite3.Error) as exc:
            raise ResourceJournalHighWaterError("high-water cache initialization failed") from exc
        if self._mode == "production":
            for identifier in sorted(identifiers):
                self._synchronize(identifier)

    def _require_production_root(self) -> AntiRollbackRootProtocol:
        root = self._anti_rollback_root
        if self._mode != "production" or root is None:
            raise ResourceJournalHighWaterError(
                "test-standalone high-water cache has no external anti-rollback root"
            )
        return root

    def _validate_schema(self, connection: sqlite3.Connection) -> None:
        if connection.execute("PRAGMA application_id").fetchone()[0] != _APPLICATION_ID:
            raise ResourceJournalHighWaterError("high-water cache schema identity is invalid")
        if connection.execute("PRAGMA user_version").fetchone()[0] != _SCHEMA_VERSION:
            raise ResourceJournalHighWaterError("high-water cache schema version is invalid")
        objects = {
            (str(row["type"]), str(row["name"]))
            for row in connection.execute(
                "SELECT type, name FROM sqlite_schema WHERE name NOT LIKE 'sqlite_%'"
            )
        }
        if objects != {("table", name) for name in _TABLE_SQL}:
            raise ResourceJournalHighWaterError("high-water cache schema was tampered")
        stored_sql = {
            str(row["name"]): str(row["sql"])
            for row in connection.execute(
                "SELECT name, sql FROM sqlite_schema "
                "WHERE type = 'table' AND name NOT LIKE 'sqlite_%'"
            )
        }
        for name, expected in _TABLE_SQL.items():
            if _normalized_sql(stored_sql.get(name, "")) != _normalized_sql(expected):
                raise ResourceJournalHighWaterError(
                    f"high-water cache schema for {name} was tampered"
                )

    def _audit(self, connection: sqlite3.Connection) -> _CacheAudit:
        self._validate_schema(connection)
        meta_rows = connection.execute(f"SELECT * FROM {_META_TABLE}").fetchall()
        if len(meta_rows) != 1:
            raise ResourceJournalHighWaterError("high-water cache metadata is missing")
        meta = meta_rows[0]
        expected_root_id = self.anti_rollback_root_authority_id
        expected_root_issuer = None if self._mode == "test-standalone" else self._root_issuer
        if (
            meta["singleton"] != 1
            or meta["schema_version"] != _SCHEMA_VERSION
            or meta["authority_id"] != self._authority_id
            or meta["mode"] != self._mode
            or meta["trusted_role_inventory_hash"] != self._inventory_hash
            or meta["journal_issuer"] != self._journal_issuer
            or meta["journal_verifier_fingerprints_json"]
            != _canonical_json(sorted(self._journal_fingerprints))
            or meta["root_authority_id"] != expected_root_id
            or meta["root_issuer"] != expected_root_issuer
            or meta["root_verifier_fingerprints_json"]
            != _canonical_json(sorted(self._root_fingerprints))
        ):
            raise ResourceJournalHighWaterError("high-water cache metadata identity was tampered")

        states: dict[str, ResourceJournalAntiRollbackReceipt] = {}
        previous_operation_hash = RESOURCE_JOURNAL_HIGH_WATER_ZERO_HASH
        operations = connection.execute(
            f"SELECT * FROM {_OPERATION_TABLE} ORDER BY operation_index"
        ).fetchall()
        for expected_index, row in enumerate(operations, start=1):
            try:
                request = _HighWaterRequest.model_validate_json(row["request_json"])
                receipt = ResourceJournalAntiRollbackReceipt.model_validate_json(
                    row["receipt_json"]
                )
            except ValidationError as exc:
                raise ResourceJournalHighWaterError(
                    "high-water cache operation row is malformed"
                ) from exc
            self._validate_candidate(request.checkpoint)
            self._validate_root_receipt(receipt, request=request)
            current = states.get(request.journal_authority_id)
            self._validate_transition(request=request, current=current)
            expected_hash = canonical_sha256(
                {
                    "contract": "rquant-resource-journal-high-water-cache-operation/v1",
                    "operation_index": expected_index,
                    "previous_operation_hash": previous_operation_hash,
                    "receipt": receipt,
                    "request": request,
                }
            )
            if (
                row["operation_index"] != expected_index
                or row["operation_id"] != request.operation_id
                or row["request_hash"] != request.request_hash
                or row["request_json"] != _model_json(request)
                or row["receipt_json"] != _model_json(receipt)
                or row["previous_operation_hash"] != previous_operation_hash
                or row["operation_hash"] != expected_hash
            ):
                raise ResourceJournalHighWaterError(
                    "high-water cache operation chain integrity failed"
                )
            states[request.journal_authority_id] = receipt
            previous_operation_hash = expected_hash

        actual_states: dict[str, ResourceJournalAntiRollbackReceipt] = {}
        for row in connection.execute(
            f"SELECT * FROM {_STATE_TABLE} ORDER BY journal_authority_id"
        ).fetchall():
            try:
                receipt = ResourceJournalAntiRollbackReceipt.model_validate_json(
                    row["receipt_json"]
                )
            except ValidationError as exc:
                raise ResourceJournalHighWaterError(
                    "high-water cached root receipt is malformed"
                ) from exc
            self._validate_root_receipt(receipt)
            if row["journal_authority_id"] != receipt.journal_authority_id or row[
                "receipt_json"
            ] != _model_json(receipt):
                raise ResourceJournalHighWaterError(
                    "high-water cached root receipt integrity failed"
                )
            actual_states[receipt.journal_authority_id] = receipt
        if actual_states != states:
            raise ResourceJournalHighWaterError(
                "high-water materialized cache diverged from its operation chain"
            )

        pending: dict[str, _HighWaterRequest] = {}
        for row in connection.execute(
            f"SELECT * FROM {_PENDING_TABLE} ORDER BY journal_authority_id"
        ).fetchall():
            try:
                request = _HighWaterRequest.model_validate_json(row["request_json"])
            except ValidationError as exc:
                raise ResourceJournalHighWaterError(
                    "high-water pending request is malformed"
                ) from exc
            self._validate_candidate(request.checkpoint)
            self._validate_transition(
                request=request,
                current=states.get(request.journal_authority_id),
            )
            if (
                row["operation_id"] != request.operation_id
                or row["journal_authority_id"] != request.journal_authority_id
                or row["request_json"] != _model_json(request)
                or row["request_hash"] != request.request_hash
            ):
                raise ResourceJournalHighWaterError("high-water pending request integrity failed")
            pending[request.journal_authority_id] = request

        if (
            meta["operation_count"] != len(operations)
            or meta["journal_root"] != previous_operation_hash
        ):
            raise ResourceJournalHighWaterError("high-water cache operation chain was rolled back")
        return _CacheAudit(states=states, pending=pending)

    def _validate_candidate(self, checkpoint: ResourceJournalHighWaterCheckpoint) -> None:
        try:
            head = json.loads(checkpoint.signed_head_json)
        except (TypeError, ValueError) as exc:
            raise ResourceJournalHighWaterError(
                "resource journal candidate head is malformed"
            ) from exc
        if not isinstance(head, dict):
            raise ResourceJournalHighWaterError("resource journal candidate head is malformed")
        key_id = head.get("key_id")
        fingerprint = head.get("public_key_fingerprint")
        record = None if not isinstance(key_id, str) else self._journal_records.get(key_id)
        if (
            record is None
            or head.get("authority_id") != checkpoint.journal_authority_id
            or head.get("lineage_id") != checkpoint.lineage_id
            or head.get("sequence") != checkpoint.sequence
            or head.get("previous_head_hash") != checkpoint.previous_head_hash
            or head.get("materialized_state_root") != checkpoint.materialized_state_root
            or head.get("issuer") != self._journal_issuer
            or head.get("key_purpose") != RESOURCE_JOURNAL_SIGNING_PURPOSE
            or head.get("namespace") != RESOURCE_JOURNAL_HEAD_NAMESPACE
            or head.get("signature_algorithm") != RESOURCE_JOURNAL_SIGNATURE_ALGORITHM
            or fingerprint != record.fingerprint
            or record.verifier.key_id.strip() != record.key_id
            or record.verifier.issuer.strip() != self._journal_issuer
            or record.verifier.key_purpose.strip() != RESOURCE_JOURNAL_SIGNING_PURPOSE
            or record.verifier.signature_algorithm.strip() != RESOURCE_JOURNAL_SIGNATURE_ALGORITHM
            or record.verifier.public_key_fingerprint.strip().lower() != record.fingerprint
        ):
            raise ResourceJournalHighWaterError(
                "resource journal candidate signer identity or purpose is invalid"
            )
        signing_value = dict(head)
        signature = signing_value.pop("signature", None)
        signing_value["contract"] = "rquant-resource-admission-journal-head/v1"
        try:
            verified = isinstance(signature, str) and record.verifier.verify(
                namespace=RESOURCE_JOURNAL_HEAD_NAMESPACE,
                payload=_canonical_json_bytes(signing_value),
                signature=signature,
            )
        except Exception:
            verified = False
        if not verified:
            raise ResourceJournalHighWaterError("resource journal candidate signature is invalid")

    def _validate_root_receipt(
        self,
        receipt: ResourceJournalAntiRollbackReceipt,
        *,
        request: _HighWaterRequest | None = None,
    ) -> None:
        self._validate_candidate(receipt.checkpoint)
        record = self._root_records.get(receipt.key_id)
        root = self._require_production_root()
        if (
            record is None
            or receipt.root_authority_id != root.authority_id
            or receipt.high_water_authority_id != self._authority_id
            or receipt.checkpoint.journal_authority_id != receipt.journal_authority_id
            or receipt.issuer != self._root_issuer
            or receipt.key_purpose != RESOURCE_JOURNAL_HIGH_WATER_PURPOSE
            or receipt.namespace != RESOURCE_JOURNAL_ANTI_ROLLBACK_RECEIPT_NAMESPACE
            or receipt.signature_algorithm != RESOURCE_JOURNAL_SIGNATURE_ALGORITHM
            or receipt.public_key_fingerprint != record.fingerprint
            or record.verifier.key_id.strip() != record.key_id
            or record.verifier.issuer.strip() != self._root_issuer
            or record.verifier.key_purpose.strip() != RESOURCE_JOURNAL_HIGH_WATER_PURPOSE
            or record.verifier.signature_algorithm.strip() != RESOURCE_JOURNAL_SIGNATURE_ALGORITHM
            or record.verifier.public_key_fingerprint.strip().lower() != record.fingerprint
        ):
            raise ResourceJournalHighWaterError(
                "external root receipt signer identity or purpose is invalid"
            )
        if request is not None and (
            receipt.operation_id != request.operation_id
            or receipt.journal_authority_id != request.journal_authority_id
            or receipt.previous_checkpoint_hash != request.previous_checkpoint_hash
            or receipt.checkpoint != request.checkpoint
        ):
            raise ResourceJournalHighWaterError(
                "external root receipt conflicts with the pending request"
            )
        try:
            verified = record.verifier.verify(
                namespace=RESOURCE_JOURNAL_ANTI_ROLLBACK_RECEIPT_NAMESPACE,
                payload=receipt.signing_bytes(),
                signature=receipt.signature,
            )
        except Exception:
            verified = False
        if not verified:
            raise ResourceJournalHighWaterError("external root receipt signature is invalid")

    @staticmethod
    def _validate_transition(
        *,
        request: _HighWaterRequest,
        current: ResourceJournalAntiRollbackReceipt | None,
    ) -> None:
        checkpoint = request.checkpoint
        if request.kind == "pin":
            if (
                current is not None
                or request.previous_checkpoint_hash != RESOURCE_JOURNAL_HIGH_WATER_ZERO_HASH
                or checkpoint.sequence != 0
                or checkpoint.previous_head_hash != RESOURCE_JOURNAL_HIGH_WATER_ZERO_HASH
            ):
                raise ResourceJournalHighWaterError(
                    "high-water genesis pin conflicts with cached state"
                )
            return
        if current is None:
            raise ResourceJournalHighWaterError("high-water lineage is not pinned in the cache")
        if (
            checkpoint.lineage_id != current.checkpoint.lineage_id
            or request.previous_checkpoint_hash != current.checkpoint.checkpoint_hash
            or checkpoint.sequence != current.checkpoint.sequence + 1
            or checkpoint.previous_head_hash != current.checkpoint.head_hash
        ):
            raise ResourceJournalHighWaterError(
                "high-water lineage, sequence, or previous head conflicts"
            )

    def _write(
        self,
        *,
        kind: Literal["pin", "advance"],
        operation_id: str,
        journal_authority_id: str,
        previous_checkpoint_hash: str,
        checkpoint: ResourceJournalHighWaterCheckpoint,
    ) -> ResourceJournalAntiRollbackReceipt:
        self._require_production_root()
        try:
            request = _HighWaterRequest(
                schema_version=1,
                contract="rquant-resource-journal-high-water-request/v1",
                kind=kind,
                operation_id=operation_id,
                high_water_authority_id=self._authority_id,
                journal_authority_id=journal_authority_id.strip(),
                previous_checkpoint_hash=previous_checkpoint_hash,
                checkpoint=checkpoint,
            )
        except ValidationError as exc:
            raise ResourceJournalHighWaterError("high-water request is malformed") from exc
        self._validate_candidate(request.checkpoint)
        self._synchronize(request.journal_authority_id)
        try:
            with self._connect() as connection:
                connection.execute("BEGIN IMMEDIATE")
                audit = self._audit(connection)
                existing = connection.execute(
                    f"SELECT request_hash, receipt_json FROM {_OPERATION_TABLE} "
                    "WHERE operation_id = ?",
                    (request.operation_id,),
                ).fetchone()
                if existing is not None:
                    if existing["request_hash"] != request.request_hash:
                        raise ResourceJournalHighWaterError(
                            "high-water operation_id payload conflicts"
                        )
                    receipt = ResourceJournalAntiRollbackReceipt.model_validate_json(
                        existing["receipt_json"]
                    )
                    connection.commit()
                    return receipt
                pending = audit.pending.get(request.journal_authority_id)
                if pending is not None and pending != request:
                    raise ResourceJournalHighWaterError(
                        "high-water journal already has another pending operation"
                    )
                self._validate_transition(
                    request=request,
                    current=audit.states.get(request.journal_authority_id),
                )
                if pending is None:
                    connection.execute(
                        f"""
                        INSERT INTO {_PENDING_TABLE}(
                            operation_id, journal_authority_id, request_json, request_hash
                        ) VALUES (?, ?, ?, ?)
                        """,
                        (
                            request.operation_id,
                            request.journal_authority_id,
                            _model_json(request),
                            request.request_hash,
                        ),
                    )
                self._audit(connection)
                connection.commit()
        except ResourceJournalHighWaterError:
            raise
        except (OSError, sqlite3.Error, ValidationError) as exc:
            raise ResourceJournalHighWaterError("high-water cache prepare failed") from exc

        self._synchronize(request.journal_authority_id)
        try:
            with self._connect() as connection:
                connection.execute("BEGIN IMMEDIATE")
                self._audit(connection)
                row = connection.execute(
                    f"SELECT request_hash, receipt_json FROM {_OPERATION_TABLE} "
                    "WHERE operation_id = ?",
                    (request.operation_id,),
                ).fetchone()
                if row is None or row["request_hash"] != request.request_hash:
                    raise ResourceJournalHighWaterError(
                        "rooted high-water operation is missing after recovery"
                    )
                receipt = ResourceJournalAntiRollbackReceipt.model_validate_json(
                    row["receipt_json"]
                )
                connection.commit()
                return receipt
        except ResourceJournalHighWaterError:
            raise
        except (OSError, sqlite3.Error, ValidationError) as exc:
            raise ResourceJournalHighWaterError("high-water cache recovery read failed") from exc

    def _synchronize(self, journal_authority_id: str) -> None:
        root = self._require_production_root()
        try:
            with self._connect() as connection:
                connection.execute("BEGIN IMMEDIATE")
                audit = self._audit(connection)
                local = audit.states.get(journal_authority_id)
                pending = audit.pending.get(journal_authority_id)
                connection.commit()
        except ResourceJournalHighWaterError:
            raise
        except (OSError, sqlite3.Error) as exc:
            raise ResourceJournalHighWaterError(
                "high-water cache synchronization read failed"
            ) from exc

        try:
            external = root.current(journal_authority_id=journal_authority_id)
        except ResourceJournalHighWaterError:
            raise
        if external is not None:
            self._validate_root_receipt(external)
        if pending is None:
            if external != local:
                raise ResourceJournalHighWaterError(
                    "external anti-rollback root rejected local cache rollback or donor state"
                )
            return

        if external is not None and self._receipt_matches_request(external, pending):
            rooted = external
        else:
            if external != local:
                raise ResourceJournalHighWaterError(
                    "external anti-rollback root conflicts with the pending cache operation"
                )
            if pending.kind == "pin":
                rooted = root.pin(
                    operation_id=pending.operation_id,
                    high_water_authority_id=self._authority_id,
                    journal_authority_id=pending.journal_authority_id,
                    checkpoint=pending.checkpoint,
                )
            else:
                rooted = root.compare_and_advance(
                    operation_id=pending.operation_id,
                    high_water_authority_id=self._authority_id,
                    journal_authority_id=pending.journal_authority_id,
                    previous_checkpoint_hash=pending.previous_checkpoint_hash,
                    checkpoint=pending.checkpoint,
                )
            self._validate_root_receipt(rooted, request=pending)
        self._finalize_pending(pending, rooted)

    @staticmethod
    def _receipt_matches_request(
        receipt: ResourceJournalAntiRollbackReceipt,
        request: _HighWaterRequest,
    ) -> bool:
        return (
            receipt.operation_id == request.operation_id
            and receipt.high_water_authority_id == request.high_water_authority_id
            and receipt.journal_authority_id == request.journal_authority_id
            and receipt.previous_checkpoint_hash == request.previous_checkpoint_hash
            and receipt.checkpoint == request.checkpoint
        )

    def _finalize_pending(
        self,
        request: _HighWaterRequest,
        receipt: ResourceJournalAntiRollbackReceipt,
    ) -> None:
        self._validate_root_receipt(receipt, request=request)
        try:
            with self._connect() as connection:
                connection.execute("BEGIN IMMEDIATE")
                audit = self._audit(connection)
                if audit.pending.get(request.journal_authority_id) != request:
                    raise ResourceJournalHighWaterError(
                        "high-water pending cache operation changed before checkpoint"
                    )
                meta = connection.execute(
                    f"SELECT operation_count, journal_root FROM {_META_TABLE} WHERE singleton = 1"
                ).fetchone()
                if meta is None:
                    raise ResourceJournalHighWaterError("high-water cache metadata is missing")
                operation_index = int(meta["operation_count"]) + 1
                previous_operation_hash = str(meta["journal_root"])
                operation_hash = canonical_sha256(
                    {
                        "contract": "rquant-resource-journal-high-water-cache-operation/v1",
                        "operation_index": operation_index,
                        "previous_operation_hash": previous_operation_hash,
                        "receipt": receipt,
                        "request": request,
                    }
                )
                connection.execute(
                    f"""
                    INSERT INTO {_OPERATION_TABLE}(
                        operation_id, operation_index, request_json, request_hash,
                        receipt_json, previous_operation_hash, operation_hash
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        request.operation_id,
                        operation_index,
                        _model_json(request),
                        request.request_hash,
                        _model_json(receipt),
                        previous_operation_hash,
                        operation_hash,
                    ),
                )
                connection.execute(
                    f"""
                    INSERT INTO {_STATE_TABLE}(journal_authority_id, receipt_json)
                    VALUES (?, ?)
                    ON CONFLICT(journal_authority_id)
                    DO UPDATE SET receipt_json = excluded.receipt_json
                    """,
                    (request.journal_authority_id, _model_json(receipt)),
                )
                deleted = connection.execute(
                    f"DELETE FROM {_PENDING_TABLE} WHERE operation_id = ? AND request_hash = ?",
                    (request.operation_id, request.request_hash),
                )
                updated = connection.execute(
                    f"""
                    UPDATE {_META_TABLE}
                    SET operation_count = ?, journal_root = ?
                    WHERE singleton = 1 AND operation_count = ? AND journal_root = ?
                    """,
                    (
                        operation_index,
                        operation_hash,
                        operation_index - 1,
                        previous_operation_hash,
                    ),
                )
                if deleted.rowcount != 1 or updated.rowcount != 1:
                    raise ResourceJournalHighWaterError(
                        "high-water local cache checkpoint compare-and-swap failed"
                    )
                self._audit(connection)
                connection.commit()
        except ResourceJournalHighWaterError:
            raise
        except (OSError, sqlite3.Error) as exc:
            raise ResourceJournalHighWaterError("high-water local cache checkpoint failed") from exc


__all__ = [
    "AntiRollbackRootProtocol",
    "RESOURCE_JOURNAL_ANTI_ROLLBACK_RECEIPT_NAMESPACE",
    "RESOURCE_JOURNAL_HEAD_NAMESPACE",
    "RESOURCE_JOURNAL_HIGH_WATER_PURPOSE",
    "RESOURCE_JOURNAL_HIGH_WATER_ZERO_HASH",
    "RESOURCE_JOURNAL_SIGNATURE_ALGORITHM",
    "RESOURCE_JOURNAL_SIGNING_PURPOSE",
    "TRUSTED_RESOURCE_ROLE_PURPOSES",
    "ResourceJournalAntiRollbackReceipt",
    "ResourceJournalHighWaterAuthority",
    "ResourceJournalHighWaterCheckpoint",
    "ResourceJournalHighWaterError",
    "ResourceJournalSignatureVerifier",
    "SQLiteResourceJournalHighWaterAuthority",
    "TrustedRoleInventory",
]
