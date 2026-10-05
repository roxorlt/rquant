"""Single-writer price decisions, cooldown and immutable events in one transaction."""

from __future__ import annotations

import fcntl
import os
import sqlite3
import stat
from collections import Counter
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from datetime import datetime, timedelta
from hashlib import sha256
from pathlib import Path
from typing import Literal, Self

from pydantic import Field, StrictInt, StrictStr, field_validator, model_validator

from rquant.manual_watchlist import OwnerId, TsCode
from rquant.price_alert_runtime_contracts import (
    PriceAlertCapacityExceeded,
    PriceAlertEventEnvelope,
    PriceAlertFrequencyPolicy,
    PriceAlertRuntimeActivation,
    PriceAlertSourceDescriptor,
    PriceRuntimeModel,
    PriceSha256,
    parse_price_alert_event,
    require_price_alert_activation,
    utc_text,
)
from rquant.runtime_contracts import AwareUtcDatetime
from rquant.strict_json import canonical_json_bytes


class PriceEvaluationRecord(PriceRuntimeModel):
    owner_id: OwnerId
    rule_id: StrictStr = Field(min_length=1, max_length=128)
    rule_version: StrictInt = Field(ge=1)
    membership_version: StrictInt = Field(ge=1)
    ts_code: TsCode
    state: Literal["triggered", "not_triggered", "unavailable"]
    reason: StrictStr = Field(min_length=1, max_length=80)
    event: PriceAlertEventEnvelope | None = None

    @field_validator("event", mode="before")
    @classmethod
    def exact_event(cls, value: object) -> object:
        if value is None:
            return None
        if isinstance(value, dict):
            value = canonical_json_bytes(value)
        return parse_price_alert_event(value)

    @model_validator(mode="after")
    def decision_binding(self) -> Self:
        if self.state == "triggered" and self.event is None:
            raise ValueError("triggered record needs its verified candidate facts")
        if self.event is not None and (
            self.state != "triggered"
            or (
                self.owner_id,
                self.rule_id,
                self.rule_version,
                self.membership_version,
                self.ts_code,
            )
            != (
                self.event.owner_id,
                self.event.rule_id,
                self.event.rule_version,
                self.event.membership_version,
                self.event.ts_code,
            )
        ):
            raise ValueError("candidate event differs from the actual rule decision")
        return self


class PriceRoundInput(PriceRuntimeModel):
    evaluated_at: AwareUtcDatetime
    scope_generation_id: PriceSha256
    scope_manifest_sha256: PriceSha256
    scope_source_generation_id: PriceSha256
    scope_source_sequence: StrictInt = Field(ge=0)
    quote_source_generation_id: PriceSha256 | None = None
    quote_batch_id: PriceSha256 | None = None
    requested_codes: StrictInt = Field(ge=0, le=500)
    valid_quotes: StrictInt = Field(ge=0, le=500)
    rule_rows_sha256: PriceSha256 | None = None
    member_rows_sha256: PriceSha256 | None = None
    scope_built_at: AwareUtcDatetime | None = None
    quote_available_at: AwareUtcDatetime | None = None
    records: tuple[PriceEvaluationRecord, ...] = Field(max_length=3200)

    @model_validator(mode="after")
    def validate_domain(self) -> Self:
        keys = tuple((row.owner_id, row.rule_id) for row in self.records)
        if keys != tuple(sorted(set(keys))):
            raise ValueError("price round must contain sorted unique owner/rule decisions")
        counts = Counter(row.owner_id for row in self.records)
        if len(counts) > 32 or any(count > 100 for count in counts.values()):
            raise ValueError("price round exceeds owner or rule capacity")
        if sum(row.event is not None for row in self.records) > 1000:
            raise ValueError("price round exceeds effective rule capacity")
        if self.valid_quotes > self.requested_codes:
            raise ValueError("price round valid quotes differ from the requested domain")
        if len(self.wire_bytes()) > 1024 * 1024:
            raise PriceAlertCapacityExceeded("price round exceeds the complete domain budget")
        if (self.quote_source_generation_id is None) != (self.quote_batch_id is None):
            raise ValueError("price quote batch and generation must be paired")
        for row in self.records:
            if row.event is not None and (
                row.event.evaluated_at != self.evaluated_at
                or row.event.scope_generation_id != self.scope_generation_id
                or row.event.scope_manifest_sha256 != self.scope_manifest_sha256
                or row.event.quote_source_generation_id != self.quote_source_generation_id
                or row.event.quote_batch_id != self.quote_batch_id
            ):
                raise ValueError("price round contains an event from another input generation")
        return self


class PriceAlertProducerEventRecord(PriceRuntimeModel):
    sequence: StrictInt = Field(ge=1)
    event: PriceAlertEventEnvelope
    payload_json: StrictStr = Field(min_length=1, max_length=4096)
    payload_sha256: PriceSha256

    @field_validator("event", mode="before")
    @classmethod
    def exact_event(cls, value: object) -> PriceAlertEventEnvelope:
        return parse_price_alert_event(
            canonical_json_bytes(value) if isinstance(value, dict) else value
        )

    @model_validator(mode="after")
    def sealed_event(self) -> Self:
        if (
            parse_price_alert_event(self.payload_json) != self.event
            or sha256(self.payload_json.encode()).hexdigest() != self.payload_sha256
        ):
            raise ValueError("price producer record differs from its sealed bytes")
        return self


class PriceRoundMetadata(PriceRuntimeModel):
    availability: Literal["ready", "unavailable"]
    reason: StrictStr | None = Field(default=None, max_length=80)
    scope_generation_id: PriceSha256 | None = None
    scope_manifest_sha256: PriceSha256 | None = None
    scope_source_generation_id: PriceSha256 | None = None
    scope_source_sequence: StrictInt | None = Field(default=None, ge=0)
    rule_rows_sha256: PriceSha256 | None = None
    member_rows_sha256: PriceSha256 | None = None
    scope_built_at: AwareUtcDatetime | None = None
    quote_source_generation_id: PriceSha256 | None = None
    quote_batch_id: PriceSha256 | None = None
    quote_available_at: AwareUtcDatetime | None = None
    requested_codes: StrictInt | None = Field(default=None, ge=0, le=500)
    valid_quotes: StrictInt | None = Field(default=None, ge=0, le=500)


class PriceRoundReceipt(PriceRuntimeModel):
    round_id: PriceSha256
    input_sha256: PriceSha256
    evaluated_at: AwareUtcDatetime
    source_high_watermark: StrictInt = Field(ge=0)
    decision_count: StrictInt = Field(ge=0)
    suppressed_count: StrictInt = Field(ge=0)
    events: tuple[PriceAlertProducerEventRecord, ...]
    input_metadata: PriceRoundMetadata


class PriceRuntimeRuleFact(PriceRuntimeModel):
    owner_id: OwnerId
    rule_id: StrictStr = Field(min_length=1, max_length=128)
    rule_version: StrictInt = Field(ge=1)
    membership_version: StrictInt = Field(ge=1)
    ts_code: TsCode
    state: Literal["triggered", "not_triggered", "unavailable"]
    reason: StrictStr = Field(min_length=1, max_length=80)
    evaluated_at: AwareUtcDatetime
    next_allowed_at: AwareUtcDatetime | None
    last_triggered_at: AwareUtcDatetime | None


class PriceProducerRuntimeSnapshot(PriceRuntimeModel):
    source: PriceAlertSourceDescriptor
    round: PriceRoundReceipt | None
    rules: tuple[PriceRuntimeRuleFact, ...] = Field(max_length=3200)
    inspected_at: AwareUtcDatetime

    @model_validator(mode="after")
    def complete_head(self) -> Self:
        if self.round is not None and (
            self.round.evaluated_at > self.inspected_at
            or self.round.source_high_watermark != self.source.high_watermark
            or self.round.decision_count != len(self.rules)
        ):
            raise ValueError("price runtime snapshot differs from its committed current round")
        if len(self.wire_bytes()) > 1024 * 1024:
            raise ValueError("price runtime facts exceed the full 1 MiB domain")
        return self


_TABLES = frozenset(
    {
        "price_alert_runtime_identity",
        "price_alert_frequency_state",
        "price_alert_evaluation_head",
        "price_alert_round_receipt",
        "price_alert_event_log",
    }
)
_INSTALL_SQL = (
    (
        "CREATE TABLE price_alert_runtime_identity (key TEXT PRI"
        "MARY KEY, body BLOB NOT NULL, high_watermark INTEGER NO"
        "T NULL, last_evaluated_at TEXT, latest_round_id TEXT)"
    ),
    (
        "CREATE TABLE price_alert_frequency_state (owner_id TEXT"
        ",rule_id TEXT,membership_version INTEGER,policy_sha256 "
        "TEXT,next_allowed_at TEXT NOT NULL,last_observation TEX"
        "T NOT NULL,PRIMARY KEY(owner_id,rule_id,membership_vers"
        "ion,policy_sha256))"
    ),
    (
        "CREATE TABLE price_alert_evaluation_head (owner_id TEXT"
        ",rule_id TEXT,body BLOB NOT NULL,evaluated_at TEXT NOT "
        "NULL,round_id TEXT NOT NULL,PRIMARY KEY(owner_id,rule_i"
        "d))"
    ),
    (
        "CREATE TABLE price_alert_round_receipt (round_id TEXT P"
        "RIMARY KEY,input_sha256 TEXT NOT NULL,body BLOB NOT NUL"
        "L,evaluated_at TEXT NOT NULL)"
    ),
    (
        "CREATE TABLE price_alert_event_log (sequence INTEGER PR"
        "IMARY KEY,event_id TEXT UNIQUE NOT NULL,owner_id TEXT N"
        "OT NULL,rule_id TEXT NOT NULL,membership_version INTEGE"
        "R NOT NULL,policy_sha256 TEXT NOT NULL,observation_key "
        "TEXT NOT NULL,payload BLOB NOT NULL,payload_sha256 TEXT"
        " NOT NULL,UNIQUE(owner_id,rule_id,membership_version,po"
        "licy_sha256,observation_key))"
    ),
    "CREATE INDEX price_alert_round_time ON price_alert_round_receipt(evaluated_at DESC,round_id)",
    (
        "CREATE INDEX price_alert_event_rule_time ON price_alert"
        "_event_log(owner_id,rule_id,sequence DESC)"
    ),
)


def _private_parent(path: Path) -> None:
    if not path.is_absolute() or path != Path(os.path.abspath(path)):
        raise ValueError("price runtime ledger path must be normalized and absolute")
    current = Path(path.anchor)
    for component in path.parts[1:-1]:
        current /= component
        observed = current.lstat()
        if not stat.S_ISDIR(observed.st_mode):
            raise ValueError("price runtime ledger path contains a symlink")
    parent = path.parent.stat()
    if parent.st_uid != os.geteuid() or stat.S_IMODE(parent.st_mode) != 0o700:
        raise ValueError("price runtime ledger parent must be owned and private")


class PriceAlertRuntimeStore:
    def __init__(self, path: Path, *, activation: PriceAlertRuntimeActivation) -> None:
        self.path = Path(path)
        self.activation = activation
        self.failpoint: Callable[[str], None] = lambda _: None
        self._closed = False
        self._lock_descriptor = -1
        self._binding = require_price_alert_activation(activation, "evaluation")
        _private_parent(self.path)
        self._take_lock()
        try:
            self._identity = self._file_identity()
            self._verify_installation()
        except BaseException:
            self.close()
            raise

    @classmethod
    def install(
        cls, path: Path, *, activation: PriceAlertRuntimeActivation
    ) -> PriceAlertRuntimeStore:
        path = Path(path)
        binding = require_price_alert_activation(activation, "evaluation")
        _private_parent(path)
        if path.exists():
            return cls(path, activation=activation)
        marker = path.with_name(path.name + ".identity.json")
        if marker.exists():
            raise ValueError("registered price ledger is missing; refusing an empty replacement")
        descriptor = os.open(
            path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0), 0o600
        )
        os.close(descriptor)
        source = PriceAlertSourceDescriptor(
            source_id=binding.source_id,
            ledger_id=binding.ledger_id,
            source_epoch=binding.source_epoch,
            generation_id=binding.generation_id,
            producer_manifest_sha256=binding.producer_manifest_sha256,
            evaluation_contract_sha256=binding.evaluation_contract_sha256,
            frequency_policy_sha256=binding.frequency_policy_sha256,
            routing_policy_sha256=binding.routing_policy_sha256,
            first_sequence=1,
            high_watermark=0,
        )
        with sqlite3.connect(path, isolation_level=None) as connection:
            connection.execute("PRAGMA synchronous=FULL")
            connection.execute("BEGIN IMMEDIATE")
            try:
                for ddl in _INSTALL_SQL:
                    connection.execute(ddl)
                connection.execute(
                    "INSERT INTO price_alert_runtime_identity VALUES(?,?,?,NULL,NULL)",
                    ("current", source.wire_bytes(), 0),
                )
                connection.execute("COMMIT")
            except BaseException:
                connection.execute("ROLLBACK")
                raise
        descriptor = os.open(
            marker, os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0), 0o600
        )
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(source.wire_bytes())
            stream.flush()
            os.fsync(stream.fileno())
        directory = os.open(path.parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
        return cls(path, activation=activation)

    def _take_lock(self) -> None:
        lock = self.path.with_name(self.path.name + ".writer.lock")
        descriptor = os.open(lock, os.O_RDWR | os.O_CREAT | getattr(os, "O_NOFOLLOW", 0), 0o600)
        observed = os.fstat(descriptor)
        if (
            not stat.S_ISREG(observed.st_mode)
            or observed.st_uid != os.geteuid()
            or observed.st_nlink != 1
            or stat.S_IMODE(observed.st_mode) != 0o600
        ):
            os.close(descriptor)
            raise ValueError("price runtime writer lock is unsafe")
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BaseException:
            os.close(descriptor)
            raise
        self._lock_descriptor = descriptor

    def _file_identity(self) -> tuple[int, int]:
        try:
            info = self.path.lstat()
        except FileNotFoundError as exc:
            raise ValueError("registered price runtime ledger is missing") from exc
        if (
            not stat.S_ISREG(info.st_mode)
            or info.st_nlink != 1
            or info.st_uid != os.geteuid()
            or stat.S_IMODE(info.st_mode) != 0o600
        ):
            raise ValueError("price runtime ledger is unsafe")
        return info.st_dev, info.st_ino

    @contextmanager
    def _connection(self, *, write: bool = False) -> Iterator[sqlite3.Connection]:
        if self._closed or self._file_identity() != self._identity:
            raise ValueError("price runtime ledger identity changed or is closed")
        connection = sqlite3.connect(
            f"file:{self.path}?mode={'rw' if write else 'ro'}", uri=True, isolation_level=None
        )
        connection.row_factory = sqlite3.Row
        try:
            connection.execute("PRAGMA foreign_keys=ON")
            if write:
                connection.execute("PRAGMA synchronous=FULL")
            else:
                connection.execute("PRAGMA query_only=ON")
            connection.execute("BEGIN IMMEDIATE" if write else "BEGIN")
            yield connection
            if self._file_identity() != self._identity:
                raise ValueError("price runtime ledger changed during transaction")
            if write:
                connection.execute("COMMIT")
            else:
                connection.execute("ROLLBACK")
        except BaseException:
            if connection.in_transaction:
                connection.execute("ROLLBACK")
            raise
        finally:
            connection.close()

    def _verify_installation(self) -> None:
        from rquant.price_alert_runtime_contracts import _activation_bytes

        marker = _activation_bytes(
            self.path.with_name(self.path.name + ".identity.json"), self.path.parent
        )
        with self._connection() as connection:
            tables = {
                row[0]
                for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")
            }
            schema = {
                row[0]
                for row in connection.execute("SELECT sql FROM sqlite_master WHERE sql IS NOT NULL")
            }
            if tables != _TABLES or schema != set(_INSTALL_SQL):
                raise ValueError("price runtime installation marker or schema differs")
            row = connection.execute(
                "SELECT * FROM price_alert_runtime_identity WHERE key='current'"
            ).fetchone()
            if row is None or bytes(row["body"]) != marker:
                raise ValueError("price runtime ledger differs from its registered identity")
            source = PriceAlertSourceDescriptor.model_validate_json(marker)
            expected = self._binding
            if (
                source.source_id,
                source.ledger_id,
                source.source_epoch,
                source.generation_id,
                source.producer_manifest_sha256,
                source.evaluation_contract_sha256,
                source.frequency_policy_sha256,
                source.routing_policy_sha256,
            ) != (
                expected.source_id,
                expected.ledger_id,
                expected.source_epoch,
                expected.generation_id,
                expected.producer_manifest_sha256,
                expected.evaluation_contract_sha256,
                expected.frequency_policy_sha256,
                expected.routing_policy_sha256,
            ):
                raise ValueError("price runtime source or epoch binding differs")
            maximum, count = connection.execute(
                "SELECT COALESCE(MAX(sequence),0),COUNT(*) FROM price_alert_event_log"
            ).fetchone()
            if row["high_watermark"] != maximum or maximum != count:
                raise ValueError("price runtime event sequence has regressed or has a gap")

    def _source(self, connection: sqlite3.Connection) -> PriceAlertSourceDescriptor:
        row = connection.execute(
            "SELECT body,high_watermark FROM price_alert_runtime_identity WHERE key='current'"
        ).fetchone()
        source = PriceAlertSourceDescriptor.model_validate_json(row["body"])
        return PriceAlertSourceDescriptor(
            **source.model_dump(exclude={"high_watermark"}), high_watermark=row["high_watermark"]
        )

    def source_descriptor(self) -> PriceAlertSourceDescriptor:
        with self._connection() as connection:
            return self._source(connection)

    @staticmethod
    def _event_record(row: sqlite3.Row) -> PriceAlertProducerEventRecord:
        payload = bytes(row["payload"])
        return PriceAlertProducerEventRecord(
            sequence=row["sequence"],
            event=parse_price_alert_event(payload),
            payload_json=payload.decode(),
            payload_sha256=row["payload_sha256"],
        )

    def events_after(
        self, after: int, *, inspected_at: datetime, limit: int = 100
    ) -> tuple[PriceAlertProducerEventRecord, ...]:
        if type(after) is not int or after < 0 or type(limit) is not int or not 1 <= limit <= 100:
            raise ValueError("price event read range is outside the route budget")
        with self._connection() as connection:
            source = self._source(connection)
            if after > source.high_watermark:
                raise ValueError("price source high watermark has regressed")
            rows = connection.execute(
                "SELECT * FROM price_alert_event_log WHERE sequence>? ORDER BY sequence LIMIT ?",
                (after, limit),
            ).fetchall()
            records = tuple(self._event_record(row) for row in rows)
            for expected, record in enumerate(records, after + 1):
                if record.sequence != expected or record.event.available_at > inspected_at:
                    raise ValueError("price source is discontinuous or not visible")
            return records

    def commit_round(
        self,
        round_input: PriceRoundInput,
        *,
        policy: PriceAlertFrequencyPolicy,
        current_scope: Callable[[], bool] | None = None,
    ) -> PriceRoundReceipt:
        binding = require_price_alert_activation(self.activation, "evaluation")
        if (
            type(round_input) is not PriceRoundInput
            or type(policy) is not PriceAlertFrequencyPolicy
        ):
            raise TypeError("price round needs exact typed inputs")
        round_input = PriceRoundInput.model_validate(round_input)
        policy = PriceAlertFrequencyPolicy.model_validate(policy)
        if binding.frequency_policy_sha256 != policy.sha256:
            raise ValueError("price cooldown policy differs from the actual activation")
        payload = round_input.wire_bytes()
        digest = sha256(payload).hexdigest()
        events = []
        suppressed = 0
        with self._connection(write=True) as connection:
            previous = connection.execute(
                "SELECT input_sha256,body FROM price_alert_round_receipt WHERE round_id=?",
                (digest,),
            ).fetchone()
            if previous is not None:
                if previous["input_sha256"] != digest:
                    raise ValueError("price round replay body conflicts")
                return PriceRoundReceipt.model_validate_json(previous["body"])
            head = connection.execute(
                "SELECT last_evaluated_at FROM price_alert_runtime_identity WHERE key='current'"
            ).fetchone()[0]
            if head is not None and round_input.evaluated_at < datetime.fromisoformat(head):
                raise ValueError("price runtime evaluation clock regressed")
            high = self._source(connection).high_watermark
            total_bytes = sum(
                candidate.stat().st_size
                for candidate in (
                    self.path,
                    Path(str(self.path) + "-wal"),
                    Path(str(self.path) + "-journal"),
                )
                if candidate.exists()
            )
            if high > 100000 or total_bytes + len(payload) * 8 + 1024 * 1024 > 512 * 1024 * 1024:
                raise PriceAlertCapacityExceeded("price runtime producer capacity is exhausted")
            for record in round_input.records:
                connection.execute(
                    (
                        "INSERT INTO price_alert_evaluation_head VALUES(?,?,?,?,?) "
                        "ON CONFLICT(owner_id,rule_id) DO UPDATE SET "
                        "body=excluded.body,evaluated_at=excluded.evaluated_at,round_"
                        "id=excluded.round_id"
                    ),
                    (
                        record.owner_id,
                        record.rule_id,
                        record.wire_bytes(),
                        utc_text(round_input.evaluated_at),
                        digest,
                    ),
                )
                self.failpoint("head")
                item = record.event
                if item is None:
                    continue
                if (
                    item.producer_manifest_sha256,
                    item.producer_commit,
                    item.source_epoch,
                    item.frequency_policy_sha256,
                ) != (
                    binding.producer_manifest_sha256,
                    binding.producer_commit,
                    binding.source_epoch,
                    policy.sha256,
                ):
                    raise ValueError("price event producer or source binding differs")
                key = (item.owner_id, item.rule_id, item.membership_version, policy.sha256)
                prior_event = connection.execute(
                    (
                        "SELECT * FROM price_alert_event_log WHERE owner_id=? AND "
                        "rule_id=? AND membership_version=? AND policy_sha256=? AND "
                        "observation_key=?"
                    ),
                    (*key, item.observation_key),
                ).fetchone()
                frequency = connection.execute(
                    "SELECT * FROM price_alert_frequency_state WHERE "
                    "owner_id=? AND rule_id=? AND membership_version=? AND policy_sha256=?",
                    key,
                ).fetchone()
                if prior_event is not None or (
                    frequency is not None
                    and round_input.evaluated_at
                    < datetime.fromisoformat(frequency["next_allowed_at"])
                ):
                    suppressed += 1
                    continue
                try:
                    require_price_alert_activation(self.activation, "event_write")
                except ValueError:
                    suppressed += 1
                    continue
                if high >= 100000:
                    raise PriceAlertCapacityExceeded(
                        "price producer exceeds 100000 immutable events"
                    )
                encoded = item.wire_bytes()
                high += 1
                connection.execute(
                    (
                        "INSERT INTO price_alert_frequency_state VALUES(?,?,?,?,?,?) "
                        "ON CONFLICT(owner_id,rule_id,membership_version,policy_sha25"
                        "6) DO UPDATE SET next_allowed_at=excluded.next_allowed_at,la"
                        "st_observation=excluded.last_observation"
                    ),
                    (
                        *key,
                        utc_text(
                            round_input.evaluated_at + timedelta(seconds=policy.cooldown_seconds)
                        ),
                        item.observation_key,
                    ),
                )
                self.failpoint("cooldown")
                connection.execute(
                    "INSERT INTO price_alert_event_log VALUES(?,?,?,?,?,?,?,?,?)",
                    (
                        high,
                        item.event_id,
                        item.owner_id,
                        item.rule_id,
                        item.membership_version,
                        policy.sha256,
                        item.observation_key,
                        encoded,
                        sha256(encoded).hexdigest(),
                    ),
                )
                self.failpoint("event")
                events.append(
                    PriceAlertProducerEventRecord(
                        sequence=high,
                        event=item,
                        payload_json=encoded.decode(),
                        payload_sha256=sha256(encoded).hexdigest(),
                    )
                )
            receipt = PriceRoundReceipt(
                round_id=digest,
                input_sha256=digest,
                evaluated_at=round_input.evaluated_at,
                source_high_watermark=high,
                decision_count=len(round_input.records),
                suppressed_count=suppressed,
                events=tuple(events),
                input_metadata=PriceRoundMetadata(
                    availability="ready",
                    **round_input.model_dump(mode="python", exclude={"records", "evaluated_at"}),
                ),
            )
            connection.execute(
                "INSERT INTO price_alert_round_receipt VALUES(?,?,?,?)",
                (digest, digest, receipt.wire_bytes(), utc_text(round_input.evaluated_at)),
            )
            self.failpoint("receipt")
            connection.execute(
                (
                    "UPDATE price_alert_runtime_identity SET high_watermark="
                    "?,last_evaluated_at=?,latest_round_id=? WHERE key='curr"
                    "ent'"
                ),
                (high, utc_text(round_input.evaluated_at), digest),
            )
            if current_scope is not None and current_scope() is not True:
                raise ValueError("price rule current generation changed before commit")
            self.failpoint("before_commit")
        self.failpoint("after_commit")
        return receipt

    def close(self) -> None:
        if self._lock_descriptor >= 0:
            fcntl.flock(self._lock_descriptor, fcntl.LOCK_UN)
            os.close(self._lock_descriptor)
            self._lock_descriptor = -1
        self._closed = True

    def record_unavailable(self, *, evaluated_at: datetime, reason: str) -> PriceRoundReceipt:
        require_price_alert_activation(self.activation, "evaluation")
        payload = canonical_json_bytes(
            {
                "availability": "unavailable",
                "reason": reason,
                "evaluated_at": utc_text(evaluated_at),
            }
        )
        digest = sha256(payload).hexdigest()
        with self._connection(write=True) as connection:
            previous = connection.execute(
                "SELECT body FROM price_alert_round_receipt WHERE round_id=?", (digest,)
            ).fetchone()
            if previous is not None:
                return PriceRoundReceipt.model_validate_json(previous[0])
            head = connection.execute(
                "SELECT last_evaluated_at FROM price_alert_runtime_identity WHERE key='current'"
            ).fetchone()[0]
            if head is not None and evaluated_at < datetime.fromisoformat(head):
                raise ValueError("price runtime clock regressed")
            total_bytes = sum(
                candidate.stat().st_size
                for candidate in (
                    self.path,
                    Path(str(self.path) + "-wal"),
                    Path(str(self.path) + "-journal"),
                )
                if candidate.exists()
            )
            if total_bytes + len(payload) * 8 + 1024 * 1024 > 512 * 1024 * 1024:
                raise ValueError("price runtime producer capacity is exhausted")
            receipt = PriceRoundReceipt(
                round_id=digest,
                input_sha256=digest,
                evaluated_at=evaluated_at,
                source_high_watermark=self._source(connection).high_watermark,
                decision_count=0,
                suppressed_count=0,
                events=(),
                input_metadata=PriceRoundMetadata(availability="unavailable", reason=reason),
            )
            connection.execute(
                "INSERT INTO price_alert_round_receipt VALUES(?,?,?,?)",
                (digest, digest, receipt.wire_bytes(), utc_text(evaluated_at)),
            )
            connection.execute(
                (
                    "UPDATE price_alert_runtime_identity SET last_evaluated_"
                    "at=?,latest_round_id=? WHERE key='current'"
                ),
                (utc_text(evaluated_at), digest),
            )
        return receipt

    def runtime_snapshot(self, *, observed_at: datetime) -> PriceProducerRuntimeSnapshot:
        with self._connection() as connection:
            source = self._source(connection)
            latest = connection.execute(
                "SELECT r.body FROM price_alert_runtime_identity i JOIN "
                "price_alert_round_receipt r ON r.round_id=i.latest_roun"
                "d_id WHERE i.key='current'"
            ).fetchone()
            round_receipt = (
                None if latest is None else PriceRoundReceipt.model_validate_json(latest[0])
            )
            facts = []
            if round_receipt is not None and round_receipt.input_metadata.availability == "ready":
                heads = connection.execute(
                    (
                        "SELECT body FROM price_alert_evaluation_head WHERE roun"
                        "d_id=? ORDER BY owner_id,rule_id LIMIT 3201"
                    ),
                    (round_receipt.round_id,),
                ).fetchall()
                if len(heads) > 3200:
                    raise ValueError("price runtime current heads exceed capacity")
                for raw in heads:
                    head = PriceEvaluationRecord.model_validate_json(raw[0])
                    frequency = connection.execute(
                        (
                            "SELECT next_allowed_at FROM price_alert_frequency_state "
                            "WHERE owner_id=? AND rule_id=? AND membership_version=? AND "
                            "policy_sha256=?"
                        ),
                        (
                            head.owner_id,
                            head.rule_id,
                            head.membership_version,
                            source.frequency_policy_sha256,
                        ),
                    ).fetchone()
                    last = connection.execute(
                        (
                            "SELECT * FROM price_alert_event_log WHERE owner_id=? AN"
                            "D rule_id=? ORDER BY sequence DESC LIMIT 1"
                        ),
                        (head.owner_id, head.rule_id),
                    ).fetchone()
                    event = None if last is None else self._event_record(last)
                    facts.append(
                        PriceRuntimeRuleFact(
                            **head.model_dump(mode="python", exclude={"event"}),
                            evaluated_at=round_receipt.evaluated_at,
                            next_allowed_at=None
                            if frequency is None
                            else datetime.fromisoformat(frequency[0]),
                            last_triggered_at=None if event is None else event.event.evaluated_at,
                        )
                    )
            compact = (
                None if round_receipt is None else round_receipt.model_copy(update={"events": ()})
            )
            return PriceProducerRuntimeSnapshot(
                source=source, round=compact, rules=tuple(facts), inspected_at=observed_at
            )


class ReadonlyPriceAlertRuntimeStore(PriceAlertRuntimeStore):
    def __init__(self, path: Path, *, activation: PriceAlertRuntimeActivation) -> None:
        from rquant.price_alert_runtime_contracts import require_verified_price_alert_activation

        self.path = Path(path)
        _private_parent(self.path)
        self.activation = activation
        self._binding = require_verified_price_alert_activation(activation, "price_alert_runtime")
        self._closed = False
        self._lock_descriptor = -1
        self._identity = self._file_identity()
        self._verify_installation()

    @contextmanager
    def _connection(self, *, write: bool = False) -> Iterator[sqlite3.Connection]:
        if write:
            raise TypeError("price peer ledger is read-only")
        with super()._connection(write=False) as connection:
            yield connection
