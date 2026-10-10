"""Transaction-owned AI usage in the original PageControl SQLite journal."""

from __future__ import annotations

import hashlib
import sqlite3
from dataclasses import dataclass
from datetime import date, datetime
from typing import Literal, Self
from uuid import UUID, uuid4

from pydantic import Field, JsonValue, model_validator

from rquant.ai_assistance_contracts import AIMeasuredUsage, AIRequestBinding, Sha256
from rquant.runtime_contracts import AwareUtcDatetime, RuntimeContractModel
from rquant.strict_json import canonical_json_bytes, strict_canonical_json_loads

MAX_RESULT_BYTES = 1024 * 1024


class AIRequestConflict(ValueError):
    pass


class AIRequestNotFound(LookupError):
    pass


class AIBudgetExceeded(ValueError):
    pass


class AITransactionRequired(RuntimeError):
    pass


class AIUsageRecord(RuntimeContractModel):
    binding: AIRequestBinding
    state: Literal["reserved", "dispatched", "completed", "unknown", "not_dispatched"]
    dispatched_at: AwareUtcDatetime | None = None
    completed_at: AwareUtcDatetime | None = None
    dispatch_token: str | None = Field(default=None, pattern=r"^[0-9a-f]{32}$")
    usage: AIMeasuredUsage = AIMeasuredUsage()
    result: JsonValue | None = None
    result_sha256: Sha256 | None = None
    error_code: str | None = Field(default=None, pattern=r"^[a-z][a-z0-9_]{0,63}$")

    @model_validator(mode="after")
    def validate_lifecycle(self) -> Self:
        sent = self.state in {"dispatched", "completed", "unknown"}
        if sent != (self.dispatched_at is not None and self.dispatch_token is not None):
            raise ValueError("AI journal dispatch identity is inconsistent")
        if not sent and (self.dispatched_at is not None or self.dispatch_token is not None):
            raise ValueError("unsent AI record has a dispatch identity")
        if self.dispatched_at is not None and self.dispatched_at < self.binding.reserved_at:
            raise ValueError("AI dispatch precedes its reservation")
        if self.completed_at is not None and self.completed_at < (self.dispatched_at or self.binding.reserved_at):
            raise ValueError("AI completion precedes original acceptance")
        terminal = self.state in {"completed", "unknown", "not_dispatched"}
        if terminal != (self.completed_at is not None):
            raise ValueError("AI terminal timestamp is inconsistent")
        expected_hash = None if self.result is None else hashlib.sha256(canonical_json_bytes(self.result)).hexdigest()
        if self.result_sha256 != expected_hash:
            raise ValueError("AI journal result hash differs from its immutable content")
        if self.state != "completed" and (self.result is not None or self.usage.input_tokens is not None or self.usage.output_tokens is not None):
            raise ValueError("uncompleted AI record cannot claim a measured result")
        return self


class AIUsageDispatch(RuntimeContractModel):
    claimed: bool
    token: str | None = None
    record: AIUsageRecord


class AIUsageDay(RuntimeContractModel):
    day: date
    calls: int = Field(strict=True, ge=0)
    input_tokens: int | None = Field(default=None, strict=True, ge=0)
    output_tokens: int | None = Field(default=None, strict=True, ge=0)
    known_input_tokens: int = Field(strict=True, ge=0)
    known_output_tokens: int = Field(strict=True, ge=0)
    unknown_usage_calls: int = Field(strict=True, ge=0)


class AIUsageSummary(RuntimeContractModel):
    start_date: date
    end_date: date
    calls: int = Field(strict=True, ge=0)
    input_tokens: int | None = Field(default=None, strict=True, ge=0)
    output_tokens: int | None = Field(default=None, strict=True, ge=0)
    known_input_tokens: int = Field(strict=True, ge=0)
    known_output_tokens: int = Field(strict=True, ge=0)
    unknown_usage_calls: int = Field(strict=True, ge=0)
    days: tuple[AIUsageDay, ...]


def install_ai_usage_tables(connection: sqlite3.Connection) -> None:
    connection.execute("""
        CREATE TABLE IF NOT EXISTS ai_request (
            request_id TEXT PRIMARY KEY,
            owner_uid TEXT NOT NULL,
            account_id TEXT NOT NULL,
            budget_date TEXT NOT NULL,
            body_sha256 TEXT NOT NULL,
            binding_json TEXT NOT NULL,
            state TEXT NOT NULL CHECK(state IN ('reserved','dispatched','completed','unknown','not_dispatched')),
            dispatch_token TEXT,
            record_json TEXT NOT NULL
        )
    """)
    connection.execute("CREATE INDEX IF NOT EXISTS ai_account_day ON ai_request(account_id,budget_date,state)")
    connection.execute("CREATE INDEX IF NOT EXISTS ai_owner_day ON ai_request(owner_uid,account_id,budget_date)")


def _limit(value: int) -> int:
    if type(value) is not int or not 0 <= value <= 2**63 - 1:
        raise ValueError("AI daily call limit requires a bounded nonnegative integer")
    return value


def _same_original(left: AIRequestBinding, right: AIRequestBinding) -> bool:
    excluded = {"reserved_at", "budget_date"}
    return left.model_dump(exclude=excluded) == right.model_dump(exclude=excluded)


class AIUsageRepository:
    def __init__(self, connection: sqlite3.Connection) -> None:
        self.connection = connection

    def _transaction(self) -> None:
        if not self.connection.in_transaction:
            raise AITransactionRequired("AI writes require the original owner transaction")

    def lookup(self, owner_uid: str, request_id: UUID, body_sha256: str | None = None) -> AIUsageRecord:
        row = self.connection.execute(
            "SELECT owner_uid,body_sha256,record_json FROM ai_request WHERE request_id=?", (str(request_id),)
        ).fetchone()
        if row is None or row[0] != owner_uid:
            raise AIRequestNotFound("original AI request is not available")
        if body_sha256 is not None and row[1] != body_sha256:
            raise AIRequestConflict("original AI request body differs")
        record = AIUsageRecord.model_validate_json(row[2])
        if record.binding.owner_uid != owner_uid or record.binding.request_id != request_id or record.binding.request_body_sha256 != row[1]:
            raise AIRequestConflict("stored AI request binding differs")
        return record

    def reserve(self, binding: AIRequestBinding, *, daily_limit: int) -> AIUsageRecord:
        self._transaction()
        value = AIRequestBinding.model_validate(binding)
        exists = self.connection.execute("SELECT 1 FROM ai_request WHERE request_id=?", (str(value.request_id),)).fetchone()
        if exists is not None:
            original = self.lookup(value.owner_uid, value.request_id, value.request_body_sha256)
            if not _same_original(original.binding, value):
                raise AIRequestConflict("original AI configuration or context differs")
            return original
        capacity = _limit(daily_limit)
        used = self.connection.execute(
            "SELECT COUNT(*) FROM ai_request WHERE account_id=? AND budget_date=? AND state!='not_dispatched'",
            (value.account_id, value.budget_date.isoformat()),
        ).fetchone()[0]
        if used >= capacity:
            raise AIBudgetExceeded("AI account daily call allowance is exhausted")
        record = AIUsageRecord(binding=value, state="reserved")
        self.connection.execute(
            "INSERT INTO ai_request VALUES(?,?,?,?,?,?,?,NULL,?)",
            (str(value.request_id), value.owner_uid, value.account_id, value.budget_date.isoformat(),
             value.request_body_sha256, value.model_dump_json(), record.state, record.model_dump_json()),
        )
        return record

    def _save(self, record: AIUsageRecord) -> AIUsageRecord:
        self._transaction()
        value = AIUsageRecord.model_validate(record)
        self.connection.execute(
            "UPDATE ai_request SET state=?,dispatch_token=?,record_json=? WHERE request_id=? AND owner_uid=? AND body_sha256=?",
            (value.state, value.dispatch_token, value.model_dump_json(), str(value.binding.request_id),
             value.binding.owner_uid, value.binding.request_body_sha256),
        )
        return value

    def reuse_interpretation_cache(self, binding: AIRequestBinding, *, now: datetime) -> AIUsageRecord:
        """Journal a reverified cache read without claiming a provider dispatch."""
        self._transaction()
        value = AIRequestBinding.model_validate(binding)
        if value.purpose != "interpretation":
            raise ValueError("only an original interpretation can reuse its sealed cache")
        exists = self.connection.execute(
            "SELECT 1 FROM ai_request WHERE request_id=?", (str(value.request_id),)
        ).fetchone()
        if exists is not None:
            original = self.lookup(value.owner_uid, value.request_id, value.request_body_sha256)
            if not _same_original(original.binding, value):
                raise AIRequestConflict("original AI configuration or context differs")
            if original.state != "reserved":
                return original
            return self._save(AIUsageRecord(
                binding=original.binding, state="not_dispatched", completed_at=now,
                error_code="cache_reused",
            ))
        record = AIUsageRecord(
            binding=value, state="not_dispatched", completed_at=now, error_code="cache_reused",
        )
        self.connection.execute(
            "INSERT INTO ai_request VALUES(?,?,?,?,?,?,?,NULL,?)",
            (str(value.request_id), value.owner_uid, value.account_id, value.budget_date.isoformat(),
             value.request_body_sha256, value.model_dump_json(), record.state, record.model_dump_json()),
        )
        return record

    def dispatch(self, owner_uid: str, request_id: UUID, body_sha256: str, *, now: datetime) -> AIUsageDispatch:
        self._transaction()
        record = self.lookup(owner_uid, request_id, body_sha256)
        if record.state != "reserved":
            return AIUsageDispatch(claimed=False, record=record)
        token = uuid4().hex
        updated = self._save(AIUsageRecord(**{**record.model_dump(), "state": "dispatched", "dispatched_at": now, "dispatch_token": token}))
        return AIUsageDispatch(claimed=True, token=token, record=updated)

    def finish(self, owner_uid: str, request_id: UUID, body_sha256: str, *, dispatch_token: str,
               now: datetime, usage: AIMeasuredUsage, result: JsonValue | None,
               error_code: str | None = None) -> AIUsageRecord:
        self._transaction()
        record = self.lookup(owner_uid, request_id, body_sha256)
        if record.dispatch_token != dispatch_token or record.state not in {"dispatched", "unknown", "completed"}:
            raise AIRequestConflict("AI completion does not own the original dispatch")
        measured = AIMeasuredUsage.model_validate(usage)
        raw = None if result is None else canonical_json_bytes(result)
        if raw is not None and len(raw) > MAX_RESULT_BYTES:
            raise ValueError("AI result exceeds the sealed result budget")
        parsed = None if raw is None else strict_canonical_json_loads(raw)
        result_hash = None if raw is None else hashlib.sha256(raw).hexdigest()
        if record.state == "completed":
            if (record.usage, record.result, record.error_code, record.result_sha256) != (measured, parsed, error_code, result_hash):
                raise AIRequestConflict("completed AI result is immutable")
            return record
        return self._save(AIUsageRecord(**{**record.model_dump(), "state": "completed", "completed_at": now,
                                          "usage": measured, "result": parsed, "result_sha256": result_hash, "error_code": error_code}))

    def unknown(self, owner_uid: str, request_id: UUID, body_sha256: str, *, dispatch_token: str,
                now: datetime) -> AIUsageRecord:
        self._transaction()
        record = self.lookup(owner_uid, request_id, body_sha256)
        if record.dispatch_token != dispatch_token:
            raise AIRequestConflict("unknown AI call does not own its original dispatch")
        if record.state in {"completed", "unknown"}:
            return record
        if record.state != "dispatched":
            raise AIRequestConflict("only an original dispatched call can be unknown")
        return self._save(AIUsageRecord(**{**record.model_dump(), "state": "unknown", "completed_at": now, "error_code": "response_unknown"}))

    def recover_dispatches(self, *, now: datetime) -> int:
        self._transaction()
        rows = self.connection.execute("SELECT owner_uid,request_id,body_sha256,dispatch_token FROM ai_request WHERE state='dispatched'")
        count = 0
        for owner, request_id, body, token in rows:
            self.unknown(owner, UUID(request_id), body, dispatch_token=token, now=now)
            count += 1
        return count

    def release(self, owner_uid: str, request_id: UUID, body_sha256: str, *, now: datetime, reason: str) -> AIUsageRecord:
        self._transaction()
        record = self.lookup(owner_uid, request_id, body_sha256)
        if reason != "not_dispatched" or record.state not in {"reserved", "not_dispatched"} or record.dispatch_token is not None:
            raise AIRequestConflict("AI call has no proof that it was not dispatched")
        if record.state == "not_dispatched":
            return record
        return self._save(AIUsageRecord(**{**record.model_dump(), "state": "not_dispatched", "completed_at": now,
                                          "error_code": reason}))

    def summary(self, owner_uid: str, account_id: str, *, start_date: date, end_date: date) -> AIUsageSummary:
        if not 0 <= (end_date - start_date).days <= 366:
            raise ValueError("AI usage query requires a bounded complete date range")
        # Sum in Python, not SQLite's signed 64-bit SUM: measured counters remain exact.
        grouped: dict[date, _UsageTotals] = {}
        total = _UsageTotals()
        cursor = self.connection.execute(
            "SELECT budget_date,record_json FROM ai_request WHERE owner_uid=? AND account_id=? AND budget_date>=? AND budget_date<=? AND state!='not_dispatched' ORDER BY budget_date DESC",
            (owner_uid, account_id, start_date.isoformat(), end_date.isoformat()),
        )
        for day_text, raw in cursor:
            record = AIUsageRecord.model_validate_json(raw)
            if record.binding.owner_uid != owner_uid or record.binding.account_id != account_id or record.binding.budget_date.isoformat() != day_text:
                raise AIRequestConflict("usage history binding differs from original index")
            grouped.setdefault(date.fromisoformat(day_text), _UsageTotals()).add(record.usage)
            total.add(record.usage)
        days = tuple(AIUsageDay(day=day, **measured.values()) for day, measured in sorted(grouped.items(), reverse=True))
        return AIUsageSummary(start_date=start_date, end_date=end_date, days=days, **total.values())


def _totals(values: list[AIMeasuredUsage]) -> dict[str, int | None]:
    known_input = sum(value.input_tokens or 0 for value in values)
    known_output = sum(value.output_tokens or 0 for value in values)
    return {"calls": len(values), "input_tokens": None if any(value.input_tokens is None for value in values) else known_input,
            "output_tokens": None if any(value.output_tokens is None for value in values) else known_output,
            "known_input_tokens": known_input, "known_output_tokens": known_output,
            "unknown_usage_calls": sum(not value.known for value in values)}


@dataclass
class _UsageTotals:
    calls: int = 0
    known_input: int = 0
    known_output: int = 0
    missing_input: bool = False
    missing_output: bool = False
    unknown_calls: int = 0

    def add(self, usage: AIMeasuredUsage) -> None:
        self.calls += 1
        self.known_input += usage.input_tokens or 0
        self.known_output += usage.output_tokens or 0
        self.missing_input |= usage.input_tokens is None
        self.missing_output |= usage.output_tokens is None
        self.unknown_calls += not usage.known

    def values(self) -> dict[str, int | None]:
        return {'calls':self.calls,'input_tokens':None if self.missing_input else self.known_input,
                'output_tokens':None if self.missing_output else self.known_output,
                'known_input_tokens':self.known_input,'known_output_tokens':self.known_output,
                'unknown_usage_calls':self.unknown_calls}
