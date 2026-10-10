"""Condition namespace on the original PageControl rule transaction."""

from __future__ import annotations

import sqlite3
from datetime import datetime
from typing import Self

from pydantic import Field, StrictBool, StrictInt, model_validator

from rquant.alert_rule_contracts import ConditionAlertRuleDefinition, ConditionAlertScopeEvidence
from rquant.manual_watchlist import OwnerId
from rquant.price_alert_rule_store import (
    MAX_ACTIVE_RULES,
    PriceAlertRuleCapacityError,
    PriceAlertRuleIntegrityError,
    PriceAlertRuleKey,
    PriceAlertRuleRepository,
    PriceAlertRuleScopeError,
    PriceAlertRuleUnavailableError,
    PriceAlertRuleVersionConflictError,
)
from rquant.runtime_contracts import AwareUtcDatetime, RuntimeContractModel, normalize_aware_utc

_DDL = (
    "CREATE TABLE condition_alert_rule(owner_id TEXT NOT NULL,rule_id"
    " TEXT NOT NULL,version INTEGER NOT NULL CHECK(version>=1),delete"
    "d INTEGER NOT NULL CHECK(deleted IN (0,1)),rule_json TEXT,update"
    "d_at_utc TEXT NOT NULL,PRIMARY KEY(owner_id,rule_id),CHECK((dele"
    "ted=1 AND rule_json IS NULL) OR (deleted=0 AND rule_json IS NOT "
    "NULL)))"
)
_INDEX = (
    "CREATE INDEX condition_alert_rule_owner_live_idx ON condition_al"
    "ert_rule(owner_id,deleted,rule_id)"
)


class ConditionAlertRuleUpsert(RuntimeContractModel):
    owner_id: OwnerId
    expected_version: StrictInt | None = Field(default=None, ge=1)
    rule: ConditionAlertRuleDefinition


class ConditionAlertRuleEntry(PriceAlertRuleKey):
    version: StrictInt = Field(ge=1)
    deleted: StrictBool
    rule: ConditionAlertRuleDefinition | None
    updated_at: AwareUtcDatetime

    @model_validator(mode="after")
    def closed_head(self) -> Self:
        if self.deleted != (self.rule is None) or (
            self.rule is not None and self.rule.rule_id != self.rule_id
        ):
            raise ValueError("condition rule head or tombstone differs from its identity")
        return self


class ConditionAlertRuleRepository(PriceAlertRuleRepository):
    def _require_schema(self) -> None:
        rows = self._connection.execute(
            "SELECT sql FROM sqlite_master WHERE tbl_name='condition_alert_ru"
            "le' AND sql IS NOT NULL"
        ).fetchall()
        if {row[0] for row in rows} != {_DDL, _INDEX}:
            raise PriceAlertRuleUnavailableError(
                "condition rule schema is not explicitly installed"
            )

    def install_schema(self) -> None:
        self._require_transaction()
        present = self._connection.execute(
            "SELECT 1 FROM sqlite_master WHERE name='condition_alert_rule'"
        ).fetchone()
        if present is None:
            self._connection.execute(_DDL)
            self._connection.execute(_INDEX)
        self._require_schema()

    @staticmethod
    def _entry(row: sqlite3.Row | tuple) -> ConditionAlertRuleEntry:
        owner, rule_id, version, deleted, body, at = row
        if type(version) is not int or type(deleted) is not int or deleted not in (0, 1):
            raise PriceAlertRuleIntegrityError("condition head scalar types differ")
        return ConditionAlertRuleEntry(
            owner_id=owner,
            rule_id=rule_id,
            version=version,
            deleted=bool(deleted),
            rule=None if body is None else ConditionAlertRuleDefinition.model_validate_json(body),
            updated_at=datetime.fromisoformat(at),
        )

    def get(self, key: PriceAlertRuleKey) -> ConditionAlertRuleEntry | None:
        self._require_schema()
        key = PriceAlertRuleKey.model_validate(key)
        row = self._connection.execute(
            "SELECT owner_id,rule_id,version,deleted,rule_json,updated_at_utc"
            " FROM condition_alert_rule WHERE owner_id=? AND rule_id=?",
            (key.owner_id, key.rule_id),
        ).fetchone()
        return None if row is None else self._entry(row)

    def list_current(self, owner_id: str) -> tuple[ConditionAlertRuleEntry, ...]:
        self._require_schema()
        owner = PriceAlertRuleKey(owner_id=owner_id, rule_id="query").owner_id
        rows = self._connection.execute(
            "SELECT owner_id,rule_id,version,deleted,rule_json,updated_at_utc"
            " FROM condition_alert_rule WHERE owner_id=? AND deleted=0 ORDER "
            "BY rule_id LIMIT ?",
            (owner, MAX_ACTIVE_RULES + 1),
        ).fetchall()
        if len(rows) > MAX_ACTIVE_RULES:
            raise PriceAlertRuleCapacityError("condition rule full head exceeds its owner budget")
        return tuple(self._entry(row) for row in rows)

    @staticmethod
    def _scope(
        command: ConditionAlertRuleUpsert,
        evidence: ConditionAlertScopeEvidence | None,
        observed: datetime,
    ) -> None:
        if evidence is None:
            if command.rule.enabled:
                raise PriceAlertRuleScopeError("condition scope is unavailable")
            return
        if type(evidence) is not ConditionAlertScopeEvidence:
            raise TypeError("condition save requires exact server-resolved scope evidence")
        evidence = ConditionAlertScopeEvidence.model_validate(evidence)
        if (
            evidence.owner_id != command.owner_id
            or evidence.scope != command.rule.scope
            or evidence.available_at > observed
        ):
            raise PriceAlertRuleScopeError(
                "condition scope owner, definition or visibility differs"
            )

    def upsert(
        self,
        command: ConditionAlertRuleUpsert,
        *,
        now: datetime,
        scope: ConditionAlertScopeEvidence | None = None,
    ) -> ConditionAlertRuleEntry:
        self._require_transaction()
        command = ConditionAlertRuleUpsert.model_validate(command)
        observed = normalize_aware_utc(now)
        self._scope(command, scope, observed)
        key = PriceAlertRuleKey(owner_id=command.owner_id, rule_id=command.rule.rule_id)
        current = self.get(key)
        if current is None:
            if command.expected_version is not None:
                raise PriceAlertRuleVersionConflictError("condition rule head does not exist")
            version = 1
        elif command.expected_version != current.version:
            raise PriceAlertRuleVersionConflictError("condition rule head changed")
        else:
            version = current.version + 1
        if (current is None or current.deleted) and len(
            self.list_current(command.owner_id)
        ) >= MAX_ACTIVE_RULES:
            raise PriceAlertRuleCapacityError("owner condition rule limit reached")
        body = command.rule.model_dump_json()
        if len(body.encode()) > 128 * 1024:
            raise ValueError("condition rule exceeds its full body budget")
        if current is None:
            self._connection.execute(
                "INSERT INTO condition_alert_rule VALUES(?,?,?,0,?,?)",
                (key.owner_id, key.rule_id, version, body, observed.isoformat()),
            )
        else:
            updated = self._connection.execute(
                "UPDATE condition_alert_rule SET version=?,deleted=0,rule_json=?,"
                "updated_at_utc=? WHERE owner_id=? AND rule_id=? AND version=?",
                (version, body, observed.isoformat(), key.owner_id, key.rule_id, current.version),
            )
            if updated.rowcount != 1:
                raise PriceAlertRuleVersionConflictError(
                    "condition rule head changed during update"
                )
        result = self.get(key)
        if result is None:
            raise PriceAlertRuleIntegrityError("condition rule write has no actual head")
        return result

    def delete_current(
        self, key: PriceAlertRuleKey, *, expected_version: int, now: datetime
    ) -> ConditionAlertRuleEntry:
        self._require_transaction()
        current = self.get(key)
        if current is None or current.deleted or current.version != expected_version:
            raise PriceAlertRuleVersionConflictError("active condition rule head changed")
        changed = self._connection.execute(
            "UPDATE condition_alert_rule SET version=?,deleted=1,rule_json=NU"
            "LL,updated_at_utc=? WHERE owner_id=? AND rule_id=? AND version=?"
            " AND deleted=0",
            (
                current.version + 1,
                normalize_aware_utc(now).isoformat(),
                key.owner_id,
                key.rule_id,
                current.version,
            ),
        )
        if changed.rowcount != 1:
            raise PriceAlertRuleVersionConflictError("condition rule head changed during delete")
        result = self.get(key)
        if result is None:
            raise PriceAlertRuleIntegrityError("condition tombstone has no actual head")
        return result
