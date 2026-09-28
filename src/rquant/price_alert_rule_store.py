"""Transaction-owned price alert rules bound to manual-watchlist revisions."""

from __future__ import annotations

import sqlite3
from datetime import datetime
from typing import Annotated, Self

from pydantic import Field, StrictBool, StrictInt, StringConstraints, model_validator

from rquant.alert_price_rule import PriceAlertRule
from rquant.manual_watchlist import (
    ManualWatchlistKey,
    ManualWatchlistRepository,
    ManualWatchlistScan,
    OwnerId,
    TsCode,
)
from rquant.runtime_contracts import AwareUtcDatetime, RuntimeContractModel, normalize_aware_utc

MAX_ACTIVE_RULES = 100
RuleId = Annotated[str, StringConstraints(min_length=1, max_length=128)]
_COLUMNS = (
    "owner_id, rule_id, version, deleted, ts_code, membership_version, rule_json, updated_at_utc"
)
_SCHEMA_COLUMNS = (
    ("owner_id", "TEXT", 1, 1),
    ("rule_id", "TEXT", 1, 2),
    ("version", "INTEGER", 1, 0),
    ("deleted", "INTEGER", 1, 0),
    ("ts_code", "TEXT", 0, 0),
    ("membership_version", "INTEGER", 0, 0),
    ("rule_json", "TEXT", 0, 0),
    ("updated_at_utc", "TEXT", 1, 0),
)


class PriceAlertRuleTransactionError(RuntimeError):
    """Rule writes require the caller's existing SQLite transaction."""


class PriceAlertRuleUnavailableError(RuntimeError):
    """The rule schema or borrowed SQLite connection is unavailable."""


class PriceAlertRuleIntegrityError(ValueError):
    """Stored rule rows cannot be treated as trustworthy authority."""


class PriceAlertRuleVersionConflictError(ValueError):
    """The expected rule revision no longer names the current head."""


class PriceAlertRuleScopeError(ValueError):
    """The requested owner and stock are not an active, exact-version member."""


class PriceAlertRuleCapacityError(ValueError):
    """The owner already has 100 nondeleted rules."""


class PriceAlertRuleKey(RuntimeContractModel):
    owner_id: OwnerId
    rule_id: RuleId


class PriceAlertRuleUpsert(RuntimeContractModel):
    owner_id: OwnerId
    ts_code: TsCode
    membership_version: StrictInt = Field(ge=1)
    expected_version: StrictInt | None = Field(default=None, ge=1)
    rule: PriceAlertRule


class PriceAlertRuleDelete(PriceAlertRuleKey):
    expected_version: StrictInt = Field(ge=1)


class PriceAlertRuleEntry(PriceAlertRuleKey):
    version: StrictInt = Field(ge=1)
    deleted: StrictBool
    ts_code: TsCode | None
    membership_version: StrictInt | None = Field(ge=1)
    rule: PriceAlertRule | None
    updated_at: AwareUtcDatetime

    @model_validator(mode="after")
    def validate_state(self) -> Self:
        if self.deleted:
            if any(
                value is not None for value in (self.ts_code, self.membership_version, self.rule)
            ):
                raise ValueError("rule tombstone contains live facts")
        elif (
            self.ts_code is None
            or self.membership_version is None
            or self.rule is None
            or self.rule.rule_id != self.rule_id
        ):
            raise ValueError("live rule identity or scope binding is invalid")
        return self


class _OwnerQuery(RuntimeContractModel):
    owner_id: OwnerId


class PriceAlertRuleRepository:
    """Borrow a PageControl SQLite connection; the caller owns commit and rollback."""

    def __init__(self, connection: sqlite3.Connection) -> None:
        self._connection = connection

    def _require_transaction(self) -> None:
        if not self._connection.in_transaction:
            raise PriceAlertRuleTransactionError("price rule writes require caller transaction")

    def _require_schema(self) -> None:
        try:
            columns = self._connection.execute("PRAGMA table_info(price_alert_rule)").fetchall()
        except sqlite3.Error as exc:
            raise PriceAlertRuleUnavailableError("price rule schema cannot be read") from exc
        actual = tuple((row[1], row[2], row[3], row[5]) for row in columns)
        if actual != _SCHEMA_COLUMNS:
            raise PriceAlertRuleUnavailableError("price rule schema is missing or incompatible")

    def install_schema(self) -> None:
        self._require_transaction()
        try:
            self._connection.execute(
                """
                CREATE TABLE IF NOT EXISTS price_alert_rule (
                    owner_id TEXT NOT NULL,
                    rule_id TEXT NOT NULL,
                    version INTEGER NOT NULL CHECK (version >= 1),
                    deleted INTEGER NOT NULL CHECK (deleted IN (0, 1)),
                    ts_code TEXT,
                    membership_version INTEGER,
                    rule_json TEXT,
                    updated_at_utc TEXT NOT NULL,
                    PRIMARY KEY (owner_id, rule_id),
                    CHECK (
                        (deleted = 1 AND ts_code IS NULL AND membership_version IS NULL
                         AND rule_json IS NULL)
                        OR
                        (deleted = 0 AND ts_code IS NOT NULL
                         AND membership_version >= 1 AND rule_json IS NOT NULL)
                    )
                )
                """
            )
            self._connection.execute(
                "CREATE INDEX IF NOT EXISTS price_alert_rule_owner_live_idx "
                "ON price_alert_rule(owner_id, deleted, rule_id)"
            )
        except sqlite3.Error as exc:
            raise PriceAlertRuleUnavailableError("price rule schema cannot be installed") from exc
        self._require_schema()

    def _select(self, sql: str, parameters: tuple[object, ...]) -> list[sqlite3.Row | tuple]:
        self._require_schema()
        try:
            return self._connection.execute(sql, parameters).fetchall()
        except sqlite3.Error as exc:
            raise PriceAlertRuleUnavailableError("price rule rows cannot be read") from exc

    @staticmethod
    def _entry(row: sqlite3.Row | tuple) -> PriceAlertRuleEntry:
        try:
            owner_id, rule_id, version, deleted, ts_code, member_version, rule_json, at = row
            if type(version) is not int or type(deleted) is not int or deleted not in (0, 1):
                raise ValueError("price rule version or deletion flag is invalid")
            if not isinstance(at, str):
                raise ValueError("price rule update time is invalid")
            if deleted:
                if any(value is not None for value in (ts_code, member_version, rule_json)):
                    raise ValueError("price rule tombstone carries live facts")
                rule = None
            else:
                if not isinstance(rule_json, str) or type(member_version) is not int:
                    raise ValueError("price rule body or membership version is invalid")
                rule = PriceAlertRule.model_validate_json(rule_json)
            return PriceAlertRuleEntry(
                owner_id=owner_id,
                rule_id=rule_id,
                version=version,
                deleted=bool(deleted),
                ts_code=ts_code,
                membership_version=member_version,
                rule=rule,
                updated_at=datetime.fromisoformat(at),
            )
        except (TypeError, ValueError) as exc:
            raise PriceAlertRuleIntegrityError("stored price rule row is invalid") from exc

    def get(self, key: PriceAlertRuleKey) -> PriceAlertRuleEntry | None:
        rows = self._select(
            f"SELECT {_COLUMNS} FROM price_alert_rule WHERE owner_id = ? AND rule_id = ? LIMIT 2",
            (key.owner_id, key.rule_id),
        )
        if len(rows) > 1:
            raise PriceAlertRuleIntegrityError("price rule identity is not unique")
        return self._entry(rows[0]) if rows else None

    def list_current(self, owner_id: str) -> tuple[PriceAlertRuleEntry, ...]:
        owner = _OwnerQuery(owner_id=owner_id).owner_id
        self._require_schema()
        live: list[PriceAlertRuleEntry] = []
        seen_live: set[str] = set()
        try:
            rows = self._connection.execute(
                f"SELECT {_COLUMNS} FROM price_alert_rule WHERE owner_id = ? ORDER BY rule_id",
                (owner,),
            )
            for row in rows:
                entry = self._entry(row)
                if entry.owner_id != owner:
                    raise PriceAlertRuleIntegrityError("price rule owner is inconsistent")
                if not entry.deleted:
                    if entry.rule_id in seen_live:
                        raise PriceAlertRuleIntegrityError("price rule identity is not unique")
                    seen_live.add(entry.rule_id)
                    live.append(entry)
                    if len(live) > MAX_ACTIVE_RULES:
                        raise PriceAlertRuleIntegrityError("owner has too many live price rules")
        except sqlite3.Error as exc:
            raise PriceAlertRuleUnavailableError("price rule rows cannot be read") from exc
        return tuple(live)

    def list_effective(self, owner_id: str, *, now: datetime) -> tuple[PriceAlertRuleEntry, ...]:
        """Filter the SQLite head; later alert execution still needs Serving scope proof."""
        observed = normalize_aware_utc(now)
        active: list[PriceAlertRuleEntry] = []
        members = ManualWatchlistRepository(self._connection)
        try:
            members.scan(ManualWatchlistScan(owner_id=owner_id, now=observed, limit=1))
        except sqlite3.Error as exc:
            raise PriceAlertRuleUnavailableError("manual watchlist schema is unavailable") from exc
        except (TypeError, ValueError) as exc:
            raise PriceAlertRuleIntegrityError("manual watchlist row is invalid") from exc
        for entry in self.list_current(owner_id):
            if entry.rule is None or not entry.rule.enabled:
                continue
            assert entry.ts_code is not None and entry.membership_version is not None
            try:
                member = members.get(
                    ManualWatchlistKey(owner_id=entry.owner_id, ts_code=entry.ts_code),
                    now=observed,
                )
            except sqlite3.Error as exc:
                raise PriceAlertRuleUnavailableError(
                    "manual watchlist schema is unavailable"
                ) from exc
            except (TypeError, ValueError) as exc:
                raise PriceAlertRuleIntegrityError("manual watchlist row is invalid") from exc
            if (
                member is not None
                and member.status == "active"
                and member.version == entry.membership_version
            ):
                active.append(entry)
        return tuple(active)

    def _active_count(self, owner_id: str) -> int:
        rows = self._select(
            "SELECT COUNT(*) FROM price_alert_rule WHERE owner_id = ? AND deleted = 0",
            (owner_id,),
        )
        if len(rows) != 1 or type(rows[0][0]) is not int or rows[0][0] > MAX_ACTIVE_RULES:
            raise PriceAlertRuleIntegrityError("owner rule count is invalid")
        return rows[0][0]

    def _require_active_member(self, command: PriceAlertRuleUpsert, *, now: datetime) -> None:
        try:
            member = ManualWatchlistRepository(self._connection).get(
                ManualWatchlistKey(owner_id=command.owner_id, ts_code=command.ts_code),
                now=now,
            )
        except sqlite3.Error as exc:
            raise PriceAlertRuleUnavailableError("manual watchlist schema is unavailable") from exc
        except (TypeError, ValueError) as exc:
            raise PriceAlertRuleIntegrityError("manual watchlist row is invalid") from exc
        if (
            member is None
            or member.owner_id != command.owner_id
            or member.ts_code != command.ts_code
            or member.status != "active"
            or member.version != command.membership_version
        ):
            raise PriceAlertRuleScopeError("owner stock is not an exact active member")

    def upsert(self, command: PriceAlertRuleUpsert, *, now: datetime) -> PriceAlertRuleEntry:
        self._require_transaction()
        observed = normalize_aware_utc(now)
        key = PriceAlertRuleKey(owner_id=command.owner_id, rule_id=command.rule.rule_id)
        current = self.get(key)
        if current is None:
            if command.expected_version is not None:
                raise PriceAlertRuleVersionConflictError("price rule head does not exist")
            next_version = 1
        elif command.expected_version != current.version:
            raise PriceAlertRuleVersionConflictError("price rule version does not match")
        else:
            next_version = current.version + 1

        newly_live = current is None or current.deleted
        if (
            newly_live
            or command.rule.enabled
            or (
                current is not None
                and (current.ts_code, current.membership_version)
                != (command.ts_code, command.membership_version)
            )
        ):
            self._require_active_member(command, now=observed)
        if newly_live and self._active_count(command.owner_id) >= MAX_ACTIVE_RULES:
            raise PriceAlertRuleCapacityError("owner price rule limit reached")

        payload = command.rule.model_dump_json()
        changed_at = observed.isoformat(timespec="microseconds")
        if current is None:
            try:
                self._connection.execute(
                    "INSERT INTO price_alert_rule "
                    "(owner_id, rule_id, version, deleted, ts_code, membership_version, "
                    "rule_json, updated_at_utc) VALUES (?, ?, ?, 0, ?, ?, ?, ?)",
                    (
                        command.owner_id,
                        command.rule.rule_id,
                        next_version,
                        command.ts_code,
                        command.membership_version,
                        payload,
                        changed_at,
                    ),
                )
            except sqlite3.IntegrityError as exc:
                raise PriceAlertRuleVersionConflictError("price rule identity changed") from exc
        else:
            try:
                changed = self._connection.execute(
                    "UPDATE price_alert_rule SET version = ?, deleted = 0, ts_code = ?, "
                    "membership_version = ?, rule_json = ?, updated_at_utc = ? "
                    "WHERE owner_id = ? AND rule_id = ? AND version = ?",
                    (
                        next_version,
                        command.ts_code,
                        command.membership_version,
                        payload,
                        changed_at,
                        command.owner_id,
                        command.rule.rule_id,
                        current.version,
                    ),
                ).rowcount
            except sqlite3.IntegrityError as exc:
                raise PriceAlertRuleIntegrityError("price rule row rejected an update") from exc
            if changed != 1:
                raise PriceAlertRuleVersionConflictError("price rule head changed")
        written = self.get(key)
        assert written is not None
        return written

    def delete(self, command: PriceAlertRuleDelete, *, now: datetime) -> PriceAlertRuleEntry:
        self._require_transaction()
        observed = normalize_aware_utc(now)
        key = PriceAlertRuleKey(owner_id=command.owner_id, rule_id=command.rule_id)
        current = self.get(key)
        if current is None or current.deleted or command.expected_version != current.version:
            raise PriceAlertRuleVersionConflictError("active price rule version does not match")
        try:
            changed = self._connection.execute(
                "UPDATE price_alert_rule SET version = ?, deleted = 1, ts_code = NULL, "
                "membership_version = NULL, rule_json = NULL, updated_at_utc = ? "
                "WHERE owner_id = ? AND rule_id = ? AND version = ? AND deleted = 0",
                (
                    current.version + 1,
                    observed.isoformat(timespec="microseconds"),
                    command.owner_id,
                    command.rule_id,
                    current.version,
                ),
            ).rowcount
        except sqlite3.IntegrityError as exc:
            raise PriceAlertRuleIntegrityError("price rule row rejected deletion") from exc
        if changed != 1:
            raise PriceAlertRuleVersionConflictError("price rule head changed")
        written = self.get(key)
        assert written is not None
        return written
