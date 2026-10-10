"""Auxiliary configuration and NAV observation state, never a paper financial ledger."""

from __future__ import annotations

import os
import sqlite3
import stat
from contextlib import closing, contextmanager
from contextvars import ContextVar
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Iterator
from uuid import uuid4

from rquant.paper_portfolio_models import PaperPortfolioConfiguration, PaperRiskObservation, PaperPortfolioStateIdentity
from rquant.portfolio.drawdown import evaluate_drawdown
from rquant.research_run_spec import _parse_decimal
from rquant.runtime_contracts import normalize_aware_utc


class PaperPortfolioStateStore:
    @classmethod
    def open_existing(cls, path: Path, *, expected_identity: PaperPortfolioStateIdentity) -> PaperPortfolioStateStore:
        target = Path(path).absolute()
        observed = target.lstat()
        if (not stat.S_ISREG(observed.st_mode) or stat.S_IMODE(observed.st_mode) != 0o600 or observed.st_uid != os.getuid()
                or (str(target), observed.st_dev, observed.st_ino) != (expected_identity.path, expected_identity.st_dev, expected_identity.st_ino)):
            raise ValueError("original paper metadata identity was replaced")
        with closing(sqlite3.connect(f"{target.as_uri()}?mode=ro", uri=True, isolation_level=None)) as connection:
            connection.execute("PRAGMA query_only=ON")
            row = connection.execute("SELECT m.instance_id,c.digest,c.body FROM metadata m JOIN configurations c ON c.digest=m.current_config WHERE m.singleton=1").fetchone()
        if row is None or row[0] != expected_identity.instance_id:
            raise ValueError("original paper metadata instance was replaced")
        configuration = PaperPortfolioConfiguration.model_validate_json(row[2])
        if configuration.fingerprint != row[1]:
            raise ValueError("original paper configured head was replaced")
        result = cls(target, configuration=configuration)
        if result.identity() != expected_identity:
            raise ValueError("original paper metadata identity changed while reopening")
        return result

    def __init__(self, path: Path, *, configuration: PaperPortfolioConfiguration) -> None:
        self.path = Path(path).absolute()
        self._active_connection: ContextVar[sqlite3.Connection | None] = ContextVar("paper_portfolio_connection", default=None)
        self.configuration = PaperPortfolioConfiguration.model_validate(configuration.model_dump(mode="python"))
        self.path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        if self.path.is_symlink() or self.path.parent.is_symlink():
            raise ValueError("paper portfolio state cannot use links")
        if not self.path.exists():
            descriptor = os.open(self.path, os.O_CREAT | os.O_EXCL | os.O_WRONLY | os.O_NOFOLLOW, 0o600)
            os.close(descriptor)
        self._identity = self._file_identity()
        with self._connection(write=True) as connection:
            connection.execute("CREATE TABLE IF NOT EXISTS metadata(singleton INTEGER PRIMARY KEY CHECK(singleton=1),instance_id TEXT NOT NULL, current_config TEXT NOT NULL)")
            connection.execute("CREATE TABLE IF NOT EXISTS configurations(digest TEXT PRIMARY KEY,version INTEGER UNIQUE NOT NULL,body TEXT NOT NULL)")
            connection.execute("CREATE TABLE IF NOT EXISTS observations(config_digest TEXT NOT NULL,observed_at TEXT NOT NULL,body TEXT NOT NULL,PRIMARY KEY(config_digest,observed_at))")
            old = connection.execute("SELECT * FROM metadata WHERE singleton=1").fetchone()
            if old is None:
                self._insert_configuration(connection, self.configuration)
                connection.execute("INSERT INTO metadata VALUES(1,?,?)", (str(uuid4()), self.configuration.fingerprint))
            elif old["current_config"] != self.configuration.fingerprint:
                raise ValueError("paper portfolio configuration differs from persisted head")
            self.instance_id = connection.execute("SELECT instance_id FROM metadata WHERE singleton=1").fetchone()[0]

    def _file_identity(self) -> tuple[int, int]:
        value = self.path.lstat()
        if not stat.S_ISREG(value.st_mode) or value.st_uid != os.getuid() or value.st_mode & 0o077:
            raise ValueError("paper portfolio state must be a private regular owned file")
        return value.st_dev, value.st_ino

    def identity(self) -> PaperPortfolioStateIdentity:
        if self._file_identity() != self._identity:
            raise ValueError("original paper metadata source was replaced")
        with self._connection() as connection:
            if connection.execute("SELECT instance_id FROM metadata WHERE singleton=1").fetchone()[0] != self.instance_id:
                raise ValueError("original paper metadata instance changed")
        return PaperPortfolioStateIdentity(path=str(self.path), instance_id=self.instance_id,
                                           st_dev=self._identity[0], st_ino=self._identity[1])

    @contextmanager
    def _connection(self, *, write: bool = False) -> Iterator[sqlite3.Connection]:
        if self._file_identity() != self._identity:
            raise ValueError("paper portfolio state source was replaced")
        active = self._active_connection.get()
        if active is not None:
            yield active
            return
        connection = sqlite3.connect(f"{self.path.as_uri()}?mode=rw", uri=True, isolation_level=None)
        connection.row_factory = sqlite3.Row
        token = None
        try:
            if self._file_identity() != self._identity:
                raise ValueError("paper portfolio state source changed during open")
            if hasattr(self, "instance_id"):
                row = connection.execute("SELECT instance_id FROM metadata WHERE singleton=1").fetchone()
                if row is None or row[0] != self.instance_id:
                    raise ValueError("paper portfolio state instance was replaced")
            if write:
                connection.execute("BEGIN IMMEDIATE")
                token = self._active_connection.set(connection)
            yield connection
            if write:
                connection.commit()
        except BaseException:
            if connection.in_transaction:
                connection.rollback()
            raise
        finally:
            if token is not None:
                self._active_connection.reset(token)
            connection.close()

    @staticmethod
    def _insert_configuration(connection: sqlite3.Connection, configuration: PaperPortfolioConfiguration) -> None:
        connection.execute("INSERT INTO configurations VALUES(?,?,?)",
                           (configuration.fingerprint, configuration.version, configuration.model_dump_json()))

    def start_configuration(self, configuration: PaperPortfolioConfiguration) -> None:
        configuration = PaperPortfolioConfiguration.model_validate(configuration.model_dump(mode="python"))
        old = self.configuration
        if (configuration.version != old.version + 1 or configuration.binding != old.binding
                or configuration.execution_cost_spec != old.execution_cost_spec or configuration.configured_at < old.configured_at):
            raise ValueError("new paper configuration must continue its exact account and sequence")
        with self._connection(write=True) as connection:
            head = connection.execute("SELECT current_config FROM metadata WHERE singleton=1").fetchone()[0]
            if head != old.fingerprint:
                raise ValueError("paper configuration head changed")
            self._insert_configuration(connection, configuration)
            connection.execute("UPDATE metadata SET current_config=? WHERE singleton=1", (configuration.fingerprint,))
        self.configuration = configuration

    def refresh_configuration(self) -> PaperPortfolioConfiguration:
        with self._connection() as connection:
            row = connection.execute("SELECT c.digest,c.body FROM metadata m JOIN configurations c ON c.digest=m.current_config WHERE m.singleton=1").fetchone()
        if row is None:
            raise ValueError("paper configured head has no immutable definition")
        value = PaperPortfolioConfiguration.model_validate_json(row["body"])
        previous = self.configuration
        if (value.fingerprint != row["digest"] or value.version < previous.version or value.binding != previous.binding
                or value.execution_cost_spec != previous.execution_cost_spec
                or (value.version == previous.version and value != previous)):
            raise ValueError("paper configured head was replaced or rolled back")
        self.configuration = value
        return value

    def configuration_at(self, fingerprint: str, *, version: int) -> PaperPortfolioConfiguration:
        with self._connection() as connection:
            row = connection.execute("SELECT digest,version,body FROM configurations WHERE digest=?", (fingerprint,)).fetchone()
        if row is None:
            raise ValueError("original paper configuration is not registered")
        value = PaperPortfolioConfiguration.model_validate_json(row["body"])
        if (value.fingerprint, value.version, value.binding, value.execution_cost_spec) != (
                row["digest"], row["version"], self.configuration.binding, self.configuration.execution_cost_spec) or value.version != version:
            raise ValueError("original immutable paper configuration was replaced")
        return value

    def observe_nav(self, nav: Decimal, *, observed_at: datetime, ledger_revision: int,
                    source_fingerprint: str) -> PaperRiskObservation:
        nav = _parse_decimal(nav, field_name="paper NAV")
        if not 0 < nav <= Decimal("1000000000000"):
            raise ValueError("paper NAV exceeds the original money budget")
        observed = normalize_aware_utc(observed_at)
        key = observed.isoformat()
        with self._connection(write=True) as connection:
            digest = self.configuration.fingerprint
            old = connection.execute("SELECT body FROM observations WHERE config_digest=? AND observed_at=?", (digest, key)).fetchone()
            if old is not None:
                result = PaperRiskObservation.model_validate_json(old[0])
                if result.nav != nav or result.ledger_revision != ledger_revision or result.source_fingerprint != source_fingerprint:
                    raise ValueError("original paper NAV observation differs")
                return result
            last = connection.execute("SELECT body FROM observations WHERE config_digest=? ORDER BY observed_at DESC LIMIT 1", (digest,)).fetchone()
            previous = PaperRiskObservation.model_validate_json(last[0]) if last else None
            if previous is not None and observed <= previous.observed_at:
                raise ValueError("paper NAV observation is out of order")
            rule = self.configuration.drawdown_rule
            decision = evaluate_drawdown(nav, observed, rule, previous.decision.state if previous and previous.decision else None) if rule else None
            result = PaperRiskObservation(account_id=self.configuration.binding.account_id,
                                          configuration_fingerprint=digest, observed_at=observed, nav=nav,
                                          ledger_revision=ledger_revision, source_fingerprint=source_fingerprint, decision=decision)
            connection.execute("INSERT INTO observations VALUES(?,?,?)", (digest, key, result.model_dump_json()))
            return result

    def observations(self) -> tuple[PaperRiskObservation, ...]:
        with self._connection() as connection:
            rows = connection.execute("SELECT body FROM observations WHERE config_digest=? ORDER BY observed_at", (self.configuration.fingerprint,)).fetchall()
            return tuple(PaperRiskObservation.model_validate_json(row[0]) for row in rows)

    def last_observation(self) -> PaperRiskObservation | None:
        with self._connection() as connection:
            row = connection.execute("SELECT body FROM observations WHERE config_digest=? ORDER BY observed_at DESC LIMIT 1",
                                     (self.configuration.fingerprint,)).fetchone()
        return PaperRiskObservation.model_validate_json(row[0]) if row else None
