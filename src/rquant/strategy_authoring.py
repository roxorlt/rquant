"""Owner/head facts reference original immutable definitions; they never grant execution."""

from __future__ import annotations

import os
import re
import sqlite3
import stat
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

from pydantic import Field, JsonValue

from rquant.definition_registry import ImmutableDefinitionRegistry, StrategySpecRegistration, TrustedExecutableRegistry
from rquant.runtime_contracts import AwareUtcDatetime, RuntimeContractModel
from rquant.strategy_authoring_commands import AcceptedStrategyTemplateArchive, AcceptedStrategyTemplateCommand, ArchiveStrategyTemplate, OwnedArchiveStrategyTemplate, OwnedSaveStrategyTemplate, OwnedStrategyTemplateCommand, SaveStrategyTemplate, StrategyAuthoringIdentity, StrategyTemplateCommand, StrategyTemplateHead, StrategyTemplateReceipt
from rquant.strategy_authoring_source import StrategySourceCatalog
from rquant.strategy_template import StrategyTemplate, TEMPLATE_FEATURE_CONTRACT, compile_strategy_template
from rquant.strategy_template_definition import strategy_template_feature_contract, template_executables


class StrategyAuthoringConflict(ValueError):
    """The original command or strategy head differs from the accepted request."""


class StrategyAuthoringIntegrityError(RuntimeError):
    """Supplementary metadata or its original definition reference is unsafe."""


class StrategyTemplateVersion(RuntimeContractModel):
    owner_id: str
    strategy_id: str
    head: StrategyTemplateHead
    parent_head: StrategyTemplateHead | None
    name: str
    change_note: str
    saved_at: AwareUtcDatetime
    rules: StrategyTemplate


class StrategyTemplateCurrent(StrategyTemplateVersion):
    archived: bool = Field(strict=True)


def _owner(value: str) -> str:
    if re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.:@-]{0,127}", value) is None:
        raise PermissionError("invalid authenticated strategy owner")
    return value


class StrategyAuthoringStore:
    def __init__(self, path: Path, *, definition_root: Path, producer_commit: str, clock: Callable[[], datetime] | None = None) -> None:
        self.path = Path(path)
        self.definition_root = Path(definition_root)
        if not self.path.is_absolute() or self.path.resolve() != self.path or not self.definition_root.is_absolute() or self.definition_root.resolve() != self.definition_root:
            raise ValueError("strategy authoring paths must be absolute and canonical")
        if re.fullmatch(r"[0-9a-f]{40}", producer_commit) is None:
            raise ValueError("invalid producer commit")
        self.producer_commit = producer_commit
        self.clock = clock or (lambda: datetime.now(UTC))

    def _now(self) -> datetime:
        now = self.clock()
        if now.tzinfo is None or now.utcoffset() is None:
            raise StrategyAuthoringIntegrityError("strategy server clock must be aware")
        return now.astimezone(UTC)

    def initialize(self) -> StrategyAuthoringIdentity:
        observed = self.path.parent.stat()
        if not stat.S_ISDIR(observed.st_mode) or observed.st_uid != os.getuid() or stat.S_IMODE(observed.st_mode) & 0o077:
            raise StrategyAuthoringIntegrityError("strategy metadata parent must be private")
        descriptor = os.open(self.path, os.O_CREAT | os.O_EXCL | os.O_WRONLY | getattr(os, "O_NOFOLLOW", 0), 0o600)
        os.close(descriptor)
        with self._connection(write=True, check_schema=False) as connection:
            connection.executescript("CREATE TABLE identity (singleton INTEGER PRIMARY KEY CHECK(singleton=1), instance_id TEXT NOT NULL); CREATE TABLE heads (strategy_id TEXT PRIMARY KEY, owner_id TEXT NOT NULL, head TEXT NOT NULL, archived INTEGER NOT NULL CHECK(archived IN (0,1))); CREATE TABLE versions (strategy_id TEXT NOT NULL, version INTEGER NOT NULL, owner_id TEXT NOT NULL, metadata TEXT NOT NULL, PRIMARY KEY(strategy_id,version)); CREATE TABLE command_refs (command_id TEXT PRIMARY KEY, owner_id TEXT NOT NULL, request_hash TEXT NOT NULL, strategy_id TEXT NOT NULL, frozen TEXT, receipt TEXT); CREATE TABLE run_admissions (command_id TEXT PRIMARY KEY, owner_id TEXT NOT NULL, request_hash TEXT NOT NULL, strategy_id TEXT NOT NULL, head TEXT NOT NULL, spec_hash TEXT NOT NULL);")
            connection.execute("INSERT INTO identity VALUES(1,?)", (uuid4().hex,))
        return self.identity()

    def _file_identity(self) -> tuple[int, int]:
        observed = self.path.lstat()
        if not stat.S_ISREG(observed.st_mode) or observed.st_uid != os.getuid() or observed.st_nlink != 1 or stat.S_IMODE(observed.st_mode) != 0o600:
            raise StrategyAuthoringIntegrityError("strategy metadata is not a private regular file")
        return observed.st_dev, observed.st_ino

    @contextmanager
    def _connection(self, *, write: bool = False, check_schema: bool = True, expected_identity: StrategyAuthoringIdentity | None = None) -> Iterator[sqlite3.Connection]:
        before = self._file_identity()
        if expected_identity is not None and (expected_identity.path != str(self.path) or before != (expected_identity.st_dev, expected_identity.st_ino)):
            raise StrategyAuthoringIntegrityError("strategy metadata identity changed")
        uri = self.path.as_uri() + ("?mode=rw" if write else "?mode=ro")
        connection = sqlite3.connect(uri, uri=True, isolation_level=None, timeout=10)
        connection.row_factory = sqlite3.Row
        try:
            if self._file_identity() != before:
                raise StrategyAuthoringIntegrityError("strategy metadata changed while opening")
            if check_schema:
                found = connection.execute("SELECT instance_id FROM identity WHERE singleton=1").fetchone()
                if found is None or re.fullmatch(r"[0-9a-f]{32}", found[0]) is None or (expected_identity is not None and found[0] != expected_identity.instance_id):
                    raise StrategyAuthoringIntegrityError("strategy metadata instance changed")
            connection.execute("BEGIN IMMEDIATE" if write else "BEGIN")
            yield connection
            if self._file_identity() != before:
                raise StrategyAuthoringIntegrityError("strategy metadata changed during operation")
            if connection.in_transaction:
                connection.execute("COMMIT")
        except BaseException:
            if connection.in_transaction:
                connection.execute("ROLLBACK")
            raise
        finally:
            connection.close()

    def identity(self) -> StrategyAuthoringIdentity:
        with self._connection() as connection:
            value = connection.execute("SELECT instance_id FROM identity WHERE singleton=1").fetchone()[0]
            device, inode = self._file_identity()
            return StrategyAuthoringIdentity(instance_id=value, path=str(self.path), st_dev=device, st_ino=inode)

    def definition_registry(self, strategy_id: str) -> ImmutableDefinitionRegistry:
        features, strategies = template_executables((strategy_id,))
        return ImmutableDefinitionRegistry(self.definition_root, execution_registry=TrustedExecutableRegistry(features=features, strategies=strategies))

    @staticmethod
    def _command_row(connection: sqlite3.Connection, request: StrategyTemplateCommand, owner_id: str) -> sqlite3.Row | None:
        row = connection.execute("SELECT * FROM command_refs WHERE command_id=?", (request.command_id,)).fetchone()
        if row is not None:
            if row["owner_id"] != owner_id:
                raise PermissionError("strategy command belongs to another owner")
            if row["request_hash"] != request.request_hash:
                raise StrategyAuthoringConflict("original strategy command body differs")
        return row

    def lookup_command(self, request: StrategyTemplateCommand, *, owner_id: str, expected_identity: StrategyAuthoringIdentity | None = None) -> StrategyTemplateReceipt | None:
        owner_id = _owner(owner_id)
        with self._connection(expected_identity=expected_identity) as connection:
            row = self._command_row(connection, request, owner_id)
            return None if row is None or row["receipt"] is None else StrategyTemplateReceipt.model_validate_json(row["receipt"])

    def accepted_command(self, request: SaveStrategyTemplate, *, owner_id: str) -> AcceptedStrategyTemplateCommand | None:
        with self._connection() as connection:
            row = self._command_row(connection, request, _owner(owner_id))
            return None if row is None or row["frozen"] is None else AcceptedStrategyTemplateCommand.model_validate_json(row["frozen"])

    @staticmethod
    def _head_row(connection: sqlite3.Connection, strategy_id: str, owner_id: str) -> sqlite3.Row:
        row = connection.execute("SELECT * FROM heads WHERE strategy_id=?", (strategy_id,)).fetchone()
        if row is None:
            raise KeyError("strategy template not found")
        if row["owner_id"] != owner_id:
            raise PermissionError("strategy template belongs to another owner")
        return row

    @staticmethod
    def _require_head(row: sqlite3.Row, head: StrategyTemplateHead) -> None:
        if StrategyTemplateHead.model_validate_json(row["head"]) != head:
            raise StrategyAuthoringConflict("strategy head changed")

    @staticmethod
    def _require_idle(connection: sqlite3.Connection, strategy_id: str, *, exclude_command_id: str = "") -> None:
        if connection.execute("SELECT 1 FROM command_refs WHERE strategy_id=? AND receipt IS NULL AND command_id!=? LIMIT 1", (strategy_id, exclude_command_id)).fetchone() is not None:
            raise StrategyAuthoringConflict("strategy has an accepted command awaiting recovery")

    def accept(self, request: SaveStrategyTemplate, *, owner_id: str, catalog: StrategySourceCatalog, expected_identity: StrategyAuthoringIdentity | None = None) -> AcceptedStrategyTemplateCommand:
        request = SaveStrategyTemplate.model_validate(request.model_dump(mode="python"))
        owner_id = _owner(owner_id)
        with self._connection(write=True, expected_identity=expected_identity) as connection:
            old = self._command_row(connection, request, owner_id)
            if old is not None:
                return AcceptedStrategyTemplateCommand.model_validate_json(old["frozen"])
            catalog.validate_rules(request.rules, owner_id=owner_id, generation_id=request.generation_id)
            strategy_id = request.strategy_id or "template_" + uuid4().hex
            if request.strategy_id is not None:
                row = self._head_row(connection, strategy_id, owner_id)
                if row["archived"]:
                    raise StrategyAuthoringConflict("strategy is archived")
                assert request.expected_head is not None
                self._require_head(row, request.expected_head)
                self._require_idle(connection, strategy_id)
            version = 1 if request.expected_head is None else request.expected_head.version + 1
            if version > 4096:
                raise StrategyAuthoringConflict("strategy version budget reached")
            frozen = AcceptedStrategyTemplateCommand(owner_id=owner_id, strategy_id=strategy_id, original_request_hash=request.request_hash, request=request, version=version, producer_commit=self.producer_commit, accepted_at=self._now(), metadata_identity=self.identity())
            connection.execute("INSERT INTO command_refs VALUES(?,?,?,?,?,NULL)", (request.command_id, owner_id, request.request_hash, strategy_id, frozen.model_dump_json()))
            return frozen

    def save(self, request: SaveStrategyTemplate, *, owner_id: str, catalog: StrategySourceCatalog, expected_identity: StrategyAuthoringIdentity | None = None) -> StrategyTemplateReceipt:
        existing = self.lookup_command(request, owner_id=owner_id, expected_identity=expected_identity)
        if existing is not None:
            return existing
        accepted = self.accept(request, owner_id=owner_id, catalog=catalog, expected_identity=expected_identity)
        return self.complete_save(accepted, expected_identity=expected_identity)

    def complete_save(self, accepted: AcceptedStrategyTemplateCommand, *, expected_identity: StrategyAuthoringIdentity | None = None) -> StrategyTemplateReceipt:
        accepted = AcceptedStrategyTemplateCommand.model_validate(accepted.model_dump(mode="python"))
        if expected_identity is not None and expected_identity != accepted.metadata_identity:
            raise StrategyAuthoringIntegrityError("save original metadata identity differs")
        request = accepted.request
        with self._connection(write=True, expected_identity=accepted.metadata_identity) as connection:
            row = self._command_row(connection, request, accepted.owner_id)
            if row is None or row["frozen"] != accepted.model_dump_json():
                raise StrategyAuthoringIntegrityError("save is not the originally accepted command")
            if row["receipt"] is not None:
                return StrategyTemplateReceipt.model_validate_json(row["receipt"])
            if request.expected_head is not None:
                current = self._head_row(connection, accepted.strategy_id, accepted.owner_id)
                self._require_head(current, request.expected_head)
                if current["archived"]:
                    raise StrategyAuthoringConflict("strategy is archived")
            registry = self.definition_registry(accepted.strategy_id)
            registered_at = self._now()
            if registered_at < accepted.accepted_at:
                raise StrategyAuthoringIntegrityError("strategy server clock precedes original admission")
            contract = strategy_template_feature_contract(producer_commit=accepted.producer_commit)
            feature = registry.register_feature_contract(contract, registered_at=registered_at, available_at=registered_at, producer_commit=accepted.producer_commit, expected_fingerprint=contract.contract_fingerprint)
            spec = compile_strategy_template(request.rules, strategy_id=accepted.strategy_id, version=accepted.version, producer_commit=accepted.producer_commit)
            registration = registry.register_strategy_spec(spec, feature_contract_fingerprint=feature.fingerprint, registered_at=registered_at, available_at=registered_at, producer_commit=accepted.producer_commit, expected_fingerprint=spec.spec_fingerprint, parent_fingerprint=None if request.expected_head is None else request.expected_head.registration_fingerprint, supersedes=None if request.expected_head is None else request.expected_head.version, replacement_reason=None if request.expected_head is None else request.change_note)
            return self._commit_saved(connection, accepted, registration)

    def _commit_saved(self, connection: sqlite3.Connection, accepted: AcceptedStrategyTemplateCommand, registration: StrategySpecRegistration) -> StrategyTemplateReceipt:
        request = accepted.request
        head = StrategyTemplateHead(version=registration.version, registration_fingerprint=registration.fingerprint, record_hash=registration.record_hash, spec_fingerprint=registration.spec.spec_fingerprint)
        if registration.logical_id != accepted.strategy_id or registration.version != accepted.version or registration.spec.parameters["rules"] != compile_strategy_template(request.rules, strategy_id=accepted.strategy_id, version=accepted.version, producer_commit=accepted.producer_commit).parameters["rules"]:
            raise StrategyAuthoringIntegrityError("original registration differs from accepted body")
        metadata = StrategyTemplateVersion(owner_id=accepted.owner_id, strategy_id=accepted.strategy_id, head=head, parent_head=request.expected_head, name=request.name, change_note=request.change_note, saved_at=registration.registered_at, rules=request.rules)
        connection.execute("INSERT INTO versions VALUES(?,?,?,?)", (accepted.strategy_id, accepted.version, accepted.owner_id, metadata.model_dump_json()))
        connection.execute("INSERT INTO heads VALUES(?,?,?,0) ON CONFLICT(strategy_id) DO UPDATE SET head=excluded.head", (accepted.strategy_id, accepted.owner_id, head.model_dump_json()))
        completed_at = self._now()
        if completed_at < registration.registered_at:
            raise StrategyAuthoringIntegrityError("strategy server completion clock precedes publication")
        receipt = StrategyTemplateReceipt(owner_id=accepted.owner_id, command_id=request.command_id, action="save", strategy_id=accepted.strategy_id, head=head, original_request_hash=request.request_hash, completed_at=completed_at)
        connection.execute("UPDATE command_refs SET receipt=? WHERE command_id=?", (receipt.model_dump_json(), request.command_id))
        return receipt

    def accept_archive(self, request: ArchiveStrategyTemplate, *, owner_id: str, catalog: StrategySourceCatalog, expected_identity: StrategyAuthoringIdentity) -> AcceptedStrategyTemplateArchive:
        request = ArchiveStrategyTemplate.model_validate(request.model_dump(mode="python"))
        owner_id = _owner(owner_id)
        with self._connection(write=True, expected_identity=expected_identity) as connection:
            old = self._command_row(connection, request, owner_id)
            if old is not None:
                return AcceptedStrategyTemplateArchive.model_validate_json(old["frozen"])
            if catalog.owner_id != owner_id:
                raise PermissionError("strategy source catalog owner differs")
            if catalog.generation_id != request.generation_id:
                raise StrategyAuthoringConflict("strategy source catalog generation differs")
            row = self._head_row(connection, request.strategy_id, owner_id)
            self._require_head(row, request.expected_head)
            self._require_idle(connection, request.strategy_id)
            if row["archived"]:
                raise StrategyAuthoringConflict("strategy is archived")
            self._version(connection, request.strategy_id, request.expected_head.version, owner_id)
            accepted = AcceptedStrategyTemplateArchive(owner_id=owner_id, request=request, original_request_hash=request.request_hash, accepted_at=self._now(), metadata_identity=self.identity())
            connection.execute("INSERT INTO command_refs VALUES(?,?,?,?,?,NULL)", (request.command_id, owner_id, request.request_hash, request.strategy_id, accepted.model_dump_json()))
            return accepted

    def archive(self, request: ArchiveStrategyTemplate, *, owner_id: str, expected_identity: StrategyAuthoringIdentity | None = None) -> StrategyTemplateReceipt:
        request = ArchiveStrategyTemplate.model_validate(request.model_dump(mode="python"))
        owner_id = _owner(owner_id)
        with self._connection(write=True, expected_identity=expected_identity) as connection:
            old = self._command_row(connection, request, owner_id)
            if old is not None and old["receipt"] is not None:
                return StrategyTemplateReceipt.model_validate_json(old["receipt"])
            row = self._head_row(connection, request.strategy_id, owner_id)
            self._require_head(row, request.expected_head)
            self._require_idle(connection, request.strategy_id, exclude_command_id=request.command_id)
            if row["archived"]:
                raise StrategyAuthoringConflict("strategy is archived")
            self._version(connection, request.strategy_id, request.expected_head.version, owner_id)
            receipt = StrategyTemplateReceipt(owner_id=owner_id, command_id=request.command_id, action="archive", strategy_id=request.strategy_id, head=request.expected_head, original_request_hash=request.request_hash, completed_at=self._now())
            connection.execute("UPDATE heads SET archived=1 WHERE strategy_id=?", (request.strategy_id,))
            if old is None:
                connection.execute("INSERT INTO command_refs VALUES(?,?,?,?,NULL,?)", (request.command_id, owner_id, request.request_hash, request.strategy_id, receipt.model_dump_json()))
            else:
                if AcceptedStrategyTemplateArchive.model_validate_json(old["frozen"]).request != request:
                    raise StrategyAuthoringIntegrityError("archive differs from original admission")
                connection.execute("UPDATE command_refs SET receipt=? WHERE command_id=?", (receipt.model_dump_json(), request.command_id))
            return receipt

    def _version(self, connection: sqlite3.Connection, strategy_id: str, version: int, owner_id: str) -> StrategyTemplateVersion:
        self._head_row(connection, strategy_id, owner_id)
        row = connection.execute("SELECT metadata FROM versions WHERE strategy_id=? AND version=? AND owner_id=?", (strategy_id, version, owner_id)).fetchone()
        if row is None:
            raise KeyError("strategy template version not found")
        metadata = StrategyTemplateVersion.model_validate_json(row[0])
        record = self.definition_registry(strategy_id).read_strategy_spec(metadata.head.registration_fingerprint)
        if record is None or (record.logical_id, record.version, record.record_hash, record.spec.spec_fingerprint) != (strategy_id, version, metadata.head.record_hash, metadata.head.spec_fingerprint):
            raise StrategyAuthoringIntegrityError("strategy version lost its exact original definition")
        expected_spec = compile_strategy_template(metadata.rules, strategy_id=strategy_id, version=version, producer_commit=record.producer_commit)
        if expected_spec.spec_fingerprint != record.spec.spec_fingerprint or metadata.owner_id != owner_id or metadata.saved_at != record.registered_at:
            raise StrategyAuthoringIntegrityError("strategy supplementary facts differ from original body")
        return metadata

    def get_version(self, strategy_id: str, version: int, *, owner_id: str) -> StrategyTemplateVersion:
        with self._connection() as connection:
            return self._version(connection, strategy_id, version, _owner(owner_id))

    def versions(self, strategy_id: str, *, owner_id: str) -> tuple[StrategyTemplateVersion, ...]:
        with self._connection() as connection:
            self._head_row(connection, strategy_id, _owner(owner_id))
            rows = connection.execute("SELECT version FROM versions WHERE strategy_id=? ORDER BY version DESC LIMIT 4097", (strategy_id,)).fetchall()
            if len(rows) > 4096:
                raise StrategyAuthoringIntegrityError("strategy versions exceed read budget")
            return tuple(self._version(connection, strategy_id, row[0], owner_id) for row in rows)

    def get_current(self, strategy_id: str, *, owner_id: str) -> StrategyTemplateCurrent:
        with self._connection() as connection:
            row = self._head_row(connection, strategy_id, _owner(owner_id))
            head = StrategyTemplateHead.model_validate_json(row["head"])
            version = self._version(connection, strategy_id, head.version, owner_id)
            if version.head != head:
                raise StrategyAuthoringIntegrityError("strategy current head differs from saved version")
            return StrategyTemplateCurrent(**version.model_dump(mode="python"), archived=bool(row["archived"]))

    def list_current(self, *, owner_id: str) -> tuple[StrategyTemplateCurrent, ...]:
        owner_id = _owner(owner_id)
        with self._connection() as connection:
            rows = connection.execute("SELECT strategy_id FROM heads WHERE owner_id=? ORDER BY strategy_id LIMIT 501", (owner_id,)).fetchall()
            if len(rows) > 500:
                raise StrategyAuthoringIntegrityError("strategy count exceeds read budget")
            return tuple(self.get_current(row[0], owner_id=owner_id) for row in rows)

    def admit_run(self, strategy_id: str, head: StrategyTemplateHead, *, owner_id: str, command_id: str, request_hash: str, spec_hash: str | None = None) -> StrategyTemplateVersion:
        owner_id = _owner(owner_id)
        exact_spec_hash = request_hash if spec_hash is None else spec_hash
        if re.fullmatch(r"[0-9a-f]{64}", exact_spec_hash) is None:
            raise ValueError("strategy run requires its exact original plan hash")
        with self._connection(write=True) as connection:
            old = connection.execute("SELECT * FROM run_admissions WHERE command_id=?", (command_id,)).fetchone()
            if old is not None:
                if old["owner_id"] != owner_id:
                    raise PermissionError("strategy run belongs to another owner")
                if (old["strategy_id"], old["request_hash"], old["spec_hash"], StrategyTemplateHead.model_validate_json(old["head"])) != (strategy_id, request_hash, exact_spec_hash, head):
                    raise StrategyAuthoringConflict("original strategy run differs")
                return self._version(connection, strategy_id, head.version, owner_id)
            row = self._head_row(connection, strategy_id, owner_id)
            self._require_head(row, head)
            self._require_idle(connection, strategy_id)
            if row["archived"]:
                raise StrategyAuthoringConflict("strategy is archived")
            version = self._version(connection, strategy_id, head.version, owner_id)
            connection.execute("INSERT INTO run_admissions VALUES(?,?,?,?,?,?)", (command_id, owner_id, request_hash, strategy_id, head.model_dump_json(), exact_spec_hash))
        return version


class StrategyAuthoringPageControlBackend:
    def __init__(self, store: StrategyAuthoringStore, *, editor_users: tuple[str, ...], enabled: bool = False) -> None:
        self.store = store
        self.editor_users = editor_users
        self.enabled = enabled

    def authorize(self, actor_id: str) -> None:
        if not self.enabled or _owner(actor_id) not in self.editor_users:
            raise PermissionError("strategy authoring is not enabled for this owner")

    def identity(self) -> StrategyAuthoringIdentity:
        return self.store.identity()

    def compile(self, request: StrategyTemplateCommand, *, authenticated_actor_id: str, catalog: StrategySourceCatalog, expected_identity: StrategyAuthoringIdentity) -> OwnedStrategyTemplateCommand:
        self.authorize(authenticated_actor_id)
        if type(request) is SaveStrategyTemplate:
            accepted = self.store.accept(request, owner_id=authenticated_actor_id, catalog=catalog, expected_identity=expected_identity)
            return OwnedSaveStrategyTemplate(**request.model_dump(mode="python"), owner_id=authenticated_actor_id, metadata_identity=expected_identity, accepted=accepted)
        if type(request) is not ArchiveStrategyTemplate:
            raise TypeError("strategy admission requires an ownerless request")
        accepted = self.store.accept_archive(request, owner_id=authenticated_actor_id, catalog=catalog, expected_identity=expected_identity)
        return OwnedArchiveStrategyTemplate(**request.model_dump(mode="python"), owner_id=authenticated_actor_id, metadata_identity=expected_identity, accepted=accepted)

    def validate(self, command: OwnedStrategyTemplateCommand) -> None:
        self.authorize(command.owner_id)
        if self.identity() != command.metadata_identity:
            raise StrategyAuthoringIntegrityError("strategy metadata changed after admission")

    def submit(self, command: OwnedStrategyTemplateCommand) -> JsonValue:
        if isinstance(command, OwnedSaveStrategyTemplate):
            self.authorize(command.owner_id)
            return self.store.complete_save(command.accepted, expected_identity=command.metadata_identity).model_dump(mode="json")
        if isinstance(command, OwnedArchiveStrategyTemplate):
            self.authorize(command.owner_id)
            return self.store.archive(command.original(), owner_id=command.owner_id, expected_identity=command.metadata_identity).model_dump(mode="json")
        raise TypeError("strategy authoring backend requires an owned command")

    def recover(self, command: OwnedStrategyTemplateCommand) -> JsonValue | None:
        if not isinstance(command, (OwnedSaveStrategyTemplate, OwnedArchiveStrategyTemplate)):
            raise TypeError("strategy authoring recovery requires an owned command")
        self.authorize(command.owner_id)
        receipt = self.store.lookup_command(command.original(), owner_id=command.owner_id, expected_identity=command.metadata_identity)
        if receipt is not None:
            return receipt.model_dump(mode="json")
        # The journal carries the already admitted original request. Completing it
        # reuses its exact immutable registration after registry/metadata interruption.
        return self.submit(command)
