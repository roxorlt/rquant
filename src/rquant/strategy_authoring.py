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
from typing import TYPE_CHECKING
from uuid import UUID, uuid4

from pydantic import Field, JsonValue

from rquant.definition_registry import (
    ImmutableDefinitionRegistry,
    StrategySpecRegistration,
    TrustedExecutableRegistry,
)
from rquant.runtime_contracts import AwareUtcDatetime, RuntimeContractModel
from rquant.strategy_authoring_commands import (
    AcceptedStrategyTemplateArchive,
    AcceptedStrategyTemplateCommand,
    ArchiveStrategyTemplate,
    OwnedArchiveStrategyTemplate,
    OwnedSaveStrategyTemplate,
    SaveStrategyTemplate,
    StrategyAuthoringIdentity,
    StrategyTemplateHead,
    StrategyTemplateReceipt,
)
from rquant.strategy_authoring_projection_contract import (
    MAX_TEMPLATE_COUNT,
    MAX_TEMPLATE_PROJECTION_BYTES,
    MAX_TEMPLATE_RUN_ADMISSIONS,
    MAX_TEMPLATE_VERSIONS,
)
from rquant.strategy_authoring_source import StrategySourceCatalog
from rquant.strategy_template import (
    StrategyTemplate,
    compile_strategy_template,
)
from rquant.strategy_template_definition import (
    strategy_template_feature_contract,
    template_executables,
)
from rquant.strategy_template_run_commands import (
    AcceptedStrategyTemplateRun,
    OwnedRunStrategyTemplate,
    RunStrategyTemplate,
    StrategyTemplateRunReceipt,
)
from rquant.strategy_template_run_commands import (
    OwnedStrategyTemplateCommandValue as OwnedStrategyTemplateCommand,
)
from rquant.strategy_template_run_commands import (
    StrategyTemplateCommandValue as StrategyTemplateCommand,
)

if TYPE_CHECKING:
    from rquant.strategy_promotion_contracts import (
        PreparedPromotionApproval, StrategyPromotionApproval,
        StrategyPromotionReview, StrategyPromotionState, StrategyPromotionTarget,
    )
    from rquant.strategy_promotion_commands import StrategyPromotionCommand
    from rquant.strategy_template_submission import StrategyTemplateRunBackend


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
    def __init__(
        self,
        path: Path,
        *,
        definition_root: Path,
        producer_commit: str,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self.path = Path(path)
        self.definition_root = Path(definition_root)
        if (
            not self.path.is_absolute()
            or self.path.resolve() != self.path
            or not self.definition_root.is_absolute()
            or self.definition_root.resolve() != self.definition_root
        ):
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
        if (
            not stat.S_ISDIR(observed.st_mode)
            or observed.st_uid != os.getuid()
            or stat.S_IMODE(observed.st_mode) & 0o077
        ):
            raise StrategyAuthoringIntegrityError("strategy metadata parent must be private")
        descriptor = os.open(
            self.path, os.O_CREAT | os.O_EXCL | os.O_WRONLY | getattr(os, "O_NOFOLLOW", 0), 0o600
        )
        os.close(descriptor)
        with self._connection(write=True, check_schema=False) as connection:
            connection.executescript(
                "CREATE TABLE identity (singleton INTEGER PRIMARY KEY CHECK(singleton=1), instance_id TEXT NOT NULL); CREATE TABLE heads (strategy_id TEXT PRIMARY KEY, owner_id TEXT NOT NULL, head TEXT NOT NULL, archived INTEGER NOT NULL CHECK(archived IN (0,1))); CREATE TABLE versions (strategy_id TEXT NOT NULL, version INTEGER NOT NULL, owner_id TEXT NOT NULL, metadata TEXT NOT NULL, PRIMARY KEY(strategy_id,version)); CREATE TABLE command_refs (command_id TEXT PRIMARY KEY, owner_id TEXT NOT NULL, request_hash TEXT NOT NULL, strategy_id TEXT NOT NULL, frozen TEXT, receipt TEXT); CREATE TABLE run_admissions (command_id TEXT PRIMARY KEY, owner_id TEXT NOT NULL, request_hash TEXT NOT NULL, strategy_id TEXT NOT NULL, head TEXT NOT NULL, spec_hash TEXT NOT NULL, frozen TEXT, receipt TEXT);"
            )
            connection.execute("INSERT INTO identity VALUES(1,?)", (uuid4().hex,))
        return self.identity()

    def _file_identity(self) -> tuple[int, int]:
        observed = self.path.lstat()
        if (
            not stat.S_ISREG(observed.st_mode)
            or observed.st_uid != os.getuid()
            or observed.st_nlink != 1
            or stat.S_IMODE(observed.st_mode) != 0o600
        ):
            raise StrategyAuthoringIntegrityError("strategy metadata is not a private regular file")
        return observed.st_dev, observed.st_ino

    @contextmanager
    def _connection(
        self,
        *,
        write: bool = False,
        check_schema: bool = True,
        expected_identity: StrategyAuthoringIdentity | None = None,
    ) -> Iterator[sqlite3.Connection]:
        before = self._file_identity()
        if expected_identity is not None and (
            expected_identity.path != str(self.path)
            or before != (expected_identity.st_dev, expected_identity.st_ino)
        ):
            raise StrategyAuthoringIntegrityError("strategy metadata identity changed")
        uri = self.path.as_uri() + ("?mode=rw" if write else "?mode=ro")
        connection = sqlite3.connect(uri, uri=True, isolation_level=None, timeout=10)
        connection.row_factory = sqlite3.Row
        try:
            if self._file_identity() != before:
                raise StrategyAuthoringIntegrityError("strategy metadata changed while opening")
            if check_schema:
                found = connection.execute(
                    "SELECT instance_id FROM identity WHERE singleton=1"
                ).fetchone()
                if (
                    found is None
                    or re.fullmatch(r"[0-9a-f]{32}", found[0]) is None
                    or (expected_identity is not None and found[0] != expected_identity.instance_id)
                ):
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
            value = connection.execute(
                "SELECT instance_id FROM identity WHERE singleton=1"
            ).fetchone()[0]
            device, inode = self._file_identity()
            return StrategyAuthoringIdentity(
                instance_id=value, path=str(self.path), st_dev=device, st_ino=inode
            )

    def definition_registry(self, strategy_id: str) -> ImmutableDefinitionRegistry:
        features, strategies = template_executables((strategy_id,))
        return ImmutableDefinitionRegistry(
            self.definition_root,
            execution_registry=TrustedExecutableRegistry(features=features, strategies=strategies),
        )

    @staticmethod
    def _command_row(
        connection: sqlite3.Connection, request: StrategyTemplateCommand, owner_id: str
    ) -> sqlite3.Row | None:
        table = "run_admissions" if type(request) is RunStrategyTemplate else "command_refs"
        other = "command_refs" if table == "run_admissions" else "run_admissions"
        if (
            connection.execute(
                f"SELECT 1 FROM {other} WHERE command_id=?", (request.command_id,)
            ).fetchone()
            is not None
        ):
            raise StrategyAuthoringConflict("original strategy command kind differs")
        row = connection.execute(
            f"SELECT * FROM {table} WHERE command_id=?", (request.command_id,)
        ).fetchone()
        if row is not None:
            if row["owner_id"] != owner_id:
                raise PermissionError("strategy command belongs to another owner")
            if row["request_hash"] != request.request_hash:
                raise StrategyAuthoringConflict("original strategy command body differs")
        return row

    def lookup_command(
        self,
        request: StrategyTemplateCommand,
        *,
        owner_id: str,
        expected_identity: StrategyAuthoringIdentity | None = None,
    ) -> StrategyTemplateReceipt | StrategyTemplateRunReceipt | None:
        owner_id = _owner(owner_id)
        with self._connection(expected_identity=expected_identity) as connection:
            row = self._command_row(connection, request, owner_id)
            model = (
                StrategyTemplateRunReceipt
                if type(request) is RunStrategyTemplate
                else StrategyTemplateReceipt
            )
            return (
                None
                if row is None or row["receipt"] is None
                else model.model_validate_json(row["receipt"])
            )

    def accepted_command(
        self, request: SaveStrategyTemplate, *, owner_id: str
    ) -> AcceptedStrategyTemplateCommand | None:
        with self._connection() as connection:
            row = self._command_row(connection, request, _owner(owner_id))
            return (
                None
                if row is None or row["frozen"] is None
                else AcceptedStrategyTemplateCommand.model_validate_json(row["frozen"])
            )

    @staticmethod
    def _head_row(connection: sqlite3.Connection, strategy_id: str, owner_id: str) -> sqlite3.Row:
        row = connection.execute(
            "SELECT * FROM heads WHERE strategy_id=?", (strategy_id,)
        ).fetchone()
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
    def _require_idle(
        connection: sqlite3.Connection, strategy_id: str, *, exclude_command_id: str = ""
    ) -> None:
        if (
            connection.execute(
                "SELECT 1 FROM command_refs WHERE strategy_id=? AND receipt IS NULL AND command_id!=? LIMIT 1",
                (strategy_id, exclude_command_id),
            ).fetchone()
            is not None
        ):
            raise StrategyAuthoringConflict("strategy has an accepted command awaiting recovery")

    def accept(
        self,
        request: SaveStrategyTemplate,
        *,
        owner_id: str,
        catalog: StrategySourceCatalog,
        expected_identity: StrategyAuthoringIdentity | None = None,
    ) -> AcceptedStrategyTemplateCommand:
        request = SaveStrategyTemplate.model_validate(request.model_dump(mode="python"))
        owner_id = _owner(owner_id)
        with self._connection(write=True, expected_identity=expected_identity) as connection:
            old = self._command_row(connection, request, owner_id)
            if old is not None:
                return AcceptedStrategyTemplateCommand.model_validate_json(old["frozen"])
            catalog.validate_rules(
                request.rules, owner_id=owner_id, generation_id=request.generation_id
            )
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
            frozen = AcceptedStrategyTemplateCommand(
                owner_id=owner_id,
                strategy_id=strategy_id,
                original_request_hash=request.request_hash,
                request=request,
                version=version,
                producer_commit=self.producer_commit,
                accepted_at=self._now(),
                metadata_identity=self.identity(),
            )
            self._require_save_capacity(connection, frozen)
            connection.execute(
                "INSERT INTO command_refs VALUES(?,?,?,?,?,NULL)",
                (
                    request.command_id,
                    owner_id,
                    request.request_hash,
                    strategy_id,
                    frozen.model_dump_json(),
                ),
            )
            return frozen

    @staticmethod
    def _require_save_capacity(
        connection: sqlite3.Connection, accepted: AcceptedStrategyTemplateCommand
    ) -> None:
        pending = (
            "receipt IS NULL AND json_extract(frozen,'$.request.kind')='save_strategy_template'"
        )
        logical_count = connection.execute(
            f"SELECT COUNT(*) FROM (SELECT strategy_id FROM heads UNION SELECT strategy_id FROM command_refs WHERE {pending})"
        ).fetchone()[0]
        exists = connection.execute(
            f"SELECT 1 FROM (SELECT strategy_id FROM heads UNION SELECT strategy_id FROM command_refs WHERE {pending}) WHERE strategy_id=?",
            (accepted.strategy_id,),
        ).fetchone()
        committed_count, committed_bytes = connection.execute(
            "SELECT COUNT(*),COALESCE(SUM(length(CAST(metadata AS BLOB))),0) FROM versions"
        ).fetchone()
        pending_count, pending_bytes = connection.execute(
            f"SELECT COUNT(*),COALESCE(SUM(length(CAST(frozen AS BLOB))),0) FROM command_refs WHERE {pending}"
        ).fetchone()
        if (
            logical_count + (exists is None) > MAX_TEMPLATE_COUNT
            or committed_count + pending_count + 1 > MAX_TEMPLATE_VERSIONS
        ):
            raise StrategyAuthoringConflict("strategy global publication count budget reached")
        # Reserve all 16 source rows, the state row and one exact recent-result
        # reference per version. Admission precedes the original registry effect.
        source_and_state_bytes = 1024 * 1024 + 4096
        projected_bytes = (
            source_and_state_bytes
            + committed_bytes
            + committed_count * 1600
            + pending_bytes
            + pending_count * 2048
            + len(accepted.model_dump_json().encode())
            + 2048
        )
        if projected_bytes > MAX_TEMPLATE_PROJECTION_BYTES:
            raise StrategyAuthoringConflict("strategy publication byte budget reached")

    def save(
        self,
        request: SaveStrategyTemplate,
        *,
        owner_id: str,
        catalog: StrategySourceCatalog,
        expected_identity: StrategyAuthoringIdentity | None = None,
    ) -> StrategyTemplateReceipt:
        existing = self.lookup_command(
            request, owner_id=owner_id, expected_identity=expected_identity
        )
        if existing is not None:
            return existing
        accepted = self.accept(
            request, owner_id=owner_id, catalog=catalog, expected_identity=expected_identity
        )
        return self.complete_save(accepted, expected_identity=expected_identity)

    def complete_save(
        self,
        accepted: AcceptedStrategyTemplateCommand,
        *,
        expected_identity: StrategyAuthoringIdentity | None = None,
    ) -> StrategyTemplateReceipt:
        accepted = AcceptedStrategyTemplateCommand.model_validate(
            accepted.model_dump(mode="python")
        )
        if expected_identity is not None and expected_identity != accepted.metadata_identity:
            raise StrategyAuthoringIntegrityError("save original metadata identity differs")
        request = accepted.request
        with self._connection(
            write=True, expected_identity=accepted.metadata_identity
        ) as connection:
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
                raise StrategyAuthoringIntegrityError(
                    "strategy server clock precedes original admission"
                )
            contract = strategy_template_feature_contract(producer_commit=accepted.producer_commit)
            feature = registry.register_feature_contract(
                contract,
                registered_at=registered_at,
                available_at=registered_at,
                producer_commit=accepted.producer_commit,
                expected_fingerprint=contract.contract_fingerprint,
            )
            spec = compile_strategy_template(
                request.rules,
                strategy_id=accepted.strategy_id,
                version=accepted.version,
                producer_commit=accepted.producer_commit,
            )
            registration = registry.register_strategy_spec(
                spec,
                feature_contract_fingerprint=feature.fingerprint,
                registered_at=registered_at,
                available_at=registered_at,
                producer_commit=accepted.producer_commit,
                expected_fingerprint=spec.spec_fingerprint,
                parent_fingerprint=None
                if request.expected_head is None
                else request.expected_head.registration_fingerprint,
                supersedes=None if request.expected_head is None else request.expected_head.version,
                replacement_reason=None if request.expected_head is None else request.change_note,
            )
            return self._commit_saved(connection, accepted, registration)

    def _commit_saved(
        self,
        connection: sqlite3.Connection,
        accepted: AcceptedStrategyTemplateCommand,
        registration: StrategySpecRegistration,
    ) -> StrategyTemplateReceipt:
        request = accepted.request
        head = StrategyTemplateHead(
            version=registration.version,
            registration_fingerprint=registration.fingerprint,
            record_hash=registration.record_hash,
            spec_fingerprint=registration.spec.spec_fingerprint,
        )
        if (
            registration.logical_id != accepted.strategy_id
            or registration.version != accepted.version
            or registration.spec.parameters["rules"]
            != compile_strategy_template(
                request.rules,
                strategy_id=accepted.strategy_id,
                version=accepted.version,
                producer_commit=accepted.producer_commit,
            ).parameters["rules"]
        ):
            raise StrategyAuthoringIntegrityError(
                "original registration differs from accepted body"
            )
        metadata = StrategyTemplateVersion(
            owner_id=accepted.owner_id,
            strategy_id=accepted.strategy_id,
            head=head,
            parent_head=request.expected_head,
            name=request.name,
            change_note=request.change_note,
            saved_at=registration.registered_at,
            rules=request.rules,
        )
        connection.execute(
            "INSERT INTO versions VALUES(?,?,?,?)",
            (accepted.strategy_id, accepted.version, accepted.owner_id, metadata.model_dump_json()),
        )
        connection.execute(
            "INSERT INTO heads VALUES(?,?,?,0) ON CONFLICT(strategy_id) DO UPDATE SET head=excluded.head",
            (accepted.strategy_id, accepted.owner_id, head.model_dump_json()),
        )
        completed_at = self._now()
        if completed_at < registration.registered_at:
            raise StrategyAuthoringIntegrityError(
                "strategy server completion clock precedes publication"
            )
        receipt = StrategyTemplateReceipt(
            owner_id=accepted.owner_id,
            command_id=request.command_id,
            action="save",
            strategy_id=accepted.strategy_id,
            head=head,
            original_request_hash=request.request_hash,
            completed_at=completed_at,
        )
        connection.execute(
            "UPDATE command_refs SET receipt=? WHERE command_id=?",
            (receipt.model_dump_json(), request.command_id),
        )
        return receipt

    def accept_archive(
        self,
        request: ArchiveStrategyTemplate,
        *,
        owner_id: str,
        catalog: StrategySourceCatalog,
        expected_identity: StrategyAuthoringIdentity,
    ) -> AcceptedStrategyTemplateArchive:
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
            accepted = AcceptedStrategyTemplateArchive(
                owner_id=owner_id,
                request=request,
                original_request_hash=request.request_hash,
                accepted_at=self._now(),
                metadata_identity=self.identity(),
            )
            connection.execute(
                "INSERT INTO command_refs VALUES(?,?,?,?,?,NULL)",
                (
                    request.command_id,
                    owner_id,
                    request.request_hash,
                    request.strategy_id,
                    accepted.model_dump_json(),
                ),
            )
            return accepted

    def archive(
        self,
        request: ArchiveStrategyTemplate,
        *,
        owner_id: str,
        expected_identity: StrategyAuthoringIdentity | None = None,
    ) -> StrategyTemplateReceipt:
        request = ArchiveStrategyTemplate.model_validate(request.model_dump(mode="python"))
        owner_id = _owner(owner_id)
        with self._connection(write=True, expected_identity=expected_identity) as connection:
            old = self._command_row(connection, request, owner_id)
            if old is not None and old["receipt"] is not None:
                return StrategyTemplateReceipt.model_validate_json(old["receipt"])
            row = self._head_row(connection, request.strategy_id, owner_id)
            self._require_head(row, request.expected_head)
            self._require_idle(
                connection, request.strategy_id, exclude_command_id=request.command_id
            )
            if row["archived"]:
                raise StrategyAuthoringConflict("strategy is archived")
            self._version(connection, request.strategy_id, request.expected_head.version, owner_id)
            receipt = StrategyTemplateReceipt(
                owner_id=owner_id,
                command_id=request.command_id,
                action="archive",
                strategy_id=request.strategy_id,
                head=request.expected_head,
                original_request_hash=request.request_hash,
                completed_at=self._now(),
            )
            connection.execute(
                "UPDATE heads SET archived=1 WHERE strategy_id=?", (request.strategy_id,)
            )
            if old is None:
                connection.execute(
                    "INSERT INTO command_refs VALUES(?,?,?,?,NULL,?)",
                    (
                        request.command_id,
                        owner_id,
                        request.request_hash,
                        request.strategy_id,
                        receipt.model_dump_json(),
                    ),
                )
            else:
                if (
                    AcceptedStrategyTemplateArchive.model_validate_json(old["frozen"]).request
                    != request
                ):
                    raise StrategyAuthoringIntegrityError("archive differs from original admission")
                connection.execute(
                    "UPDATE command_refs SET receipt=? WHERE command_id=?",
                    (receipt.model_dump_json(), request.command_id),
                )
            return receipt

    def _version(
        self, connection: sqlite3.Connection, strategy_id: str, version: int, owner_id: str
    ) -> StrategyTemplateVersion:
        self._head_row(connection, strategy_id, owner_id)
        row = connection.execute(
            "SELECT metadata FROM versions WHERE strategy_id=? AND version=? AND owner_id=?",
            (strategy_id, version, owner_id),
        ).fetchone()
        if row is None:
            raise KeyError("strategy template version not found")
        metadata = StrategyTemplateVersion.model_validate_json(row[0])
        record = self.definition_registry(strategy_id).read_strategy_spec(
            metadata.head.registration_fingerprint
        )
        if record is None or (
            record.logical_id,
            record.version,
            record.record_hash,
            record.spec.spec_fingerprint,
        ) != (strategy_id, version, metadata.head.record_hash, metadata.head.spec_fingerprint):
            raise StrategyAuthoringIntegrityError(
                "strategy version lost its exact original definition"
            )
        expected_spec = compile_strategy_template(
            metadata.rules,
            strategy_id=strategy_id,
            version=version,
            producer_commit=record.producer_commit,
        )
        if (
            expected_spec.spec_fingerprint != record.spec.spec_fingerprint
            or metadata.owner_id != owner_id
            or metadata.saved_at != record.registered_at
        ):
            raise StrategyAuthoringIntegrityError(
                "strategy supplementary facts differ from original body"
            )
        return metadata

    def get_version(
        self, strategy_id: str, version: int, *, owner_id: str
    ) -> StrategyTemplateVersion:
        with self._connection() as connection:
            return self._version(connection, strategy_id, version, _owner(owner_id))

    def versions(self, strategy_id: str, *, owner_id: str) -> tuple[StrategyTemplateVersion, ...]:
        with self._connection() as connection:
            self._head_row(connection, strategy_id, _owner(owner_id))
            rows = connection.execute(
                "SELECT version FROM versions WHERE strategy_id=? ORDER BY version DESC LIMIT 4097",
                (strategy_id,),
            ).fetchall()
            if len(rows) > 4096:
                raise StrategyAuthoringIntegrityError("strategy versions exceed read budget")
            return tuple(self._version(connection, strategy_id, row[0], owner_id) for row in rows)

    def get_current(self, strategy_id: str, *, owner_id: str) -> StrategyTemplateCurrent:
        with self._connection() as connection:
            row = self._head_row(connection, strategy_id, _owner(owner_id))
            head = StrategyTemplateHead.model_validate_json(row["head"])
            version = self._version(connection, strategy_id, head.version, owner_id)
            if version.head != head:
                raise StrategyAuthoringIntegrityError(
                    "strategy current head differs from saved version"
                )
            return StrategyTemplateCurrent(
                **version.model_dump(mode="python"), archived=bool(row["archived"])
            )

    def list_current(self, *, owner_id: str) -> tuple[StrategyTemplateCurrent, ...]:
        owner_id = _owner(owner_id)
        with self._connection() as connection:
            rows = connection.execute(
                "SELECT strategy_id FROM heads WHERE owner_id=? ORDER BY strategy_id LIMIT 501",
                (owner_id,),
            ).fetchall()
            if len(rows) > 500:
                raise StrategyAuthoringIntegrityError("strategy count exceeds read budget")
            return tuple(self.get_current(row[0], owner_id=owner_id) for row in rows)

    @staticmethod
    def _promotion_schema(connection: sqlite3.Connection, *, create: bool = False) -> bool:
        tables = {
            "strategy_manual_stage",
            "strategy_manual_review",
            "strategy_manual_preparation",
            "strategy_manual_approval",
        }
        found = {
            row[0]
            for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")
        } & tables
        if found and found != tables:
            raise StrategyAuthoringIntegrityError("partial manual promotion schema")
        if not found and create:
            connection.execute(
                "CREATE TABLE strategy_manual_stage(target_key TEXT PRIMARY KEY,owner_id TEXT NOT NULL,state_json TEXT NOT NULL)"
            )
            connection.execute(
                "CREATE TABLE strategy_manual_review(review_id TEXT PRIMARY KEY,command_id TEXT UNIQUE NOT NULL,owner_id TEXT NOT NULL,request_hash TEXT NOT NULL,target_key TEXT NOT NULL,body_json TEXT NOT NULL)"
            )
            connection.execute(
                "CREATE TABLE strategy_manual_preparation(command_id TEXT PRIMARY KEY,owner_id TEXT NOT NULL,request_hash TEXT NOT NULL,body_json TEXT NOT NULL)"
            )
            connection.execute(
                "CREATE TABLE strategy_manual_approval(command_id TEXT PRIMARY KEY,effect_id TEXT UNIQUE NOT NULL,owner_id TEXT NOT NULL,request_hash TEXT NOT NULL,target_key TEXT NOT NULL,body_json TEXT NOT NULL)"
            )
            return True
        return bool(found)

    def _require_promotion_target(
        self,
        connection: sqlite3.Connection,
        target: StrategyPromotionTarget,
        *,
        current: bool,
        verify_builtin: Callable[[StrategyPromotionTarget], None] | None = None,
    ) -> None:
        if target.source_kind == "builtin":
            if verify_builtin is None:
                raise PermissionError("builtin promotion owner is not explicitly installed")
            verify_builtin(target)
            return
        value = self._version(connection, target.strategy_id, target.head.version, target.owner_id)
        if (value.head, value.name) != (target.head, target.name):
            raise StrategyAuthoringIntegrityError(
                "promotion target differs from original definition"
            )
        registration = self.definition_registry(target.strategy_id).read_strategy_spec(
            target.head.registration_fingerprint
        )
        if (
            registration is None
            or registration.spec.parameter_fingerprint != target.parameter_fingerprint
        ):
            raise StrategyAuthoringIntegrityError(
                "promotion fixed parameters differ from original definition"
            )
        if current:
            row = self._head_row(connection, target.strategy_id, target.owner_id)
            self._require_head(row, target.head)
            if row["archived"]:
                raise StrategyAuthoringConflict("strategy is archived")

    def _promotion_state_in(
        self, connection: sqlite3.Connection, target: StrategyPromotionTarget
    ) -> StrategyPromotionState:
        from rquant.strategy_promotion_contracts import StrategyPromotionState

        row = connection.execute(
            "SELECT owner_id,state_json FROM strategy_manual_stage WHERE target_key=?",
            (target.version_key,),
        ).fetchone()
        if row is None:
            return StrategyPromotionState(target=target)
        value = StrategyPromotionState.model_validate_json(row[1])
        if row[0] != target.owner_id or value.target != target:
            raise StrategyAuthoringIntegrityError("manual stage owner or exact version differs")
        return value

    def promotion_state(
        self,
        target: StrategyPromotionTarget,
        *,
        verify_builtin: Callable[[StrategyPromotionTarget], None] | None = None,
    ) -> StrategyPromotionState:
        from rquant.strategy_promotion_contracts import StrategyPromotionState

        with self._connection() as connection:
            self._require_promotion_target(
                connection, target, current=False, verify_builtin=verify_builtin
            )
            if not self._promotion_schema(connection):
                return StrategyPromotionState(target=target)
            return self._promotion_state_in(connection, target)

    @staticmethod
    def _promotion_lookup_in(
        connection: sqlite3.Connection, request: StrategyPromotionCommand, actor_id: str
    ) -> StrategyPromotionReview | PreparedPromotionApproval | StrategyPromotionApproval | None:
        from rquant.strategy_promotion_contracts import (
            PreparedPromotionApproval,
            StrategyPromotionApproval,
            StrategyPromotionReview,
        )

        rows = []
        for table in ("command_refs", "run_admissions"):
            original = connection.execute(
                f"SELECT owner_id FROM {table} WHERE command_id=?", (request.command_id,)
            ).fetchone()
            if original is not None:
                if original[0] != actor_id:
                    raise PermissionError("original command belongs to another actor")
                raise StrategyAuthoringConflict("original UUID has another request kind")
        if not StrategyAuthoringStore._promotion_schema(connection):
            return None
        for table, model in (
            ("strategy_manual_review", StrategyPromotionReview),
            ("strategy_manual_preparation", PreparedPromotionApproval),
            ("strategy_manual_approval", StrategyPromotionApproval),
        ):
            row = connection.execute(
                f"SELECT owner_id,request_hash,body_json FROM {table} WHERE command_id=?",
                (request.command_id,),
            ).fetchone()
            if row is not None:
                if row[0] != actor_id:
                    raise PermissionError("original promotion command belongs to another actor")
                if row[1] != request.request_hash:
                    raise StrategyAuthoringConflict("original promotion request body differs")
                rows.append(model.model_validate_json(row[2]))
        if len(rows) > 1:
            raise StrategyAuthoringIntegrityError("original promotion UUID has conflicting facts")
        return rows[0] if rows else None

    def lookup_promotion_command(
        self, request: StrategyPromotionCommand, *, actor_id: str
    ) -> StrategyPromotionReview | PreparedPromotionApproval | StrategyPromotionApproval | None:
        with self._connection() as connection:
            return self._promotion_lookup_in(connection, request, _owner(actor_id))

    def record_promotion_review(
        self,
        request: StrategyPromotionCommand,
        review: StrategyPromotionReview,
        *,
        verify_builtin: Callable[[StrategyPromotionTarget], None] | None = None,
    ) -> StrategyPromotionReview:
        from rquant.strategy_promotion_contracts import (
            MAX_PROMOTION_REVIEWS,
            StrategyPromotionReview,
        )
        from rquant.strategy_promotion_commands import RequestPromotionReview

        review = StrategyPromotionReview.model_validate(review.model_dump(mode="python"))
        if type(request) is not RequestPromotionReview or (
            str(review.command_id),
            review.actor_id,
            review.target,
            review.expected_revision,
            review.selection,
        ) != (
            request.command_id,
            request.target.owner_id,
            request.target,
            request.expected_revision,
            request.selection,
        ):
            raise StrategyAuthoringIntegrityError("review differs from its exact request")
        with self._connection(write=True, expected_identity=review.metadata_identity) as connection:
            self._promotion_schema(connection, create=True)
            old = self._promotion_lookup_in(connection, request, review.actor_id)
            if old is not None:
                if type(old) is not StrategyPromotionReview:
                    return self._bad_promotion_kind()
                return old
            self._require_promotion_target(
                connection, review.target, current=True, verify_builtin=verify_builtin
            )
            state = self._promotion_state_in(connection, review.target)
            if (state.revision, state.stage) != (review.expected_revision, review.from_stage):
                raise StrategyAuthoringConflict("manual stage changed before review")
            if (
                connection.execute("SELECT COUNT(*) FROM strategy_manual_review").fetchone()[0]
                >= MAX_PROMOTION_REVIEWS
            ):
                raise StrategyAuthoringConflict("manual review capacity reached")
            if (
                connection.execute("SELECT COUNT(*) FROM strategy_manual_stage").fetchone()[0]
                >= 4096
                and connection.execute(
                    "SELECT 1 FROM strategy_manual_stage WHERE target_key=?",
                    (review.target.version_key,),
                ).fetchone()
                is None
            ):
                raise StrategyAuthoringConflict("manual version capacity reached")
            connection.execute(
                "INSERT INTO strategy_manual_stage VALUES(?,?,?) ON CONFLICT(target_key) DO NOTHING",
                (review.target.version_key, review.target.owner_id, state.model_dump_json()),
            )
            connection.execute(
                "INSERT INTO strategy_manual_review VALUES(?,?,?,?,?,?)",
                (
                    review.review_id,
                    request.command_id,
                    review.actor_id,
                    request.request_hash,
                    review.target.version_key,
                    review.model_dump_json(),
                ),
            )
        return review

    @staticmethod
    def _bad_promotion_kind() -> None:
        raise StrategyAuthoringIntegrityError("promotion UUID has another original kind")

    def promotion_review(self, review_id: str, *, owner_id: str) -> StrategyPromotionReview:
        from rquant.strategy_promotion_contracts import StrategyPromotionReview

        with self._connection() as connection:
            if not self._promotion_schema(connection):
                raise KeyError("promotion review is unavailable")
            row = connection.execute(
                "SELECT owner_id,body_json FROM strategy_manual_review WHERE review_id=?",
                (review_id,),
            ).fetchone()
            if row is None:
                raise KeyError("promotion review is unavailable")
            if row[0] != owner_id:
                raise PermissionError("promotion review belongs to another owner")
            value = StrategyPromotionReview.model_validate_json(row[1])
            if value.review_id != review_id or value.actor_id != owner_id:
                raise StrategyAuthoringIntegrityError("review index differs from original fact")
            return value

    def record_promotion_preparation(
        self,
        request: StrategyPromotionCommand,
        prepared: PreparedPromotionApproval,
        *,
        verify_builtin: Callable[[StrategyPromotionTarget], None] | None = None,
    ) -> PreparedPromotionApproval:
        from rquant.strategy_promotion_contracts import PreparedPromotionApproval
        from rquant.strategy_promotion_commands import PreparePromotionApproval

        prepared = PreparedPromotionApproval.model_validate(prepared.model_dump(mode="python"))
        if type(request) is not PreparePromotionApproval or (
            str(prepared.preparation_id),
            prepared.actor_id,
            prepared.review.target,
            prepared.review.review_id,
        ) != (request.command_id, request.target.owner_id, request.target, request.review_id):
            raise StrategyAuthoringIntegrityError(
                "preparation differs from its exact original request"
            )
        with self._connection(
            write=True, expected_identity=prepared.review.metadata_identity
        ) as connection:
            self._promotion_schema(connection, create=True)
            old = self._promotion_lookup_in(connection, request, prepared.actor_id)
            if old is not None:
                if type(old) is not PreparedPromotionApproval:
                    self._bad_promotion_kind()
                return old
            original = connection.execute(
                "SELECT body_json FROM strategy_manual_review WHERE review_id=? AND owner_id=?",
                (request.review_id, prepared.actor_id),
            ).fetchone()
            if original is None or original[0] != prepared.review.model_dump_json():
                raise StrategyAuthoringIntegrityError(
                    "preparation review is not the recorded original"
                )
            self._require_promotion_target(
                connection, request.target, current=True, verify_builtin=verify_builtin
            )
            state = self._promotion_state_in(connection, request.target)
            if state.revision != prepared.review.expected_revision:
                raise StrategyAuthoringConflict("manual stage changed before confirmation")
            if (
                connection.execute("SELECT COUNT(*) FROM strategy_manual_preparation").fetchone()[0]
                >= 4096
            ):
                raise StrategyAuthoringConflict("manual preparation capacity reached")
            connection.execute(
                "INSERT INTO strategy_manual_preparation VALUES(?,?,?,?)",
                (
                    request.command_id,
                    prepared.actor_id,
                    request.request_hash,
                    prepared.model_dump_json(),
                ),
            )
        return prepared

    def apply_promotion_approval(
        self,
        request: StrategyPromotionCommand,
        *,
        effect_id: UUID,
        verify: Callable[[StrategyPromotionState], None],
        verify_builtin: Callable[[StrategyPromotionTarget], None] | None = None,
    ) -> StrategyPromotionApproval:
        from rquant.strategy_promotion import build_approval
        from rquant.strategy_promotion_contracts import StrategyPromotionApproval

        review = request.preparation.review
        with self._connection(write=True, expected_identity=review.metadata_identity) as connection:
            self._promotion_schema(connection, create=True)
            old = self._promotion_lookup_in(connection, request, review.actor_id)
            if old is not None:
                if type(old) is not StrategyPromotionApproval:
                    self._bad_promotion_kind()
                if old.effect_id != effect_id:
                    raise StrategyAuthoringConflict("original promotion effect differs")
                return old
            self._require_promotion_target(
                connection, request.target, current=True, verify_builtin=verify_builtin
            )
            stored = connection.execute(
                "SELECT body_json FROM strategy_manual_review WHERE review_id=?",
                (review.review_id,),
            ).fetchone()
            if stored is None or stored[0] != review.model_dump_json():
                raise StrategyAuthoringIntegrityError(
                    "approval review is not the recorded original"
                )
            preparation = connection.execute(
                "SELECT body_json FROM strategy_manual_preparation WHERE command_id=?",
                (str(request.preparation.preparation_id),),
            ).fetchone()
            if preparation is None or preparation[0] != request.preparation.model_dump_json():
                raise StrategyAuthoringIntegrityError(
                    "approval preparation is not the recorded original"
                )
            state = self._promotion_state_in(connection, request.target)
            if (state.revision, state.stage) != (review.expected_revision, review.from_stage):
                raise StrategyAuthoringConflict("manual stage revision conflict")
            verify(state)
            approval = build_approval(request, state, effect_id=effect_id, applied_at=self._now())
            changed = connection.execute(
                "UPDATE strategy_manual_stage SET state_json=? WHERE target_key=? AND state_json=?",
                (
                    approval.after.model_dump_json(),
                    request.target.version_key,
                    state.model_dump_json(),
                ),
            ).rowcount
            if changed != 1:
                raise StrategyAuthoringConflict("manual stage CAS conflict")
            connection.execute(
                "INSERT INTO strategy_manual_approval VALUES(?,?,?,?,?,?)",
                (
                    request.command_id,
                    str(effect_id),
                    review.actor_id,
                    request.request_hash,
                    request.target.version_key,
                    approval.model_dump_json(),
                ),
            )
        return approval

    def promotion_snapshot(
        self, *, owner_id: str | None = None
    ) -> tuple[tuple[StrategyPromotionState, ...], tuple[StrategyPromotionReview, ...]]:
        from rquant.strategy_promotion_contracts import (
            StrategyPromotionReview,
            StrategyPromotionState,
        )

        with self._connection() as connection:
            if not self._promotion_schema(connection):
                return (), ()
            clause = " WHERE owner_id=?" if owner_id is not None else ""
            values = () if owner_id is None else (_owner(owner_id),)
            rows = connection.execute(
                "SELECT owner_id,target_key,state_json FROM strategy_manual_stage"
                + clause
                + " ORDER BY target_key LIMIT 4097",
                values,
            ).fetchall()
            if len(rows) > 4096:
                raise StrategyAuthoringIntegrityError("manual state capacity exceeded")
            states = tuple(StrategyPromotionState.model_validate_json(row[2]) for row in rows)
            if any(
                (state.target.owner_id, state.target.version_key) != (row[0], row[1])
                for state, row in zip(states, rows, strict=True)
            ):
                raise StrategyAuthoringIntegrityError("manual state index differs from exact fact")
            reviews = connection.execute(
                "SELECT body_json FROM strategy_manual_review"
                + clause
                + " ORDER BY rowid DESC LIMIT 1000",
                values,
            ).fetchall()
            return states, tuple(
                StrategyPromotionReview.model_validate_json(row[0]) for row in reviews
            )

    def promotion_approvals(
        self, *, owner_id: str | None = None
    ) -> tuple[StrategyPromotionApproval, ...]:
        from rquant.strategy_promotion_contracts import StrategyPromotionApproval

        with self._connection() as connection:
            if not self._promotion_schema(connection):
                return ()
            clause = " WHERE owner_id=?" if owner_id is not None else ""
            values = () if owner_id is None else (_owner(owner_id),)
            rows = connection.execute(
                "SELECT command_id,effect_id,owner_id,target_key,body_json FROM strategy_manual_approval"
                + clause
                + " ORDER BY rowid DESC LIMIT 12289",
                values,
            ).fetchall()
            if len(rows) > 12288:
                raise StrategyAuthoringIntegrityError("manual approval capacity exceeded")
            approvals = tuple(StrategyPromotionApproval.model_validate_json(row[4]) for row in rows)
            if any(
                (
                    str(value.command_id),
                    str(value.effect_id),
                    value.actor_id,
                    value.after.target.version_key,
                )
                != (row[0], row[1], row[2], row[3])
                for row, value in zip(rows, approvals, strict=True)
            ):
                raise StrategyAuthoringIntegrityError(
                    "manual approval index differs from original fact"
                )
            return approvals

    def admit_run(
        self,
        strategy_id: str,
        head: StrategyTemplateHead,
        *,
        owner_id: str,
        command_id: str,
        request_hash: str,
        spec_hash: str | None = None,
    ) -> StrategyTemplateVersion:
        owner_id = _owner(owner_id)
        exact_spec_hash = request_hash if spec_hash is None else spec_hash
        if re.fullmatch(r"[0-9a-f]{64}", exact_spec_hash) is None:
            raise ValueError("strategy run requires its exact original plan hash")
        with self._connection(write=True) as connection:
            old = connection.execute(
                "SELECT * FROM run_admissions WHERE command_id=?", (command_id,)
            ).fetchone()
            if old is not None:
                if old["owner_id"] != owner_id:
                    raise PermissionError("strategy run belongs to another owner")
                if (
                    old["strategy_id"],
                    old["request_hash"],
                    old["spec_hash"],
                    StrategyTemplateHead.model_validate_json(old["head"]),
                ) != (strategy_id, request_hash, exact_spec_hash, head):
                    raise StrategyAuthoringConflict("original strategy run differs")
                return self._version(connection, strategy_id, head.version, owner_id)
            row = self._head_row(connection, strategy_id, owner_id)
            self._require_head(row, head)
            self._require_idle(connection, strategy_id)
            if row["archived"]:
                raise StrategyAuthoringConflict("strategy is archived")
            version = self._version(connection, strategy_id, head.version, owner_id)
            self._require_run_capacity(connection)
            connection.execute(
                "INSERT INTO run_admissions(command_id,owner_id,request_hash,strategy_id,head,spec_hash) VALUES(?,?,?,?,?,?)",
                (
                    command_id,
                    owner_id,
                    request_hash,
                    strategy_id,
                    head.model_dump_json(),
                    exact_spec_hash,
                ),
            )
        return version

    @staticmethod
    def _require_run_capacity(connection: sqlite3.Connection) -> None:
        if (
            connection.execute("SELECT COUNT(*) FROM run_admissions").fetchone()[0]
            >= MAX_TEMPLATE_RUN_ADMISSIONS
        ):
            raise StrategyAuthoringConflict("strategy run admission budget reached")

    def accepted_run(
        self,
        request: RunStrategyTemplate,
        *,
        owner_id: str,
        expected_identity: StrategyAuthoringIdentity,
    ) -> AcceptedStrategyTemplateRun | None:
        with self._connection(expected_identity=expected_identity) as connection:
            row = self._command_row(connection, request, _owner(owner_id))
            if row is None:
                return None
            if row["frozen"] is None:
                raise StrategyAuthoringIntegrityError(
                    "template run lacks its original accepted plan"
                )
            accepted = AcceptedStrategyTemplateRun.model_validate_json(row["frozen"])
            if (
                accepted.metadata_identity != expected_identity
                or accepted.request != request
                or accepted.owner_id != owner_id
                or accepted.spec.spec_hash != row["spec_hash"]
            ):
                raise StrategyAuthoringIntegrityError("template accepted run identity differs")
            return accepted

    def verify_new_run(
        self,
        request: RunStrategyTemplate,
        *,
        owner_id: str,
        expected_identity: StrategyAuthoringIdentity,
    ) -> None:
        with self._connection(expected_identity=expected_identity) as connection:
            if self._command_row(connection, request, _owner(owner_id)) is not None:
                return
            row = self._head_row(connection, request.strategy_id, owner_id)
            self._require_head(row, request.expected_head)
            self._require_idle(connection, request.strategy_id)
            if row["archived"]:
                raise StrategyAuthoringConflict("strategy is archived")
            if (
                self._version(connection, request.strategy_id, request.head.version, owner_id).head
                != request.head
            ):
                raise StrategyAuthoringConflict("selected strategy version differs")
            self._require_run_capacity(connection)

    def commit_run_admission(
        self, accepted: AcceptedStrategyTemplateRun
    ) -> AcceptedStrategyTemplateRun:
        accepted = AcceptedStrategyTemplateRun.model_validate(accepted.model_dump(mode="python"))
        request = accepted.request
        with self._connection(
            write=True, expected_identity=accepted.metadata_identity
        ) as connection:
            old = self._command_row(connection, request, _owner(accepted.owner_id))
            if old is not None:
                return AcceptedStrategyTemplateRun.model_validate_json(old["frozen"])
            row = self._head_row(connection, request.strategy_id, accepted.owner_id)
            self._require_head(row, request.expected_head)
            self._require_idle(connection, request.strategy_id)
            if row["archived"]:
                raise StrategyAuthoringConflict("strategy is archived")
            version = self._version(
                connection, request.strategy_id, request.head.version, accepted.owner_id
            )
            if version.head != request.head:
                raise StrategyAuthoringConflict("selected strategy version differs")
            self._require_run_capacity(connection)
            connection.execute(
                "INSERT INTO run_admissions VALUES(?,?,?,?,?,?,?,NULL)",
                (
                    request.command_id,
                    accepted.owner_id,
                    request.request_hash,
                    request.strategy_id,
                    request.head.model_dump_json(),
                    accepted.spec.spec_hash,
                    accepted.model_dump_json(),
                ),
            )
        return accepted

    def complete_run_receipt(
        self, accepted: AcceptedStrategyTemplateRun, receipt: StrategyTemplateRunReceipt
    ) -> StrategyTemplateRunReceipt:
        with self._connection(
            write=True, expected_identity=accepted.metadata_identity
        ) as connection:
            row = self._command_row(connection, accepted.request, _owner(accepted.owner_id))
            if (
                row is None
                or AcceptedStrategyTemplateRun.model_validate_json(row["frozen"]) != accepted
            ):
                raise StrategyAuthoringIntegrityError("template original run admission differs")
            if row["receipt"] is not None:
                return StrategyTemplateRunReceipt.model_validate_json(row["receipt"])
            if (
                receipt.owner_id,
                receipt.command_id,
                receipt.strategy_id,
                receipt.head,
                receipt.original_request_hash,
                receipt.spec_hash,
            ) != (
                accepted.owner_id,
                accepted.request.command_id,
                accepted.request.strategy_id,
                accepted.request.head,
                accepted.request.request_hash,
                accepted.spec.spec_hash,
            ):
                raise StrategyAuthoringIntegrityError("template original run receipt differs")
            connection.execute(
                "UPDATE run_admissions SET receipt=? WHERE command_id=?",
                (receipt.model_dump_json(), receipt.command_id),
            )
        return receipt


class StrategyAuthoringPageControlBackend:
    def __init__(
        self,
        store: StrategyAuthoringStore,
        *,
        editor_users: tuple[str, ...],
        enabled: bool = False,
        run_backend: StrategyTemplateRunBackend | None = None,
    ) -> None:
        self.store = store
        self.editor_users = editor_users
        self.enabled = enabled
        if run_backend is not None:
            from rquant.strategy_template_submission import StrategyTemplateRunBackend

            if (
                type(run_backend) is not StrategyTemplateRunBackend
                or run_backend.store is not store
            ):
                raise TypeError("template authoring requires its concrete run backend")
        self.run_backend = run_backend

    def authorize(self, actor_id: str) -> None:
        if not self.enabled or _owner(actor_id) not in self.editor_users:
            raise PermissionError("strategy authoring is not enabled for this owner")

    def identity(self) -> StrategyAuthoringIdentity:
        return self.store.identity()

    def compile(
        self,
        request: StrategyTemplateCommand,
        *,
        authenticated_actor_id: str,
        catalog: StrategySourceCatalog,
        expected_identity: StrategyAuthoringIdentity,
    ) -> OwnedStrategyTemplateCommand:
        self.authorize(authenticated_actor_id)
        if type(request) is RunStrategyTemplate:
            if self.run_backend is None:
                raise PermissionError("template run producer is not installed")
            return self.run_backend.compile(
                request, owner_id=authenticated_actor_id, expected_identity=expected_identity
            )
        if type(request) is SaveStrategyTemplate:
            accepted = self.store.accept(
                request,
                owner_id=authenticated_actor_id,
                catalog=catalog,
                expected_identity=expected_identity,
            )
            return OwnedSaveStrategyTemplate(
                **request.model_dump(mode="python"),
                owner_id=authenticated_actor_id,
                metadata_identity=expected_identity,
                accepted=accepted,
            )
        if type(request) is not ArchiveStrategyTemplate:
            raise TypeError("strategy admission requires an ownerless request")
        accepted = self.store.accept_archive(
            request,
            owner_id=authenticated_actor_id,
            catalog=catalog,
            expected_identity=expected_identity,
        )
        return OwnedArchiveStrategyTemplate(
            **request.model_dump(mode="python"),
            owner_id=authenticated_actor_id,
            metadata_identity=expected_identity,
            accepted=accepted,
        )

    def validate(self, command: OwnedStrategyTemplateCommand) -> None:
        self.authorize(command.owner_id)
        if self.identity() != command.metadata_identity:
            raise StrategyAuthoringIntegrityError("strategy metadata changed after admission")

    def submit(self, command: OwnedStrategyTemplateCommand) -> JsonValue:
        if isinstance(command, OwnedRunStrategyTemplate):
            self.authorize(command.owner_id)
            if self.run_backend is None:
                raise PermissionError("template run producer is not installed")
            return self.run_backend.submit(command).model_dump(mode="json")
        if isinstance(command, OwnedSaveStrategyTemplate):
            self.authorize(command.owner_id)
            return self.store.complete_save(
                command.accepted, expected_identity=command.metadata_identity
            ).model_dump(mode="json")
        if isinstance(command, OwnedArchiveStrategyTemplate):
            self.authorize(command.owner_id)
            return self.store.archive(
                command.original(),
                owner_id=command.owner_id,
                expected_identity=command.metadata_identity,
            ).model_dump(mode="json")
        raise TypeError("strategy authoring backend requires an owned command")

    def recover(self, command: OwnedStrategyTemplateCommand) -> JsonValue | None:
        if not isinstance(
            command,
            (OwnedSaveStrategyTemplate, OwnedArchiveStrategyTemplate, OwnedRunStrategyTemplate),
        ):
            raise TypeError("strategy authoring recovery requires an owned command")
        self.authorize(command.owner_id)
        receipt = self.store.lookup_command(
            command.original(),
            owner_id=command.owner_id,
            expected_identity=command.metadata_identity,
        )
        if receipt is not None:
            return receipt.model_dump(mode="json")
        # The journal carries the already admitted original request. Completing it
        # reuses its exact immutable registration after registry/metadata interruption.
        return self.submit(command)
