"""Immutable formula pool creation from a verified market task and sealed result."""

from __future__ import annotations

import hashlib
import os
import re
import stat
from collections.abc import Callable
from contextlib import suppress
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Literal
from uuid import uuid4

from pydantic import Field, field_validator, model_validator

from rquant.page_control import SaveFormulaPoolV1
from rquant.runtime_contracts import (
    AwareUtcDatetime,
    RuntimeContractModel,
    canonical_sha256,
    normalize_aware_utc,
)
from rquant.screen.formula_market_jobs import FormulaMarketJobStore
from rquant.screen.tdx.ast import SYNTAX_VERSION
from rquant.strict_json import canonical_json_bytes, strict_canonical_json_loads

_NAME = re.compile(r"^[\w\u4e00-\u9fff-]+$")
_MAX_DEFINITION_BYTES = 16 * 1024
_DIR_FLAGS = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)
_READ_FLAGS = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
_WRITE_FLAGS = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0)


class FormulaPoolCreationEvidence(RuntimeContractModel):
    task_id: str = Field(pattern=r"^[0-9a-f]{32}$")
    request_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    result_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    formula_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    trade_date: date
    decision_at: AwareUtcDatetime
    universe_identity: str = Field(pattern=r"^[0-9a-f]{64}$")
    projection_identity: str = Field(pattern=r"^[0-9a-f]{64}$")


class FormulaPoolDefinitionV1(RuntimeContractModel):
    """Reusable formula and a separate, dated proof of why it was first saved."""

    schema_version: Literal[1] = 1
    pool_name: str = Field(min_length=6, max_length=85)
    display_name: str = Field(min_length=1, max_length=80)
    formula: str = Field(min_length=1, max_length=4096)
    syntax_version: Literal["tdx-v1"] = "tdx-v1"
    created_at: AwareUtcDatetime
    creation: FormulaPoolCreationEvidence
    command_id: str = Field(min_length=1, max_length=128)
    command_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    version: str = Field(pattern=r"^[0-9a-f]{64}$")

    @field_validator("pool_name")
    @classmethod
    def require_user_name(cls, value: str) -> str:
        if not value.startswith("user/") or _NAME.fullmatch(value.removeprefix("user/")) is None:
            raise ValueError("formula pool requires a safe user/ name")
        return value

    @model_validator(mode="after")
    def validate_definition(self) -> FormulaPoolDefinitionV1:
        if not self.display_name.strip():
            raise ValueError("formula pool display name is required")
        if hashlib.sha256(self.formula.encode("utf-8")).hexdigest() != self.creation.formula_sha256:
            raise ValueError("formula pool formula digest disagrees with creation evidence")
        if canonical_sha256(self.model_dump(mode="python", exclude={"version"})) != self.version:
            raise ValueError("formula pool definition version is invalid")
        return self

    @classmethod
    def create(
        cls,
        *,
        pool_name: str,
        display_name: str,
        formula: str,
        created_at: datetime,
        creation: FormulaPoolCreationEvidence,
        command_id: str,
        command_hash: str,
    ) -> FormulaPoolDefinitionV1:
        content = {
            "schema_version": 1,
            "pool_name": pool_name,
            "display_name": display_name.strip(),
            "formula": formula,
            "syntax_version": "tdx-v1",
            "created_at": normalize_aware_utc(created_at),
            "creation": creation,
            "command_id": command_id,
            "command_hash": command_hash,
        }
        return cls(**content, version=canonical_sha256(content))


def _canonical_path(path: Path) -> Path:
    candidate = Path(path)
    if (
        not candidate.is_absolute()
        or candidate != Path(os.path.abspath(candidate))
        or candidate.resolve(strict=False) != candidate
    ):
        raise ValueError("formula pool directory must be an absolute canonical path")
    return candidate


def _open_private_directory(path: Path, *, create: bool) -> int:
    _canonical_path(path)
    if create:
        path.mkdir(mode=0o700, parents=True, exist_ok=True)
    before = path.lstat()
    if (
        not stat.S_ISDIR(before.st_mode)
        or before.st_uid != os.geteuid()
        or stat.S_IMODE(before.st_mode) != 0o700
    ):
        raise ValueError("formula pool directory must be private and owned")
    directory = os.open(path, _DIR_FLAGS)
    opened = os.fstat(directory)
    if (opened.st_dev, opened.st_ino) != (before.st_dev, before.st_ino):
        os.close(directory)
        raise ValueError("formula pool directory changed while opening")
    return directory


def _file_identity(value: os.stat_result) -> tuple[int, int, int, int, int, int, int, int]:
    return (
        value.st_dev,
        value.st_ino,
        value.st_mode,
        value.st_uid,
        value.st_nlink,
        value.st_size,
        value.st_mtime_ns,
        value.st_ctime_ns,
    )


class FormulaPoolDefinitionStore:
    """Separate private directory; create-only names and version-checked reads."""

    def __init__(self, *, definition_root: Path, rule_pool_root: Path) -> None:
        self.definition_root = _canonical_path(definition_root)
        self.rule_pool_root = _canonical_path(rule_pool_root)
        if self.definition_root == self.rule_pool_root:
            raise ValueError("formula and rule pools require separate directories")

    @staticmethod
    def _name(base_name: str) -> str:
        if _NAME.fullmatch(base_name) is None or base_name in {".", ".."}:
            raise ValueError("formula pool name contains unsafe characters")
        return f"{base_name}.json"

    def read(
        self, base_name: str, *, expected_version: str | None = None
    ) -> FormulaPoolDefinitionV1:
        name = self._name(base_name)
        directory = _open_private_directory(self.definition_root, create=False)
        try:
            before = os.stat(name, dir_fd=directory, follow_symlinks=False)
            if (
                not stat.S_ISREG(before.st_mode)
                or before.st_uid != os.geteuid()
                or stat.S_IMODE(before.st_mode) != 0o600
                or before.st_nlink != 1
                or not 0 < before.st_size <= _MAX_DEFINITION_BYTES
            ):
                raise ValueError("formula pool definition file is unsafe")
            descriptor = os.open(name, _READ_FLAGS, dir_fd=directory)
            try:
                if _file_identity(os.fstat(descriptor)) != _file_identity(before):
                    raise ValueError("formula pool definition changed while opening")
                payload = os.read(descriptor, _MAX_DEFINITION_BYTES + 1)
                if (
                    len(payload) != before.st_size
                    or _file_identity(os.fstat(descriptor)) != _file_identity(before)
                    or _file_identity(os.stat(name, dir_fd=directory, follow_symlinks=False))
                    != _file_identity(before)
                ):
                    raise ValueError("formula pool definition changed while reading")
            finally:
                os.close(descriptor)
        finally:
            os.close(directory)
        definition = FormulaPoolDefinitionV1.model_validate(strict_canonical_json_loads(payload))
        if (
            canonical_json_bytes(definition.model_dump(mode="json")) != payload
            or definition.pool_name != f"user/{base_name}"
        ):
            raise ValueError("formula pool definition content is invalid")
        if expected_version is not None and definition.version != expected_version:
            raise ValueError("formula pool version conflict")
        return definition

    def _reject_rule_name(self, base_name: str) -> None:
        try:
            directory = _open_private_directory(self.rule_pool_root, create=False)
        except FileNotFoundError:
            return
        try:
            try:
                os.stat(self._name(base_name), dir_fd=directory, follow_symlinks=False)
            except FileNotFoundError:
                return
            raise FileExistsError("a rule pool already uses this user/ name")
        finally:
            os.close(directory)

    def create(self, definition: FormulaPoolDefinitionV1) -> FormulaPoolDefinitionV1:
        definition = FormulaPoolDefinitionV1.model_validate(definition)
        base_name = definition.pool_name.removeprefix("user/")
        name = self._name(base_name)
        self._reject_rule_name(base_name)
        try:
            self.read(base_name)
        except FileNotFoundError:
            pass
        else:
            raise FileExistsError("formula pool name already exists")
        payload = canonical_json_bytes(definition.model_dump(mode="json"))
        if not 0 < len(payload) <= _MAX_DEFINITION_BYTES:
            raise ValueError("formula pool definition exceeds byte budget")
        directory = _open_private_directory(self.definition_root, create=True)
        stage = f".{name}.{uuid4().hex}.stage"
        try:
            descriptor = os.open(stage, _WRITE_FLAGS, 0o600, dir_fd=directory)
            try:
                os.fchmod(descriptor, 0o600)
                view = memoryview(payload)
                while view:
                    written = os.write(descriptor, view)
                    if written <= 0:
                        raise OSError("formula pool definition write made no progress")
                    view = view[written:]
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
            self._reject_rule_name(base_name)
            os.link(stage, name, src_dir_fd=directory, dst_dir_fd=directory, follow_symlinks=False)
            os.unlink(stage, dir_fd=directory)
            os.fsync(directory)
        finally:
            with suppress(FileNotFoundError):
                os.unlink(stage, dir_fd=directory)
            os.close(directory)
        return self.read(base_name, expected_version=definition.version)


class FormulaPoolSaveBackend:
    """Build a reusable definition only from one verified successful task."""

    def __init__(
        self,
        *,
        task_store: FormulaMarketJobStore,
        definitions: FormulaPoolDefinitionStore,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self.task_store = task_store
        self.definitions = definitions
        self.clock = clock or (lambda: datetime.now(UTC))

    @staticmethod
    def _receipt(definition: FormulaPoolDefinitionV1) -> dict[str, str]:
        return {"pool_name": definition.pool_name, "version": definition.version}

    def recover(self, command: SaveFormulaPoolV1) -> dict[str, str] | None:
        command = SaveFormulaPoolV1.model_validate(command)
        try:
            existing = self.definitions.read(command.base_name)
        except FileNotFoundError:
            return None
        except ValueError:
            # An invalid existing file cannot prove this command committed successfully.
            return None
        if (
            existing.command_id != command.command_id
            or existing.command_hash != canonical_sha256(command.model_dump(mode="json"))
            or existing.creation.task_id != command.task_id
            or existing.display_name != command.display_name.strip()
        ):
            return None
        return self._receipt(existing)

    def submit(self, command: SaveFormulaPoolV1) -> dict[str, str]:
        command = SaveFormulaPoolV1.model_validate(command)
        recovered = self.recover(command)
        if recovered is not None:
            return recovered
        if command.expected_version is not None:
            raise ValueError("formula pool v1 is create-only; expected_version must be empty")
        if SYNTAX_VERSION != "tdx-v1":
            raise ValueError("formula syntax contract is not supported for pool creation")
        request, result = self.task_store.read_succeeded_task(command.task_id)
        definition = FormulaPoolDefinitionV1.create(
            pool_name=f"user/{command.base_name}",
            display_name=command.display_name,
            formula=request.formula,
            created_at=self.clock(),
            creation=FormulaPoolCreationEvidence(
                task_id=command.task_id,
                request_sha256=result.request_sha256,
                result_sha256=result.content_sha256,
                formula_sha256=result.formula_sha256,
                trade_date=request.trade_date,
                decision_at=request.decision_at,
                universe_identity=request.expected_universe_sha256,
                projection_identity=request.expected_projection_identity,
            ),
            command_id=command.command_id,
            command_hash=canonical_sha256(command.model_dump(mode="json")),
        )
        return self._receipt(self.definitions.create(definition))
