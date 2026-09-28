"""Bounded, replayable admission and publication for saved formula pools."""

from __future__ import annotations

import argparse
import os
import re
import stat
import sys
from collections.abc import Callable, Sequence
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Literal

from pydantic import Field, field_validator, model_validator

from rquant.formula_market_private_config import FormulaMarketPrivateConfig
from rquant.formula_pool_daily import FormulaPoolDailyRecalculator, FormulaPoolDailyResultV1
from rquant.formula_pool_definition import (
    FormulaPoolDefinitionStore,
    FormulaPoolDefinitionV1,
    _canonical_path,
    _open_private_directory,
)
from rquant.runtime_contracts import RuntimeContractModel, canonical_sha256
from rquant.screen.formula_market_jobs import (
    FormulaMarketJobActiveError,
    FormulaMarketJobStore,
)
from rquant.screen.tdx.ast import SYNTAX_VERSION
from rquant.strict_json import canonical_json_bytes, strict_canonical_json_loads

_MAX_DEFINITIONS = 512
_MAX_PAGE = 64
_MAX_CONFIG_BYTES = 8192
_CURSOR = re.compile(r"^([0-9a-f]{64}):([1-9][0-9]{0,3})$")
_READ_FLAGS = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)


class FormulaPoolBatchPrivateConfig(RuntimeContractModel):
    market: FormulaMarketPrivateConfig
    definition_root: Path
    rule_pool_root: Path
    daily_result_root: Path

    @field_validator("definition_root", "rule_pool_root", "daily_result_root")
    @classmethod
    def require_canonical_absolute_path(cls, value: Path) -> Path:
        return _canonical_path(value)

    @model_validator(mode="after")
    def require_distinct_roots(self) -> FormulaPoolBatchPrivateConfig:
        paths = (
            self.market.universe_root,
            self.market.projection_root,
            self.market.state_path,
            self.market.artifact_directory,
            self.definition_root,
            self.rule_pool_root,
            self.daily_result_root,
        )
        if len(set(paths)) != len(paths) or any(
            left != right and left in right.parents for left in paths for right in paths
        ):
            raise ValueError("formula batch paths must be distinct")
        return self


def load_private_formula_pool_batch_config(path: Path) -> FormulaPoolBatchPrivateConfig:
    """Read one explicit owner-private config, with no environment fallback."""
    candidate = _canonical_path(path)
    before = candidate.lstat()
    if (
        not stat.S_ISREG(before.st_mode)
        or before.st_uid != os.geteuid()
        or stat.S_IMODE(before.st_mode) != 0o600
        or before.st_nlink != 1
        or not 0 < before.st_size <= _MAX_CONFIG_BYTES
    ):
        raise ValueError("formula batch config must be an owner-private regular file")
    descriptor = os.open(candidate, _READ_FLAGS)
    try:
        opened = os.fstat(descriptor)
        if _identity(opened) != _identity(before):
            raise ValueError("formula batch config changed while opening")
        payload = os.read(descriptor, _MAX_CONFIG_BYTES + 1)
        if (
            len(payload) != before.st_size
            or _identity(os.fstat(descriptor)) != _identity(before)
            or _identity(candidate.lstat()) != _identity(before)
        ):
            raise ValueError("formula batch config changed while reading")
    finally:
        os.close(descriptor)
    config = FormulaPoolBatchPrivateConfig.model_validate(strict_canonical_json_loads(payload))
    if canonical_json_bytes(config.model_dump(mode="json")) != payload:
        raise ValueError("formula batch config is not canonical")
    return config


def _identity(value: os.stat_result) -> tuple[int, ...]:
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


class FormulaPoolBatchDailyProof(RuntimeContractModel):
    pool_name: str
    definition_version: str = Field(pattern=r"^[0-9a-f]{64}$")
    trade_date: date
    task_id: str = Field(pattern=r"^[0-9a-f]{32}$")
    run_identity: str = Field(pattern=r"^[0-9a-f]{64}$")
    universe_identity: str = Field(pattern=r"^[0-9a-f]{64}$")
    projection_identity: str = Field(pattern=r"^[0-9a-f]{64}$")
    match_count: int = Field(ge=0)
    no_match_count: int = Field(ge=0)
    unknown_count: int = Field(ge=0)
    member_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    content_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")

    @classmethod
    def from_daily(cls, daily: FormulaPoolDailyResultV1) -> FormulaPoolBatchDailyProof:
        return cls.model_validate(
            daily.model_dump(
                include={
                    "pool_name",
                    "definition_version",
                    "trade_date",
                    "task_id",
                    "run_identity",
                    "universe_identity",
                    "projection_identity",
                    "match_count",
                    "no_match_count",
                    "unknown_count",
                    "member_sha256",
                    "content_sha256",
                }
            )
        )


class FormulaPoolBatchItem(RuntimeContractModel):
    pool_name: str
    definition_version: str = Field(pattern=r"^[0-9a-f]{64}$")
    status: Literal["completed", "waiting", "failed"]
    task_id: str | None = Field(default=None, pattern=r"^[0-9a-f]{32}$")
    error_code: str | None = None
    daily: FormulaPoolBatchDailyProof | None = None

    @model_validator(mode="after")
    def require_evidence_for_completion(self) -> FormulaPoolBatchItem:
        if (self.status == "completed") != (self.daily is not None):
            raise ValueError("completed pool requires daily evidence")
        if self.daily is not None and (
            self.daily.pool_name != self.pool_name
            or self.daily.definition_version != self.definition_version
            or self.daily.task_id != self.task_id
        ):
            raise ValueError("batch pool result does not match its evidence")
        if (self.status == "failed") != (self.error_code is not None):
            raise ValueError("failed pool requires safe error category")
        return self


class FormulaPoolBatchResult(RuntimeContractModel):
    """One invocation's page; counts never aggregate earlier invocations."""

    trade_date: date
    catalog_identity: str = Field(pattern=r"^[0-9a-f]{64}$")
    total_count: int = Field(ge=0)
    page_start: int = Field(ge=0)
    pools: tuple[FormulaPoolBatchItem, ...]
    completed_count: int = Field(ge=0)
    waiting_count: int = Field(ge=0)
    failed_count: int = Field(ge=0)
    unprocessed_count: int = Field(
        ge=0, description="Catalog entries outside this invocation, including previous pages"
    )
    next_cursor: str | None = None
    all_complete: bool = Field(
        description="True only when this invocation verified every catalog entry"
    )

    @model_validator(mode="after")
    def require_honest_summary(self) -> FormulaPoolBatchResult:
        counts = {
            status: sum(item.status == status for item in self.pools)
            for status in ("completed", "waiting", "failed")
        }
        if (
            self.completed_count != counts["completed"]
            or self.waiting_count != counts["waiting"]
            or self.failed_count != counts["failed"]
            or self.unprocessed_count != self.total_count - len(self.pools)
            or self.page_start + len(self.pools) > self.total_count
            or self.all_complete
            != (self.page_start == 0 and self.completed_count == self.total_count)
        ):
            raise ValueError("formula batch summary is inconsistent")
        return self


class FormulaPoolBatchCoordinator:
    """Preflight the full catalog, then touch only one bounded stable page."""

    def __init__(
        self,
        *,
        config: FormulaPoolBatchPrivateConfig,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self.config = FormulaPoolBatchPrivateConfig.model_validate(config)
        self.clock = clock or (lambda: datetime.now(UTC))
        self.definitions = FormulaPoolDefinitionStore(
            definition_root=self.config.definition_root,
            rule_pool_root=self.config.rule_pool_root,
        )
        store = FormulaMarketJobStore(
            state_path=self.config.market.state_path,
            artifact_directory=self.config.market.artifact_directory,
            clock=self.clock,
        )
        self.runner = FormulaPoolDailyRecalculator(
            config=self.config.market,
            task_store=store,
            definitions=self.definitions,
            result_root=self.config.daily_result_root,
            clock=self.clock,
        )

    def _catalog(self) -> tuple[tuple[FormulaPoolDefinitionV1, ...], str]:
        rules = _open_private_directory(self.config.rule_pool_root, create=False)
        os.close(rules)
        directory = _open_private_directory(self.config.definition_root, create=False)
        try:
            names = sorted(entry.name for entry in os.scandir(directory))
            if len(names) > _MAX_DEFINITIONS:
                raise ValueError("formula definition catalog exceeds capacity")
            definitions: list[FormulaPoolDefinitionV1] = []
            for name in names:
                if not name.endswith(".json"):
                    raise ValueError("formula definition catalog contains a stage or invalid file")
                base_name = name.removesuffix(".json")
                definition = self.definitions.read(base_name)
                if definition.syntax_version != SYNTAX_VERSION:
                    raise ValueError("formula definition syntax is unsupported")
                self.definitions._reject_rule_name(base_name)
                definitions.append(definition)
            if names != sorted(entry.name for entry in os.scandir(directory)):
                raise ValueError("formula definition catalog changed during preflight")
        finally:
            os.close(directory)
        values = tuple(definitions)
        identity = canonical_sha256(
            [(definition.pool_name, definition.version) for definition in values]
        )
        return values, identity

    def run(
        self, trade_date: date, *, limit: int = _MAX_PAGE, cursor: str | None = None
    ) -> FormulaPoolBatchResult:
        if type(limit) is not int or not 1 <= limit <= _MAX_PAGE:
            raise ValueError("formula batch page limit must be between 1 and 64")
        catalog, catalog_identity = self._catalog()
        # Even an empty catalog must have an available, closed target day.
        universe, projection, _ = self.runner._sources(trade_date)
        cursor_identity = canonical_sha256(
            {
                "trade_date": trade_date,
                "catalog_identity": catalog_identity,
                "universe_identity": universe,
                "projection_identity": projection,
            }
        )
        start = 0
        if cursor is not None:
            match = _CURSOR.fullmatch(cursor)
            if match is None or match.group(1) != cursor_identity:
                raise ValueError("formula batch cursor is invalid or stale")
            start = int(match.group(2))
            if start >= len(catalog):
                raise ValueError("formula batch cursor is outside catalog")
        end = min(start + limit, len(catalog))
        results = tuple(self._process(definition, trade_date) for definition in catalog[start:end])
        if self._catalog()[1] != catalog_identity:
            raise ValueError("formula definition catalog changed during batch")
        if self.runner._sources(trade_date)[:2] != (universe, projection) or any(
            item.daily is not None
            and (item.daily.universe_identity, item.daily.projection_identity)
            != (universe, projection)
            for item in results
        ):
            raise ValueError("formula batch sources changed during reconciliation")
        next_cursor = f"{cursor_identity}:{end}" if end < len(catalog) else None
        completed = sum(item.status == "completed" for item in results)
        waiting = sum(item.status == "waiting" for item in results)
        failed = sum(item.status == "failed" for item in results)
        return FormulaPoolBatchResult(
            trade_date=trade_date,
            catalog_identity=catalog_identity,
            total_count=len(catalog),
            page_start=start,
            pools=results,
            completed_count=completed,
            waiting_count=waiting,
            failed_count=failed,
            unprocessed_count=len(catalog) - len(results),
            next_cursor=next_cursor,
            all_complete=start == 0 and completed == len(catalog),
        )

    def _process(
        self, definition: FormulaPoolDefinitionV1, trade_date: date
    ) -> FormulaPoolBatchItem:
        base_name = definition.pool_name.removeprefix("user/")
        try:
            receipt = self.runner.admit(base_name, definition.version, trade_date)
        except FormulaMarketJobActiveError:
            return FormulaPoolBatchItem(
                pool_name=definition.pool_name,
                definition_version=definition.version,
                status="waiting",
            )
        except (OSError, RuntimeError, ValueError):
            return FormulaPoolBatchItem(
                pool_name=definition.pool_name,
                definition_version=definition.version,
                status="failed",
                error_code="admission_rejected",
            )
        if receipt.status in {"queued", "running"}:
            return FormulaPoolBatchItem(
                pool_name=definition.pool_name,
                definition_version=definition.version,
                status="waiting",
                task_id=receipt.task_id,
            )
        if receipt.status == "failed":
            return FormulaPoolBatchItem(
                pool_name=definition.pool_name,
                definition_version=definition.version,
                status="failed",
                task_id=receipt.task_id,
                error_code=receipt.error_code or "task_failed",
            )
        try:
            daily = self.runner.publish(base_name, definition.version, trade_date)
        except (OSError, RuntimeError, ValueError):
            return FormulaPoolBatchItem(
                pool_name=definition.pool_name,
                definition_version=definition.version,
                status="failed",
                task_id=receipt.task_id,
                error_code="publication_rejected",
            )
        return FormulaPoolBatchItem(
            pool_name=definition.pool_name,
            definition_version=definition.version,
            status="completed",
            task_id=receipt.task_id,
            daily=FormulaPoolBatchDailyProof.from_daily(daily),
        )


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Reconcile a bounded local formula pool day batch")
    parser.add_argument("--config", required=True, type=Path)
    parser.add_argument("--trade-date", required=True)
    parser.add_argument("--limit", type=int, default=_MAX_PAGE)
    parser.add_argument("--cursor")
    args = parser.parse_args(list(argv) if argv is not None else None)
    try:
        config = load_private_formula_pool_batch_config(args.config)
        target = date.fromisoformat(args.trade_date)
    except (OSError, ValueError):
        print("formula_batch_config_invalid", file=sys.stderr)
        return 2
    try:
        result = FormulaPoolBatchCoordinator(config=config).run(
            target, limit=args.limit, cursor=args.cursor
        )
    except (OSError, RuntimeError, ValueError):
        print("formula_batch_unavailable", file=sys.stderr)
        return 1
    sys.stdout.buffer.write(
        canonical_json_bytes(result.model_dump(mode="json"), trailing_newline=True)
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
