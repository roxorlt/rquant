"""Trusted one-day formula pool recalculation and immutable result evidence."""

from __future__ import annotations

import os
import stat
from collections.abc import Callable
from contextlib import suppress
from datetime import UTC, date, datetime, time
from pathlib import Path
from typing import Literal
from uuid import uuid4
from zoneinfo import ZoneInfo

from pydantic import Field, model_validator

from rquant.formula_market_private_config import FormulaMarketPrivateConfig
from rquant.formula_pool_definition import (
    _NAME,
    FormulaPoolDefinitionStore,
    FormulaPoolDefinitionV1,
    _canonical_path,
    _file_identity,
    _open_private_directory,
)
from rquant.runtime_contracts import RuntimeContractModel, canonical_sha256, normalize_aware_utc
from rquant.screen.formula_history_projection import VerifiedFormulaHistoryProjection
from rquant.screen.formula_market_jobs import (
    FormulaMarketJobReceipt,
    FormulaMarketJobRequest,
    FormulaMarketJobResult,
    FormulaMarketJobStore,
    FormulaMarketJobWorker,
)
from rquant.screen.formula_market_universe import (
    load_formula_market_universe,
    peek_formula_market_universe_identity,
)
from rquant.screen.tdx.ast import SYNTAX_VERSION
from rquant.strict_json import canonical_json_bytes, strict_canonical_json_loads

_SHANGHAI = ZoneInfo("Asia/Shanghai")
_MAX_DAILY_BYTES = 2 * 1024 * 1024
_READ_FLAGS = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
_WRITE_FLAGS = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0)


class FormulaPoolDailyResultV1(RuntimeContractModel):
    """One definition version evaluated on one day's two fixed source generations."""

    schema_version: Literal[1] = 1
    pool_name: str = Field(min_length=6, max_length=85)
    definition_version: str = Field(pattern=r"^[0-9a-f]{64}$")
    trade_date: date
    run_identity: str = Field(pattern=r"^[0-9a-f]{64}$")
    task_id: str = Field(pattern=r"^[0-9a-f]{32}$")
    request_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    result_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    universe_identity: str = Field(pattern=r"^[0-9a-f]{64}$")
    projection_identity: str = Field(pattern=r"^[0-9a-f]{64}$")
    market_total: int = Field(ge=0, strict=True)
    match_count: int = Field(ge=0, strict=True)
    no_match_count: int = Field(ge=0, strict=True)
    unknown_count: int = Field(ge=0, strict=True)
    unknown_reasons: dict[str, int]
    match_codes: tuple[str, ...]
    member_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    content_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")

    @model_validator(mode="after")
    def validate_daily_result(self) -> FormulaPoolDailyResultV1:
        if (
            not self.pool_name.startswith("user/")
            or _NAME.fullmatch(self.pool_name.removeprefix("user/")) is None
            or self.market_total != self.match_count + self.no_match_count + self.unknown_count
            or len(self.match_codes) != self.match_count
            or tuple(sorted(set(self.match_codes))) != self.match_codes
            or any(type(count) is not int or count < 0 for count in self.unknown_reasons.values())
            or sum(self.unknown_reasons.values()) != self.unknown_count
            or self.member_sha256 != canonical_sha256(self.match_codes)
            or self.run_identity
            != _run_identity(
                self.pool_name,
                self.definition_version,
                self.trade_date,
                self.universe_identity,
                self.projection_identity,
            )
            or self.content_sha256
            != canonical_sha256(self.model_dump(mode="python", exclude={"content_sha256"}))
        ):
            raise ValueError("formula pool daily result is inconsistent")
        return self

    @classmethod
    def create(
        cls,
        *,
        definition: FormulaPoolDefinitionV1,
        request: FormulaMarketJobRequest,
        result: FormulaMarketJobResult,
    ) -> FormulaPoolDailyResultV1:
        summary = result.summary
        content = {
            "schema_version": 1,
            "pool_name": definition.pool_name,
            "definition_version": definition.version,
            "trade_date": request.trade_date,
            "run_identity": request.idempotency_key,
            "task_id": result.task_id,
            "request_sha256": result.request_sha256,
            "result_sha256": result.content_sha256,
            "universe_identity": request.expected_universe_sha256,
            "projection_identity": request.expected_projection_identity,
            "market_total": summary.market_total,
            "match_count": summary.match_count,
            "no_match_count": summary.no_match_count,
            "unknown_count": summary.unknown_count,
            "unknown_reasons": summary.unknown_reasons,
            "match_codes": summary.match_codes,
            "member_sha256": canonical_sha256(summary.match_codes),
        }
        return cls(**content, content_sha256=canonical_sha256(content))


def _run_identity(
    pool_name: str,
    definition_version: str,
    trade_date: date,
    universe_identity: str,
    projection_identity: str,
) -> str:
    return canonical_sha256(
        {
            "contract": "formula-pool-daily/v1",
            "pool_name": pool_name,
            "definition_version": definition_version,
            "trade_date": trade_date,
            "universe_identity": universe_identity,
            "projection_identity": projection_identity,
        }
    )


def _read_daily_file(directory: int, name: str) -> bytes:
    before = os.stat(name, dir_fd=directory, follow_symlinks=False)
    if (
        not stat.S_ISREG(before.st_mode)
        or before.st_uid != os.geteuid()
        or stat.S_IMODE(before.st_mode) != 0o600
        or before.st_nlink != 1
        or not 0 < before.st_size <= _MAX_DAILY_BYTES
    ):
        raise ValueError("formula pool daily result file is unsafe")
    descriptor = os.open(name, _READ_FLAGS, dir_fd=directory)
    try:
        if _file_identity(os.fstat(descriptor)) != _file_identity(before):
            raise ValueError("formula pool daily result changed while opening")
        remaining = before.st_size
        chunks: list[bytes] = []
        while remaining:
            chunk = os.read(descriptor, min(remaining, 1024 * 1024))
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        if (
            remaining
            or _file_identity(os.fstat(descriptor)) != _file_identity(before)
            or _file_identity(os.stat(name, dir_fd=directory, follow_symlinks=False))
            != _file_identity(before)
        ):
            raise ValueError("formula pool daily result changed while reading")
        return b"".join(chunks)
    finally:
        os.close(descriptor)


class FormulaPoolDailyResultStore:
    """A private per-pool directory with one exclusive, immutable file per day."""

    def __init__(self, root: Path) -> None:
        self.root = _canonical_path(root)

    def _pool_dir(self, definition: FormulaPoolDefinitionV1) -> Path:
        return self.root / definition.pool_name.removeprefix("user/")

    def read(
        self, definition: FormulaPoolDefinitionV1, trade_date: date
    ) -> FormulaPoolDailyResultV1:
        root = _open_private_directory(self.root, create=False)
        try:
            directory = _open_private_directory(self._pool_dir(definition), create=False)
            try:
                payload = _read_daily_file(directory, f"{trade_date.isoformat()}.json")
            finally:
                os.close(directory)
        finally:
            os.close(root)
        daily = FormulaPoolDailyResultV1.model_validate(strict_canonical_json_loads(payload))
        if (
            canonical_json_bytes(daily.model_dump(mode="json")) != payload
            or daily.pool_name != definition.pool_name
            or daily.definition_version != definition.version
            or daily.trade_date != trade_date
        ):
            raise ValueError("formula pool daily result does not match its file")
        return daily

    def create(self, daily: FormulaPoolDailyResultV1) -> None:
        daily = FormulaPoolDailyResultV1.model_validate(daily)
        payload = canonical_json_bytes(daily.model_dump(mode="json"))
        if not 0 < len(payload) <= _MAX_DAILY_BYTES:
            raise ValueError("formula pool daily result exceeds byte budget")
        root = _open_private_directory(self.root, create=True)
        try:
            pool_path = self.root / daily.pool_name.removeprefix("user/")
            directory = _open_private_directory(pool_path, create=True)
            try:
                stage = f".{daily.trade_date.isoformat()}.{uuid4().hex}.stage"
                try:
                    descriptor = os.open(stage, _WRITE_FLAGS, 0o600, dir_fd=directory)
                    try:
                        os.fchmod(descriptor, 0o600)
                        view = memoryview(payload)
                        while view:
                            written = os.write(descriptor, view)
                            if written <= 0:
                                raise OSError("formula pool daily write made no progress")
                            view = view[written:]
                        os.fsync(descriptor)
                    finally:
                        os.close(descriptor)
                    os.link(
                        stage,
                        f"{daily.trade_date.isoformat()}.json",
                        src_dir_fd=directory,
                        dst_dir_fd=directory,
                        follow_symlinks=False,
                    )
                    os.unlink(stage, dir_fd=directory)
                    os.fsync(directory)
                finally:
                    with suppress(FileNotFoundError):
                        os.unlink(stage, dir_fd=directory)
            finally:
                os.close(directory)
        finally:
            os.close(root)


class FormulaPoolDailyRecalculator:
    """Admit, finish, and verify a single saved formula on a closed market day."""

    def __init__(
        self,
        *,
        config: FormulaMarketPrivateConfig,
        task_store: FormulaMarketJobStore,
        definitions: FormulaPoolDefinitionStore,
        result_root: Path,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self.config = FormulaMarketPrivateConfig.model_validate(config)
        if (
            task_store.state_path != self.config.state_path
            or task_store.artifact_directory != self.config.artifact_directory
        ):
            raise ValueError("formula pool task store differs from trusted configuration")
        self.task_store = task_store
        self.definitions = definitions
        self.results = FormulaPoolDailyResultStore(result_root)
        self.clock = clock or (lambda: datetime.now(UTC))

    def _sources(self, trade_date: date) -> tuple[str, str, datetime]:
        if type(trade_date) is not date:
            raise ValueError("formula pool trade date is invalid")
        now = normalize_aware_utc(self.clock())
        closed_at = datetime.combine(trade_date, time(17), tzinfo=_SHANGHAI)
        if now < closed_at.astimezone(UTC):
            raise ValueError("formula pool date has not closed at 17:00 Shanghai time")
        try:
            universe = peek_formula_market_universe_identity(self.config.universe_root, trade_date)
            captured = load_formula_market_universe(
                self.config.universe_root, trade_date, expected_sha256=universe
            )
            projection = VerifiedFormulaHistoryProjection(self.config.projection_root)
            catalog = projection.catalog()
            verified = projection.require_open_day(trade_date, expected_identity=catalog.identity)
            projection.require_day_bars_for_codes(
                trade_date,
                tuple(entry.ts_code for entry in captured.entries),
                expected_identity=verified.identity,
            )
            if verified.updated_at > now:
                raise ValueError("formula pool target day history is unavailable")
            if (
                peek_formula_market_universe_identity(self.config.universe_root, trade_date)
                != universe
                or projection.catalog().identity != verified.identity
            ):
                raise ValueError("formula pool sources changed during admission")
            return universe, verified.identity, now
        except (OSError, RuntimeError, ValueError) as exc:
            raise ValueError("formula pool target day sources are unavailable or changed") from exc

    @staticmethod
    def _require_syntax(definition: FormulaPoolDefinitionV1) -> None:
        if definition.syntax_version != SYNTAX_VERSION:
            raise ValueError("formula pool syntax version is unsupported")

    def _definition(self, base_name: str, version: str) -> FormulaPoolDefinitionV1:
        definition = self.definitions.read(base_name, expected_version=version)
        self._require_syntax(definition)
        return definition

    def _verified_task(
        self,
        *,
        task_id: str,
        definition: FormulaPoolDefinitionV1,
        trade_date: date,
        universe_identity: str,
        projection_identity: str,
    ) -> tuple[FormulaMarketJobRequest, FormulaMarketJobResult]:
        request, result = self.task_store.read_succeeded_task(task_id)
        summary = result.summary
        if (
            request.idempotency_key
            != _run_identity(
                definition.pool_name,
                definition.version,
                trade_date,
                universe_identity,
                projection_identity,
            )
            or request.formula != definition.formula
            or request.trade_date != trade_date
            or request.universe_root != self.config.universe_root
            or request.projection_root != self.config.projection_root
            or request.expected_universe_sha256 != universe_identity
            or request.expected_projection_identity != projection_identity
            or summary.trade_date != trade_date
            or summary.universe_identity != universe_identity
            or summary.projection_identity != projection_identity
        ):
            raise ValueError("formula pool daily task differs from definition or sources")
        return request, result

    def read_exact(
        self, base_name: str, definition_version: str, trade_date: date
    ) -> FormulaPoolDailyResultV1:
        definition = self.definitions.read(base_name, expected_version=definition_version)
        daily = self.results.read(definition, trade_date)
        request, result = self._verified_task(
            task_id=daily.task_id,
            definition=definition,
            trade_date=trade_date,
            universe_identity=daily.universe_identity,
            projection_identity=daily.projection_identity,
        )
        expected = FormulaPoolDailyResultV1.create(
            definition=definition,
            request=request,
            result=result,
        )
        if expected != daily:
            raise ValueError("formula pool daily result disagrees with sealed task")
        return daily

    def admit(
        self, base_name: str, definition_version: str, trade_date: date
    ) -> FormulaMarketJobReceipt:
        definition = self._definition(base_name, definition_version)
        universe, projection, now = self._sources(trade_date)
        run_identity = _run_identity(
            definition.pool_name, definition.version, trade_date, universe, projection
        )
        try:
            existing_daily = self.read_exact(base_name, definition_version, trade_date)
        except FileNotFoundError:
            existing_daily = None
        if existing_daily is not None and existing_daily.run_identity != run_identity:
            raise ValueError("formula pool daily result conflicts with current sources")
        existing_task = self.task_store.admission_by_key(run_identity)
        if existing_task is not None:
            request, task_id = existing_task
            if (
                request.formula != definition.formula
                or request.trade_date != trade_date
                or request.universe_root != self.config.universe_root
                or request.projection_root != self.config.projection_root
                or request.expected_universe_sha256 != universe
                or request.expected_projection_identity != projection
            ):
                raise ValueError("formula pool daily task identity conflicts")
            return self.task_store.status(task_id)
        if existing_daily is not None:
            raise ValueError("formula pool daily result has no matching task")
        request = FormulaMarketJobRequest(
            idempotency_key=run_identity,
            formula=definition.formula,
            trade_date=trade_date,
            decision_at=now,
            universe_root=self.config.universe_root,
            projection_root=self.config.projection_root,
            expected_universe_sha256=universe,
            expected_projection_identity=projection,
        )
        return self.task_store.submit(request)

    def publish(
        self, base_name: str, definition_version: str, trade_date: date
    ) -> FormulaPoolDailyResultV1:
        definition = self._definition(base_name, definition_version)
        universe, projection, _ = self._sources(trade_date)
        run_identity = _run_identity(
            definition.pool_name, definition.version, trade_date, universe, projection
        )
        try:
            existing = self.read_exact(base_name, definition_version, trade_date)
        except FileNotFoundError:
            existing = None
        if existing is not None:
            if existing.run_identity != run_identity:
                raise ValueError("formula pool daily result conflicts with current sources")
            return existing
        admitted = self.task_store.admission_by_key(run_identity)
        if admitted is None:
            raise ValueError("formula pool daily task was not admitted")
        _, task_id = admitted
        request, result = self._verified_task(
            task_id=task_id,
            definition=definition,
            trade_date=trade_date,
            universe_identity=universe,
            projection_identity=projection,
        )
        daily = FormulaPoolDailyResultV1.create(
            definition=definition, request=request, result=result
        )
        if self._sources(trade_date)[:2] != (universe, projection):
            raise ValueError("formula pool sources changed before daily publication")
        try:
            self.results.create(daily)
        except FileExistsError:
            existing = self.read_exact(base_name, definition_version, trade_date)
            if existing != daily:
                raise ValueError(
                    "formula pool daily result conflicts with existing evidence"
                ) from None
            return existing
        return self.read_exact(base_name, definition_version, trade_date)

    def run_one(
        self, base_name: str, definition_version: str, trade_date: date
    ) -> FormulaPoolDailyResultV1:
        """Run at most the one admitted task, then publish only its verified success."""
        admitted = self.admit(base_name, definition_version, trade_date)
        if admitted.status in {"queued", "running"}:
            completed = FormulaMarketJobWorker(
                self.task_store,
                trusted_source_roots=(self.config.universe_root, self.config.projection_root),
            ).run_one()
            if completed is None:
                completed = self.task_store.status(admitted.task_id)
            if completed.task_id != admitted.task_id or completed.status in {"queued", "running"}:
                raise ValueError("formula pool daily task is held by another worker")
        return self.publish(base_name, definition_version, trade_date)
