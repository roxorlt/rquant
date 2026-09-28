"""Verify formula pool Serving rows and their exact bounded daily result."""

from __future__ import annotations

import os
import stat
from datetime import date, datetime
from pathlib import Path
from typing import Literal

from rquant.formula_pool_daily import FormulaPoolDailyResultV1
from rquant.formula_pool_definition import _file_identity
from rquant.formula_pool_serving_projection import (
    FORMULA_POOL_PROJECTION_TABLES,
    FormulaPoolDefinitionRow,
    FormulaPoolLatestResultRow,
    FormulaPoolStateRow,
    validate_formula_pool_projections,
)
from rquant.serving_read_models import PAGE_PROJECTION_CONTRACTS, ServingProjectionInput
from rquant.strict_json import canonical_json_bytes, strict_canonical_json_loads
from rquant.web.serving import BorrowedGeneration

Availability = Literal["unavailable", "not_published", "empty", "ready"]
_TABLES = tuple(sorted(FORMULA_POOL_PROJECTION_TABLES))
_MAX_DAILY_BYTES = 2 * 1024 * 1024
_DIR_FLAGS = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)
_READ_FLAGS = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)


def read_formula_pool_snapshot(
    borrowed: BorrowedGeneration | None,
) -> tuple[
    Availability,
    datetime | None,
    tuple[FormulaPoolDefinitionRow, ...],
    tuple[FormulaPoolLatestResultRow, ...],
]:
    """Read only the request's borrowed generation; malformed groups cannot become empty."""
    if borrowed is None:
        return "unavailable", None, (), ()
    physical = FORMULA_POOL_PROJECTION_TABLES & borrowed.manifest.row_counts.keys()
    if physical and physical != FORMULA_POOL_PROJECTION_TABLES:
        raise ValueError("formula pool physical group is incomplete")
    marks = borrowed.cursor.execute(
        "SELECT table_name, available, row_count, owner_dataset_id, "
        "owner_generation_id, available_at FROM projection_status "
        f"WHERE table_name IN ({', '.join('?' for _ in _TABLES)}) ORDER BY table_name LIMIT ?",
        (*_TABLES, len(_TABLES) + 1),
    ).fetchall()
    if not marks:
        if physical:
            raise ValueError("formula pool status is missing from physical generation")
        return "not_published", None, (), ()
    if len(marks) != len(_TABLES) or tuple(row[0] for row in marks) != _TABLES:
        raise ValueError("formula pool status group is incomplete")
    if any(
        type(available) is not bool or type(count) is not int for _, available, count, *_ in marks
    ):
        raise ValueError("formula pool status types are invalid")
    if not any(row[1] for row in marks):
        if any(
            count
            or borrowed.manifest.row_counts.get(str(name), 0)
            or owner != "signals"
            or generation is not None
            or at is not None
            for name, _, count, owner, generation, at in marks
        ):
            raise ValueError("unpublished formula pool status is inconsistent")
        return "not_published", None, (), ()
    generation = borrowed.manifest.source_generations.get("signals")
    watermark = next(
        (item for item in borrowed.manifest.watermarks if item.dataset_id == "signals"), None
    )
    available_at = marks[0][5]
    if generation is None or watermark is None or generation != watermark.generation_id:
        raise ValueError("formula pool source generation is absent")
    if not isinstance(available_at, datetime) or any(
        not available
        or owner != "signals"
        or owner_generation != generation
        or at != available_at
        or at > borrowed.manifest.built_at
        or count != borrowed.manifest.row_counts.get(str(name))
        or not 0 <= count <= PAGE_PROJECTION_CONTRACTS[str(name)].max_rows
        for name, available, count, owner, owner_generation, at in marks
    ):
        raise ValueError("formula pool status differs from Serving generation")
    projections: dict[str, ServingProjectionInput] = {}
    for name, _, count, *_ in marks:
        contract = PAGE_PROJECTION_CONTRACTS[name]
        columns = contract.column_names
        raw = borrowed.cursor.execute(
            f"SELECT {', '.join(columns)} FROM {name} "
            f"ORDER BY {', '.join(contract.sort_keys)} LIMIT ?",
            (contract.max_rows + 1,),
        ).fetchall()
        if len(raw) != count:
            raise ValueError("formula pool row count differs from manifest")
        rows = tuple(
            {
                column: value.isoformat() if isinstance(value, (date, datetime)) else value
                for column, value in zip(columns, values, strict=True)
            }
            for values in raw
        )
        projections[name] = ServingProjectionInput(
            table_name=name,
            available_at=available_at,
            rows=rows,
            owner_dataset_id="signals",
            owner_generation_id=generation,
        )
    validate_formula_pool_projections(projections)
    state = FormulaPoolStateRow.model_validate(projections["formula_pool_state"].rows[0])
    definitions = tuple(
        FormulaPoolDefinitionRow.model_validate(item)
        for item in projections["formula_pool_definition"].rows
    )
    latest = tuple(
        FormulaPoolLatestResultRow.model_validate(item)
        for item in projections["formula_pool_latest_result"].rows
    )
    return state.availability, available_at, definitions, latest


def _open_checked_dir(path: Path, *, parent: int | None = None) -> int:
    before = (
        path.lstat() if parent is None else os.stat(path.name, dir_fd=parent, follow_symlinks=False)
    )
    if not stat.S_ISDIR(before.st_mode) or before.st_mode & 0o022:
        raise ValueError("formula pool daily directory is unsafe")
    descriptor = os.open(path if parent is None else path.name, _DIR_FLAGS, dir_fd=parent)
    if _file_identity(os.fstat(descriptor)) != _file_identity(before):
        os.close(descriptor)
        raise ValueError("formula pool daily directory changed while opening")
    return descriptor


def read_indexed_daily_result(
    root: Path, index: FormulaPoolLatestResultRow
) -> FormulaPoolDailyResultV1:
    """Read one controlled path, then compare every indexed identity and digest."""
    root = Path(root)
    if not root.is_absolute() or ".." in root.parts or root.resolve(strict=False) != root:
        raise ValueError("formula pool daily root is not canonical")
    index = FormulaPoolLatestResultRow.model_validate(index)
    base_name = index.pool_name.removeprefix("user/")
    root_fd = _open_checked_dir(root)
    try:
        pool_fd = _open_checked_dir(root / base_name, parent=root_fd)
        try:
            name = f"{index.trade_date.isoformat()}.json"
            before = os.stat(name, dir_fd=pool_fd, follow_symlinks=False)
            if (
                not stat.S_ISREG(before.st_mode)
                or before.st_mode & 0o022
                or before.st_nlink != 1
                or not 0 < before.st_size <= _MAX_DAILY_BYTES
                or before.st_size != index.byte_count
            ):
                raise ValueError("formula pool daily file is unsafe")
            descriptor = os.open(name, _READ_FLAGS, dir_fd=pool_fd)
            try:
                if _file_identity(os.fstat(descriptor)) != _file_identity(before):
                    raise ValueError("formula pool daily file changed while opening")
                payload = os.read(descriptor, _MAX_DAILY_BYTES + 1)
                if (
                    len(payload) != before.st_size
                    or _file_identity(os.fstat(descriptor)) != _file_identity(before)
                    or _file_identity(os.stat(name, dir_fd=pool_fd, follow_symlinks=False))
                    != _file_identity(before)
                ):
                    raise ValueError("formula pool daily file changed while reading")
            finally:
                os.close(descriptor)
        finally:
            os.close(pool_fd)
    finally:
        os.close(root_fd)
    daily = FormulaPoolDailyResultV1.model_validate(strict_canonical_json_loads(payload))
    if (
        canonical_json_bytes(daily.model_dump(mode="json")) != payload
        or daily.pool_name != index.pool_name
        or daily.definition_version != index.definition_version
        or daily.trade_date != index.trade_date
        or daily.task_id != index.task_id
        or daily.request_sha256 != index.request_sha256
        or daily.result_sha256 != index.result_sha256
        or daily.universe_identity != index.universe_identity
        or daily.projection_identity != index.projection_identity
        or daily.market_total != index.market_total
        or daily.match_count != index.match_count
        or daily.no_match_count != index.no_match_count
        or daily.unknown_count != index.unknown_count
        or canonical_json_bytes(daily.unknown_reasons).decode() != index.unknown_reasons_json
        or daily.member_sha256 != index.member_sha256
        or daily.content_sha256 != index.content_sha256
    ):
        raise ValueError("formula pool daily file differs from its Serving index")
    return daily
