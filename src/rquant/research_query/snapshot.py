"""Offline exact-column publication and pinned public query snapshot."""

from __future__ import annotations

import hashlib
import os
import stat
from datetime import datetime
from pathlib import Path
from typing import Literal
from uuid import uuid4

from pydantic import Field

from .contracts import QueryModel

PUBLIC_SCHEMA: dict[str, tuple[tuple[str, str], ...]] = {
    "daily_bar": (
        ("ts_code", "VARCHAR"),
        ("trade_date", "DATE"),
        ("open", "DOUBLE"),
        ("high", "DOUBLE"),
        ("low", "DOUBLE"),
        ("close", "DOUBLE"),
        ("vol", "DOUBLE"),
        ("amount", "DOUBLE"),
    ),
    "adj_factor": (("ts_code", "VARCHAR"), ("trade_date", "DATE"), ("adj_factor", "DOUBLE")),
    "trade_calendar": (
        ("exchange", "VARCHAR"),
        ("cal_date", "DATE"),
        ("is_open", "BOOLEAN"),
        ("pretrade_date", "DATE"),
    ),
}


class QuerySnapshotManifest(QueryModel):
    contract: Literal["research-query-public-v1"] = "research-query-public-v1"
    filename: str = Field(pattern=r"^query-[0-9a-f]{64}\.duckdb$")
    file_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    source_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    source_identity: tuple[int, int, int, int]
    source_at: datetime
    built_at: datetime
    row_counts: dict[str, int]
    date_ranges: dict[str, tuple[str | None, str | None]]


def file_identity(path: Path, *, readonly: bool = False) -> tuple[int, int, int, int]:
    if not path.is_absolute() or path != path.resolve() or ".." in path.parts:
        raise ValueError("查询来源路径未通过核验")
    try:
        info = path.lstat()
    except OSError as exc:
        raise ValueError("查询来源不可用") from exc
    if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
        raise ValueError("查询来源必须为独立普通文件")
    if readonly and stat.S_IMODE(info.st_mode) != 0o400:
        raise ValueError("查询快照必须只读")
    return info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns


def digest_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def verify_public_schema(connection: object) -> None:
    tables = connection.execute(
        "SELECT schema_name,table_name FROM duckdb_tables() WHERE NOT internal ORDER BY table_name"
    ).fetchall()
    if tables != [("main", name) for name in sorted(PUBLIC_SCHEMA)]:
        raise ValueError("查询快照包含额外或缺失数据表")
    for table, columns in PUBLIC_SCHEMA.items():
        actual = connection.execute(
            "SELECT column_name,data_type FROM information_schema.columns "
            "WHERE table_schema='main' AND table_name=? ORDER BY ordinal_position",
            [table],
        ).fetchall()
        if actual != list(columns):
            raise ValueError("查询快照字段不匹配")
    for sql in (
        "SELECT count(*) FROM duckdb_views() WHERE NOT internal",
        "SELECT count(*) FROM duckdb_functions() WHERE NOT internal",
        "SELECT count(*) FROM duckdb_sequences()",
        "SELECT count(*) FROM duckdb_types() WHERE NOT internal",
        "SELECT count(*) FROM duckdb_indexes()",
    ):
        if connection.execute(sql).fetchone()[0]:
            raise ValueError("查询快照包含额外对象")
    schemas = connection.execute(
        "SELECT schema_name FROM duckdb_schemas() WHERE NOT internal"
    ).fetchall()
    if any(row[0] != "main" for row in schemas):
        raise ValueError("查询快照包含额外 schema")


def _readonly(path: Path) -> object:
    import duckdb

    return duckdb.connect(
        str(path),
        read_only=True,
        config={"enable_external_access": False, "threads": 1, "memory_limit": "512MiB"},
    )


class VerifiedQuerySnapshot:
    def __init__(self, root: Path) -> None:
        self.root = Path(root)
        if self.root != self.root.resolve() or not self.root.is_absolute():
            raise ValueError("查询目录路径未通过核验")
        info = self.root.lstat()
        if (
            not stat.S_ISDIR(info.st_mode)
            or stat.S_IMODE(info.st_mode) != 0o700
            or info.st_uid != os.geteuid()
        ):
            raise ValueError("查询目录权限未通过核验")
        self.root_identity = (info.st_dev, info.st_ino)
        self.manifest_path = self.root / "manifest.json"
        self.manifest_identity = file_identity(self.manifest_path, readonly=True)
        if self.manifest_identity[2] > 16 * 1024:
            raise ValueError("查询清单过长")
        self.manifest = QuerySnapshotManifest.model_validate_json(self.manifest_path.read_bytes())
        self.path = self.root / self.manifest.filename
        if self.manifest.filename != f"query-{self.manifest.file_sha256}.duckdb":
            raise ValueError("查询文件名称与摘要不符")
        self.identity = file_identity(self.path, readonly=True)
        if digest_file(self.path) != self.manifest.file_sha256:
            raise ValueError("查询来源摘要不符")
        self.verify_current()

    def verify_current(self) -> None:
        info = self.root.lstat()
        if (info.st_dev, info.st_ino) != self.root_identity or stat.S_IMODE(info.st_mode) != 0o700:
            raise ValueError("查询目录发生变化")
        if (
            file_identity(self.path, readonly=True) != self.identity
            or file_identity(self.manifest_path, readonly=True) != self.manifest_identity
        ):
            raise ValueError("查询来源发生变化，请重新核验")
        with _readonly(self.path) as connection:
            verify_public_schema(connection)
        if file_identity(self.path, readonly=True) != self.identity:
            raise ValueError("查询来源核验期间发生变化")


def build_query_snapshot(
    source: Path, root: Path, *, source_sha256: str, source_at: datetime
) -> QuerySnapshotManifest:
    """Trusted offline CLI publishes columns only after source byte verification."""
    from datetime import UTC

    import duckdb

    if source_at.tzinfo is None:
        raise ValueError("数据时刻必须带时区")
    before = file_identity(source)
    if digest_file(source) != source_sha256 or file_identity(source) != before:
        raise ValueError("原来源摘要或身份不匹配")
    root = Path(root)
    if not root.is_absolute() or root != root.resolve() or root == source.parent:
        raise ValueError("公开查询目录必须独立且路径可核验")
    root.mkdir(mode=0o700, parents=True, exist_ok=True)
    if stat.S_IMODE(root.lstat().st_mode) != 0o700 or root.lstat().st_uid != os.geteuid():
        raise ValueError("公开查询目录必须私有")
    scratch = root / f".query-{uuid4().hex}.duckdb"
    try:
        counts: dict[str, int] = {}
        ranges: dict[str, tuple[str | None, str | None]] = {}
        with (
            _readonly(source) as original,
            duckdb.connect(
                str(scratch),
                config={"threads": 1, "memory_limit": "512MiB", "enable_external_access": False},
            ) as target,
        ):
            target.execute("BEGIN TRANSACTION")
            for table, columns in PUBLIC_SCHEMA.items():
                actual = dict(
                    original.execute(
                        "SELECT column_name,data_type FROM information_schema.columns "
                        "WHERE table_schema='main' AND table_name=?",
                        [table],
                    ).fetchall()
                )
                if any(actual.get(name) != kind for name, kind in columns):
                    raise ValueError("原数据字段合同不匹配")
                definition = ",".join(name + " " + kind for name, kind in columns)
                target.execute(f"CREATE TABLE {table} ({definition})")
                cursor = original.execute(
                    f"SELECT {','.join(name for name, _ in columns)} FROM {table}"
                )
                count = 0
                while batch := cursor.fetchmany(1000):
                    count += len(batch)
                    if count > 100_000_000:
                        raise ValueError("公开查询来源超过容量")
                    target.executemany(
                        f"INSERT INTO {table} VALUES ({','.join('?' for _ in columns)})", batch
                    )
                counts[table] = count
                field = "cal_date" if table == "trade_calendar" else "trade_date"
                earliest, latest = target.execute(
                    f"SELECT min({field}),max({field}) FROM {table}"
                ).fetchone()
                ranges[table] = (
                    str(earliest) if earliest else None,
                    str(latest) if latest else None,
                )
            verify_public_schema(target)
            target.execute("COMMIT")
            target.execute("CHECKPOINT")
        if (
            file_identity(source) != before
            or digest_file(source) != source_sha256
            or file_identity(source) != before
        ):
            raise ValueError("原来源在发布期间发生变化")
        digest = digest_file(scratch)
        filename = f"query-{digest}.duckdb"
        os.chmod(scratch, 0o400)
        os.replace(scratch, root / filename)
        manifest = QuerySnapshotManifest(
            filename=filename,
            file_sha256=digest,
            source_sha256=source_sha256,
            source_identity=before,
            source_at=source_at,
            built_at=datetime.now(UTC),
            row_counts=counts,
            date_ranges=ranges,
        )
        pending = root / f".manifest-{uuid4().hex}.json"
        with pending.open("x", encoding="utf-8") as handle:
            handle.write(manifest.model_dump_json())
            handle.flush()
            os.fsync(handle.fileno())
        os.chmod(pending, 0o400)
        os.replace(pending, root / "manifest.json")
        VerifiedQuerySnapshot(root)
        return manifest
    finally:
        scratch.unlink(missing_ok=True)
        Path(str(scratch) + ".wal").unlink(missing_ok=True)
