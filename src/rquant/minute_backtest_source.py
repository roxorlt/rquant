"""Restore exact bounded input bytes into a new private replay workspace."""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import stat
from dataclasses import dataclass
from io import BytesIO
from pathlib import Path
from typing import TYPE_CHECKING

import duckdb
import pandas as pd
import pyarrow.parquet as parquet

from rquant.live_contracts import LiveChannel
from rquant.feature_spool import FeatureBatchSpool
from rquant.live_spool import LiveBatchSpool
from rquant.market_minute_gateway import MarketMinuteGateway
from rquant.intraday_feature_engine import INPUT_COLUMNS
from rquant.minute_backtest_contracts import (
    MAX_INPUT_BYTES,
    MAX_WORK_UNITS,
    MINUTE_RUNTIME_INPUT_TABLE,
    FrozenMinuteRuntimeInput,
    MinuteReplayConstraintPublication,
    MinuteReplayWork,
    MinuteRuntimeSourceReceipt,
)
from rquant.runtime_paper_quote import PaperPitQuoteResolver, PaperQuoteResolverConfig
from rquant.paper_execution_constraints import PaperExecutionConstraintBatch, PaperExecutionConstraintPointer
from rquant.strategy_candidate_snapshot import StrategyCandidateSnapshot, StrategyCandidateSnapshotSpool, asia_shanghai_trade_date


if TYPE_CHECKING:
    from rquant.minute_backtest_parameter_contracts import FrozenMinuteParameterInput


def _new_private_directory(path: Path) -> None:
    if not path.is_absolute() or path != Path(os.path.abspath(path)):
        raise ValueError("minute replay workspace must be absolute and normalized")
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(path.anchor, flags)
    try:
        for component in path.parts[1:-1]:
            child = os.open(component, flags, dir_fd=descriptor)
            os.close(descriptor)
            descriptor = child
        os.mkdir(path.name, mode=0o700, dir_fd=descriptor)
        child = os.open(path.name, flags, dir_fd=descriptor)
        try:
            observed = os.fstat(child)
            if observed.st_uid != os.getuid() or stat.S_IMODE(observed.st_mode) != 0o700:
                raise ValueError("minute replay workspace must be a new private owned directory")
        finally:
            os.close(child)
    finally:
        os.close(descriptor)


def _read_private(path: Path, maximum: int = MAX_INPUT_BYTES) -> bytes:
    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
    try:
        before = os.fstat(descriptor)
        if not stat.S_ISREG(before.st_mode) or before.st_uid != os.getuid() or before.st_nlink != 1 or before.st_size > maximum:
            raise ValueError("minute replay material is not an independent owned regular file")
        with os.fdopen(descriptor, "rb", closefd=False) as stream:
            result = stream.read(maximum + 1)
        after = os.fstat(descriptor)
        def identity(value: os.stat_result) -> tuple[int, ...]:
            return (value.st_dev, value.st_ino, value.st_mode, value.st_uid, value.st_nlink, value.st_size, value.st_mtime_ns, value.st_ctime_ns)
        if identity(before) != identity(after) or identity(path.lstat()) != identity(after) or len(result) != before.st_size:
            raise ValueError("minute replay material changed during read")
        return result
    finally:
        os.close(descriptor)


def _check_parquet_rows(payload: bytes, *, remaining_rows: int) -> int:
    metadata = parquet.ParquetFile(BytesIO(payload)).metadata
    if metadata.num_rows > remaining_rows:
        raise ValueError("minute replay Parquet rows exceed the frozen physical work budget")
    return metadata.num_rows


@dataclass(frozen=True)
class RestoredMinuteRuntimeSource:
    root: Path
    value: FrozenMinuteRuntimeInput | FrozenMinuteParameterInput
    historical_minutes: pd.DataFrame
    warmup_sha256: str
    work: MinuteReplayWork
    constraint_publications: tuple[MinuteReplayConstraintPublication, ...]

    def verify_unchanged(self) -> None:
        for material in self.value.materials:
            if hashlib.sha256(_read_private(self.root / material.relative_path)).hexdigest() != material.content_sha256:
                raise ValueError("minute replay input material changed after restore")


def restore_minute_runtime_source(
    value: FrozenMinuteRuntimeInput, *, expected: MinuteRuntimeSourceReceipt, research_root: Path
) -> RestoredMinuteRuntimeSource:
    value = FrozenMinuteRuntimeInput.model_validate(value.model_dump(mode="python"))
    expected = MinuteRuntimeSourceReceipt.model_validate(expected.model_dump(mode="python"))
    expected.verify(value)
    return _restore_minute_replay_archive(value, expected_work=expected.work, research_root=research_root)


def _restore_minute_replay_archive(
    value: FrozenMinuteRuntimeInput | FrozenMinuteParameterInput, *,
    expected_work: MinuteReplayWork, research_root: Path,
) -> RestoredMinuteRuntimeSource:
    _new_private_directory(research_root)
    root = research_root / "input"
    root.mkdir(mode=0o700)
    for material in value.materials:
        destination = root / material.relative_path
        destination.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        # mkdir(parents=True) can use default modes for intermediate directories.
        for parent in destination.parents:
            if parent == research_root:
                break
            parent.chmod(0o700)
        descriptor = os.open(destination, os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0), 0o600)
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(material.payload())
            stream.flush()
            os.fsync(stream.fileno())
    candidates = StrategyCandidateSnapshotSpool(root / "candidates")
    candidates.initialize_publisher_root()
    raw = LiveBatchSpool(root / "market", read_only=True)
    descriptor = raw.source_descriptor(LiveChannel.MARKET_MINUTE)
    records = raw.list_after(LiveChannel.MARKET_MINUTE, sequence=-1)
    if not records or tuple(record.envelope.sequence for record in records) != tuple(range(descriptor.high_watermark + 1)):
        raise ValueError("minute replay requires the complete original market prefix")
    raw_rows = 0
    codes: set[str] = set()
    for record in records:
        envelope = record.envelope
        if envelope.producer_commit != value.producer_commit or envelope.event_time_end > envelope.available_at:
            raise ValueError("minute replay raw source has future time or different code provenance")
        if not value.start_date <= asia_shanghai_trade_date(envelope.event_time_end) <= value.end_date:
            raise ValueError("minute replay raw source is outside the exact selected range")
        if envelope.available_at > value.tick_times[-1]:
            raise ValueError("minute replay clock omits a source publication")
        payload = raw.read_payload(record)
        count = _check_parquet_rows(payload, remaining_rows=MAX_WORK_UNITS - raw_rows)
        if count != envelope.row_count:
            raise ValueError("minute replay Parquet rows differ from the original envelope")
        frame = MarketMinuteGateway.normalize_frame(MarketMinuteGateway.decode_payload(payload))
        if len(frame) != envelope.row_count or (not frame.empty and (frame.trade_time > envelope.available_at).any()):
            raise ValueError("minute replay raw row count or visibility differs")
        raw_rows += len(frame)
        codes.update(frame.ts_code.astype(str))
    static_rows = 0
    for material in value.materials:
        if material.relative_path.startswith("candidates/generations/"):
            snapshot = StrategyCandidateSnapshot.model_validate_json(material.payload())
            if snapshot.producer_commit != value.producer_commit:
                raise ValueError("minute replay candidate provenance differs")
            static_rows += len(snapshot.rows)
            codes.update(row.candidate_id for row in snapshot.rows)
    warmup_material = next(item for item in value.materials if item.relative_path == "warmup.parquet")
    _check_parquet_rows(warmup_material.payload(), remaining_rows=MAX_WORK_UNITS - raw_rows - static_rows)
    historical = pd.read_parquet(BytesIO(warmup_material.payload()))
    if not set(INPUT_COLUMNS).issubset(historical.columns):
        raise ValueError("minute replay warmup schema is incomplete")
    historical_times = pd.to_datetime(historical.trade_time, utc=True)
    if not historical.empty and (historical_times >= value.tick_times[0]).any():
        raise ValueError("minute replay warmup includes current or future source rows")
    if not historical.empty and (historical_times > value.warmup_available_at).any():
        raise ValueError("minute replay warmup event follows its frozen availability")
    if "available_at" in historical and (pd.to_datetime(historical.available_at, utc=True) > value.warmup_available_at).any():
        raise ValueError("minute replay warmup material was not visible at its frozen cutoff")
    codes.update(historical.ts_code.astype(str))
    by_path = {item.relative_path: item for item in value.materials}
    publications = []
    for material in value.materials:
        if material.relative_path.startswith("constraint-publications/"):
            pointer = PaperExecutionConstraintPointer.model_validate_json(material.payload())
            original = by_path.get(f"constraints/generations/{pointer.batch_hash}.json")
            if original is None or pointer.file_sha256 != original.content_sha256:
                raise ValueError("minute replay constraint receipt lacks its exact original batch")
            batch = PaperExecutionConstraintBatch.model_validate_json(original.payload())
            publication = MinuteReplayConstraintPublication(pointer=pointer, batch=batch)
            if pointer.producer_commit != value.producer_commit or material.relative_path != f"constraint-publications/{pointer.sequence}.json":
                raise ValueError("minute replay constraint publication provenance differs")
            publications.append(publication)
            static_rows += len(batch.records)
            codes.update(record.ts_code for record in batch.records)
    publications.sort(key=lambda item: item.pointer.sequence)
    if not publications or any(left.pointer.published_at >= right.pointer.published_at or left.pointer.sequence >= right.pointer.sequence
        for left, right in zip(publications, publications[1:])):
        raise ValueError("minute replay constraint publication timeline is not ordered")
    if by_path["constraints/current.json"].payload() != next(item for item in value.materials
        if item.relative_path == f"constraint-publications/{publications[-1].pointer.sequence}.json").payload():
        raise ValueError("minute replay final constraint pointer differs from original publication history")
    if "seed/broker.sqlite3" in by_path:
        for name, tables in (("broker.sqlite3", ("paper_intent", "paper_order", "paper_fill", "paper_lot", "paper_lot_consumption", "paper_execution_receipt")),
            ("runner.sqlite3", ("candidate_state", "processed_batch", "runner_signal"))):
            with sqlite3.connect(f"{(root / 'seed' / name).as_uri()}?mode=ro&immutable=1", uri=True) as connection:
                for table in tables:
                    static_rows += connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                if name == "broker.sqlite3":
                    codes.update(row[0] for row in connection.execute("SELECT DISTINCT ts_code FROM paper_lot"))
                    for (stamp,) in connection.execute("SELECT executed_at FROM paper_fill"):
                        if pd.Timestamp(stamp) >= value.tick_times[0]:
                            raise ValueError("minute replay seed includes current or future execution")
        seeded_features = FeatureBatchSpool(root / "seed/features", cursor_root=research_root / "seed-validation-cursors", read_only=True)
        for record in seeded_features.list_after(sequence=-1):
            if record.envelope.available_at >= value.tick_times[0]:
                raise ValueError("minute replay seed feature is from the current or future clock")
            static_rows += record.envelope.row_count
            codes.update(seeded_features.read_result(record).frame.ts_code.astype(str))
        seeded_raw = LiveBatchSpool(root / "market", cursor_root=root / "seed/raw-cursors", source_read_only=True)
        cursor = seeded_raw.load_cursor("feature-live", LiveChannel.MARKET_MINUTE)
        if cursor is None or cursor.updated_at >= value.tick_times[0]:
            raise ValueError("minute replay seed lacks an original prior raw cursor")
    calendar_material = next(item for item in value.materials if item.relative_path == "calendar.json")
    calendar = json.loads(calendar_material.payload(), object_pairs_hook=_unique_object)
    calendar_rows = calendar.get("rows", calendar.get("trade_calendar")) if isinstance(calendar, dict) else calendar
    if not isinstance(calendar_rows, list):
        raise ValueError("minute replay calendar lacks its original physical rows")
    work = MinuteReplayWork(raw_rows=raw_rows, warmup_rows=len(historical), static_rows=static_rows + len(calendar_rows) + len(value.tick_times),
        market_batches=len(records), union_codes=len(codes), daily_observations=len(value.daily_trade_dates))
    if work != value.work or work != expected_work:
        raise ValueError("minute replay physical work differs from the independently frozen source bound")
    resolver = PaperPitQuoteResolver(PaperQuoteResolverConfig(raw_spool_root=root / "market", trade_calendar_path=root / "calendar.json",
        trade_calendar_sha256=calendar_material.content_sha256, execution_constraint_root=root / "constraints",
        expected_producer_commit=value.producer_commit, timestamp_semantics=value.execution_profile.timestamp_semantics))
    for tick in value.tick_times:
        day = resolver.trade_date_at(tick)
        if day not in value.market_calendar.open_dates or not value.start_date <= day <= value.end_date:
            raise ValueError("minute replay clocks differ from selected authoritative calendar range")
    source = RestoredMinuteRuntimeSource(root=root, value=value, historical_minutes=historical,
        warmup_sha256=warmup_material.content_sha256, work=work, constraint_publications=tuple(publications))
    source.verify_unchanged()
    return source


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, item in pairs:
        if key in result:
            raise ValueError("minute replay payload contains duplicate JSON fields")
        result[key] = item
    return result


def read_minute_runtime_input_table(
    connection: duckdb.DuckDBPyConnection, *, expected: MinuteRuntimeSourceReceipt
) -> FrozenMinuteRuntimeInput:
    schema = connection.execute(f"PRAGMA table_info('{MINUTE_RUNTIME_INPUT_TABLE}')").fetchall()
    if tuple((row[1], row[2], bool(row[3]), bool(row[5])) for row in schema) != (
        ("input_hash", "VARCHAR", True, True), ("payload", "VARCHAR", True, False)
    ):
        raise ValueError("minute runtime source requires the exact typed schema and primary key")
    tables = connection.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'").fetchall()
    if tables != [(MINUTE_RUNTIME_INPUT_TABLE,)]:
        raise ValueError("minute runtime source must contain only its exact input table")
    rows = connection.execute(f"SELECT input_hash, payload FROM {MINUTE_RUNTIME_INPUT_TABLE}").fetchmany(2)
    if len(rows) != 1:
        raise ValueError("minute runtime source requires exactly one complete input")
    identity, payload = rows[0]
    if not isinstance(payload, str) or len(payload.encode("utf-8")) > MAX_INPUT_BYTES:
        raise ValueError("minute runtime payload exceeds UTF-8 byte budget")
    try:
        json.loads(payload, object_pairs_hook=_unique_object, parse_constant=lambda _: (_ for _ in ()).throw(ValueError("non-finite JSON")))
    except RecursionError as error:
        raise ValueError("minute replay JSON nesting exceeds its finite decoder") from error
    value = FrozenMinuteRuntimeInput.model_validate_json(payload)
    if identity != value.input_hash:
        raise ValueError("minute runtime source row hash differs from its content")
    expected.verify(value)
    return value


def write_minute_runtime_input_table(connection: duckdb.DuckDBPyConnection, value: FrozenMinuteRuntimeInput) -> None:
    checked = FrozenMinuteRuntimeInput.model_validate(value.model_dump(mode="python"))
    if connection.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'").fetchall():
        raise ValueError("minute runtime producer requires a new empty private source")
    connection.execute(f"CREATE TABLE {MINUTE_RUNTIME_INPUT_TABLE} (input_hash VARCHAR PRIMARY KEY, payload VARCHAR NOT NULL)")
    connection.execute(f"INSERT INTO {MINUTE_RUNTIME_INPUT_TABLE} VALUES (?, ?)", [checked.input_hash, checked.model_dump_json()])
