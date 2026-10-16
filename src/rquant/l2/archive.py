"""Verified received-date archives. Remote catalog is the only progress record."""

from __future__ import annotations

import argparse
import fcntl
import gzip
import hashlib
import json
import os
import re
import stat
import tempfile
import urllib.error
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Annotated, Literal
from zoneinfo import ZoneInfo

from pydantic import BaseModel, ConfigDict, Field, StrictBool, StrictInt, model_validator

from rquant.l2.cache import RawL2Cache
from rquant.l2.models import EpochId, ErrorCode, FeedStatus, RawL2Batch, Sha256
from rquant.research.snapshot_archive import R2

PRODUCTION_PREFIX = "l2/v1/rquant"
SHANGHAI = ZoneInfo("Asia/Shanghai")
MAX_MANIFEST = 8 << 20
MAX_LINE = 8 << 20
MAX_SHARD = 32 << 20
MAX_COMPRESSED = 33 << 20
DEFAULT_OUTPUT_BYTES = 64 << 30
Count = Annotated[StrictInt, Field(ge=0, le=2**63 - 1)]
Positive = Annotated[StrictInt, Field(ge=1, le=2**63 - 1)]
Day = Annotated[str, Field(pattern=r"^\d{4}-\d{2}-\d{2}$")]


class ArchiveError(RuntimeError):
    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code)


class Model(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, populate_by_name=True)


class SourceObservation(Model):
    state: Literal[
        "unconfigured",
        "starting",
        "connected",
        "seeded_waiting",
        "listening_candidate",
        "disconnected",
        "blocked",
        "stopped",
    ]
    epoch_id: EpochId | None
    gap: StrictBool
    error_code: ErrorCode | None
    source_freshness: Literal["unknown"] = "unknown"


class Chunk(Model):
    key: str
    received_date: Day
    first_sequence: Positive
    last_sequence: Positive
    records: Positive
    compressed_bytes: Annotated[StrictInt, Field(ge=1, le=MAX_COMPRESSED)]
    expanded_bytes: Annotated[StrictInt, Field(ge=1, le=MAX_SHARD)]
    sha256: Sha256
    content_sha256: Sha256
    epochs: tuple[EpochId, ...] = Field(min_length=1)

    @model_validator(mode="after")
    def validate_range(self) -> Chunk:
        _day(self.received_date)
        if (
            self.last_sequence - self.first_sequence + 1 != self.records
            or tuple(sorted(set(self.epochs))) != self.epochs
            or any(not re.fullmatch(r"[a-zA-Z0-9_-]{1,64}", e) for e in self.epochs)
        ):
            raise ValueError("invalid chunk range or epochs")
        return self


class DayManifest(Model):
    schema_: Literal["rquant-l2-day-v1"] = Field(default="rquant-l2-day-v1", alias="schema")
    received_date: Day
    chunks: tuple[Chunk, ...] = Field(min_length=1)


class DayReference(Model):
    key: str
    sha256: Sha256
    size: Annotated[StrictInt, Field(ge=1, le=MAX_MANIFEST)]
    records: Positive


class Catalog(Model):
    schema_: Literal["rquant-l2-catalog-v1"] = Field(default="rquant-l2-catalog-v1", alias="schema")
    last_sequence: Count
    source_first_batch_sha256: Sha256 | None
    source_last_batch_sha256: Sha256 | None
    source_observation: SourceObservation
    coverage: Literal["received_date_only;granularity_unverified;unknown_day_coverage"] = (
        "received_date_only;granularity_unverified;unknown_day_coverage"
    )
    days: dict[Day, DayReference]

    @model_validator(mode="after")
    def validate_boundary(self) -> Catalog:
        for day in self.days:
            _day(day)
        if (
            (self.last_sequence == 0) != (self.source_last_batch_sha256 is None)
            or self.last_sequence
            and self.source_first_batch_sha256 is None
            or not self.last_sequence
            and self.days
        ):
            raise ValueError("invalid catalog boundaries")
        return self


class ArchiveReceipt(Model):
    schema_: Literal["rquant-l2-archive-receipt-v1"] = Field(
        default="rquant-l2-archive-receipt-v1", alias="schema"
    )
    operation: Literal["init", "run", "check", "list", "fetch"]
    status: Literal[
        "initialized",
        "already_initialized",
        "unchanged",
        "archived",
        "checked",
        "listed",
        "fetched",
        "deferred_market_hours",
        "failed",
    ]
    prefix: str
    catalog_sha256: Sha256 | None = None
    last_sequence: Count = 0
    code: str | None = None
    days: tuple[str, ...] = ()
    records: Count = 0
    first_sequence: Count = 0
    content_sha256: Sha256 | None = None
    destination: str | None = None
    day_manifest_sha256: Sha256 | None = None
    shard_sha256: tuple[Sha256, ...] = ()
    sequence_ranges: tuple[tuple[Positive, Positive], ...] = ()


def _prefix(value: str) -> str:
    if value != PRODUCTION_PREFIX and not re.fullmatch(r"l2/v1/_acceptance/[0-9a-f]{32}", value):
        raise ArchiveError("invalid_prefix")
    return value


def _day(value: str) -> str:
    if (
        not re.fullmatch(r"\d{4}-\d{2}-\d{2}", value)
        or date.fromisoformat(value).isoformat() != value
    ):
        raise ValueError("invalid received date")
    return value


def _sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _file_sha(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for data in iter(lambda: stream.read(1 << 20), b""):
            digest.update(data)
    return digest.hexdigest()


def _bytes(model: BaseModel) -> bytes:
    return model.model_dump_json(by_alias=True).encode() + b"\n"


def _unique(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate object key")
        result[key] = value
    return result


def _json(data: bytes) -> object:
    return json.loads(
        data,
        object_pairs_hook=_unique,
        parse_constant=lambda value: (_ for _ in ()).throw(ValueError("nonfinite JSON")),
    )


def _observation(status: FeedStatus) -> SourceObservation:
    return SourceObservation(
        state=status.state, epoch_id=status.epoch_id, gap=status.gap, error_code=status.error_code
    )


def _batch(cache: RawL2Cache, sequence: int) -> RawL2Batch:
    # The existing bounded reader avoids read_batches' incidental moving-tail status read.
    return cache._read_batch(cache.root / "batches" / f"{sequence:020d}", sequence)


def _separate_state(cache: RawL2Cache, state_root: Path) -> None:
    source, state = cache.root.resolve(), state_root.resolve()
    if source == state or source in state.parents or state in source.parents:
        raise ArchiveError("unsafe_state")


@contextmanager
def _job_lock(root: Path) -> Iterator[None]:
    if root.is_symlink():
        raise ArchiveError("unsafe_state")
    root.mkdir(mode=0o700, parents=True, exist_ok=True)
    fd = os.open(root / ".archive.lock", os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
    try:
        if not stat.S_ISREG(os.fstat(fd).st_mode):
            raise ArchiveError("unsafe_state")
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise ArchiveError("archive_busy") from None
        yield
    finally:
        os.close(fd)


def _write_receipt(root: Path, receipt: ArchiveReceipt) -> None:
    fd, name = tempfile.mkstemp(prefix=".receipt-", dir=root)
    try:
        with os.fdopen(fd, "wb") as stream:
            stream.write(_bytes(receipt))
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(name, root / "last-run.json")
        directory = os.open(root, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        Path(name).unlink(missing_ok=True)


def _head(client: R2, key: str, maximum: int) -> dict | None:
    head = client.head(key, include_etag=True)
    if head is not None and (
        type(head.get("size")) is not int
        or not 0 < head["size"] <= maximum
        or not re.fullmatch(r"[0-9a-f]{64}", str(head.get("sha256")))
    ):
        raise ArchiveError("remote_metadata")
    return head


def _get(client: R2, key: str, maximum: int, directory: Path) -> tuple[Path, dict]:
    head = _head(client, key, maximum)
    if head is None:
        raise ArchiveError("missing_remote")
    fd, name = tempfile.mkstemp(dir=directory)
    os.close(fd)
    path = Path(name)
    client.get_file(key, path, maximum_bytes=maximum)
    if path.stat().st_size != head["size"] or _file_sha(path) != head["sha256"]:
        raise ArchiveError("remote_integrity")
    return path, head


def _etag(head: dict) -> str:
    token = head.get("etag")
    if not isinstance(token, str) or re.fullmatch(r'"[\x21\x23-\x7e]*"', token) is None:
        raise ArchiveError("missing_etag")
    return token


def _load_catalog(client: R2, prefix: str, temporary: Path) -> tuple[Catalog, str, str]:
    key = prefix + "/catalog.json"
    if client.head(key) is None:
        raise ArchiveError("missing_catalog")
    path, head = _get(client, key, MAX_MANIFEST, temporary)
    wire = _json(path.read_bytes())
    if not isinstance(wire, dict) or wire.get("schema") != "rquant-l2-catalog-v1":
        raise ArchiveError("invalid_catalog_schema")
    return Catalog.model_validate(wire), head["sha256"], _etag(head)


def _chunk_key(prefix: str, chunk: Chunk) -> str:
    return (
        f"{prefix}/days/{chunk.received_date}/chunks/{chunk.first_sequence:020d}-"
        f"{chunk.last_sequence:020d}-{chunk.content_sha256}.jsonl.gz"
    )


def _index(client: R2, prefix: str, catalog: Catalog, temporary: Path) -> dict[str, DayManifest]:
    result: dict[str, DayManifest] = {}
    intervals: list[tuple[int, int]] = []
    for day, ref in catalog.days.items():
        if ref.key != f"{prefix}/days/{day}/manifests/{ref.sha256}.json":
            raise ArchiveError("invalid_manifest_path")
        path, head = _get(client, ref.key, MAX_MANIFEST, temporary)
        if (head["sha256"], head["size"]) != (ref.sha256, ref.size):
            raise ArchiveError("manifest_integrity")
        wire = _json(path.read_bytes())
        if not isinstance(wire, dict) or wire.get("schema") != "rquant-l2-day-v1":
            raise ArchiveError("invalid_day_schema")
        manifest = DayManifest.model_validate(wire)
        if manifest.received_date != day or sum(c.records for c in manifest.chunks) != ref.records:
            raise ArchiveError("manifest_count_date")
        prior = 0
        for chunk in manifest.chunks:
            if (
                chunk.received_date != day
                or chunk.key != _chunk_key(prefix, chunk)
                or chunk.first_sequence <= prior
            ):
                raise ArchiveError("invalid_chunk_reference")
            prior = chunk.last_sequence
            remote = _head(client, chunk.key, MAX_COMPRESSED)
            if remote is None or (remote["size"], remote["sha256"]) != (
                chunk.compressed_bytes,
                chunk.sha256,
            ):
                raise ArchiveError("missing_or_corrupt_shard")
            intervals.append((chunk.first_sequence, chunk.last_sequence))
        result[day] = manifest
    cursor = 0
    for start, end in sorted(intervals):
        if start != cursor + 1:
            raise ArchiveError("archive_sequence_gap")
        cursor = end
    if cursor != catalog.last_sequence:
        raise ArchiveError("archive_sequence_gap")
    return result


def _put(
    client: R2,
    key: str,
    path: Path,
    temporary: Path,
    *,
    etag: str | None = None,
    immutable: bool = False,
) -> str:
    digest = _file_sha(path)
    maximum = MAX_COMPRESSED if key.endswith(".jsonl.gz") else MAX_MANIFEST
    if path.stat().st_size > maximum:
        raise ArchiveError("object_limit")
    if immutable and client.head(key) is not None:
        existing, head = _get(client, key, maximum, temporary)
        if head["sha256"] != digest or existing.read_bytes() != path.read_bytes():
            raise ArchiveError("immutable_conflict")
        return _etag(head)
    try:
        client.put_file(
            key, path, digest, **({"if_none_match": "*"} if etag is None else {"if_match": etag})
        )
    except OSError as exc:
        if isinstance(exc, urllib.error.HTTPError) and exc.code == 412:
            raise ArchiveError("precondition_failed") from None
        # An uncertain reply can only be confirmed by exact remote bytes.
        try:
            verified, head = _get(client, key, maximum, temporary)
            if _file_sha(verified) == digest and verified.read_bytes() == path.read_bytes():
                return _etag(head)
        except (OSError, ValueError, ArchiveError):
            pass
        raise ArchiveError("publication_uncertain") from None
    verified, head = _get(client, key, maximum, temporary)
    if head["sha256"] != digest or verified.read_bytes() != path.read_bytes():
        raise ArchiveError("publication_integrity")
    return _etag(head)


def _source(cache: RawL2Cache, catalog: Catalog, highwater: int) -> tuple[str | None, str | None]:
    if highwater < catalog.last_sequence:
        raise ArchiveError("source_truncated")
    first = _sha(_bytes(_batch(cache, 1))) if highwater else None
    if catalog.source_first_batch_sha256 is not None and first != catalog.source_first_batch_sha256:
        raise ArchiveError("source_changed")
    if (
        catalog.last_sequence
        and _sha(_bytes(_batch(cache, catalog.last_sequence))) != catalog.source_last_batch_sha256
    ):
        raise ArchiveError("source_changed")
    return first, _sha(_bytes(_batch(cache, highwater))) if highwater else None


def initialize(
    cache: RawL2Cache, state_root: Path, *, client: R2, prefix: str = PRODUCTION_PREFIX
) -> ArchiveReceipt:
    prefix = _prefix(prefix)
    _separate_state(cache, state_root)
    with _job_lock(state_root), tempfile.TemporaryDirectory(dir=state_root) as work:
        temporary, snapshot = Path(work), cache.read_status()
        if client.head(prefix + "/catalog.json") is not None:
            catalog, digest, _ = _load_catalog(client, prefix, temporary)
            _index(client, prefix, catalog, temporary)
            _source(cache, catalog, snapshot.last_sequence)
            receipt = ArchiveReceipt(
                operation="init",
                status="already_initialized",
                prefix=prefix,
                catalog_sha256=digest,
                last_sequence=catalog.last_sequence,
            )
        else:
            first = _sha(_bytes(_batch(cache, 1))) if snapshot.last_sequence else None
            catalog = Catalog(
                last_sequence=0,
                source_first_batch_sha256=first,
                source_last_batch_sha256=None,
                source_observation=_observation(snapshot),
                days={},
            )
            path = temporary / "catalog"
            path.write_bytes(_bytes(catalog))
            _put(client, prefix + "/catalog.json", path, temporary)
            receipt = ArchiveReceipt(
                operation="init",
                status="initialized",
                prefix=prefix,
                catalog_sha256=_file_sha(path),
            )
        _write_receipt(state_root, receipt)
        return receipt


def _chunk_lines(client: R2, chunk: Chunk, temporary: Path) -> Iterator[bytes]:
    path, head = _get(client, chunk.key, MAX_COMPRESSED, temporary)
    if (head["size"], head["sha256"]) != (chunk.compressed_bytes, chunk.sha256):
        raise ArchiveError("shard_integrity")
    total, count, epochs, digest = 0, 0, set(), hashlib.sha256()
    try:
        with gzip.open(path, "rb") as stream:
            while line := stream.readline(MAX_LINE + 1):
                total += len(line)
                if len(line) > MAX_LINE or not line.endswith(b"\n") or total > MAX_SHARD:
                    raise ArchiveError("expanded_limit")
                record = RawL2Batch.model_validate(_json(line))
                if (
                    record.local_batch_sequence != chunk.first_sequence + count
                    or record.received_at.astimezone(SHANGHAI).date().isoformat()
                    != chunk.received_date
                    or _bytes(record) != line
                ):
                    raise ArchiveError("shard_record_mismatch")
                epochs.add(record.epoch_id)
                count += 1
                digest.update(line)
                yield line
    except (OSError, EOFError, ValueError) as exc:
        raise ArchiveError("invalid_shard") from exc
    if (
        count != chunk.records
        or total != chunk.expanded_bytes
        or digest.hexdigest() != chunk.content_sha256
        or tuple(sorted(epochs)) != chunk.epochs
    ):
        raise ArchiveError("shard_content_mismatch")


def _new_chunks(
    cache: RawL2Cache, start: int, highwater: int, client: R2, prefix: str, temporary: Path
) -> list[Chunk]:
    chunks: list[Chunk] = []
    sequence = start
    while sequence <= highwater:
        first, day, total, epochs = sequence, None, 0, set()
        content = hashlib.sha256()
        path = temporary / f"chunk-{first}"
        with (
            path.open("wb") as raw,
            gzip.GzipFile(filename="", mode="wb", fileobj=raw, mtime=0) as stream,
        ):
            while sequence <= highwater:
                record = _batch(cache, sequence)
                current_day = record.received_at.astimezone(SHANGHAI).date().isoformat()
                line = _bytes(record)
                if len(line) > MAX_LINE:
                    raise ArchiveError("record_limit")
                if day is not None and (current_day != day or total + len(line) > MAX_SHARD):
                    break
                day = current_day
                stream.write(line)
                content.update(line)
                total += len(line)
                epochs.add(record.epoch_id)
                sequence += 1
        chunk = Chunk(
            key="pending",
            received_date=day,
            first_sequence=first,
            last_sequence=sequence - 1,
            records=sequence - first,
            expanded_bytes=total,
            compressed_bytes=path.stat().st_size,
            sha256=_file_sha(path),
            content_sha256=content.hexdigest(),
            epochs=tuple(sorted(epochs)),
        )
        chunk = chunk.model_copy(update={"key": _chunk_key(prefix, chunk)})
        _put(client, chunk.key, path, temporary, immutable=True)
        # Uploaded bytes must also pass the same typed restore path.
        for _ in _chunk_lines(client, chunk, temporary):
            pass
        chunks.append(chunk)
    return chunks


def run_archive(
    cache: RawL2Cache,
    state_root: Path,
    *,
    client: R2,
    prefix: str = PRODUCTION_PREFIX,
    now: datetime | None = None,
) -> ArchiveReceipt:
    prefix = _prefix(prefix)
    clock = now or datetime.now(UTC)
    if clock.tzinfo is None:
        raise ArchiveError("invalid_clock")
    local = clock.astimezone(SHANGHAI)
    if local.weekday() < 5 and (7, 0) <= (local.hour, local.minute) <= (16, 30):
        return ArchiveReceipt(operation="run", status="deferred_market_hours", prefix=prefix)
    _separate_state(cache, state_root)
    with _job_lock(state_root), tempfile.TemporaryDirectory(dir=state_root) as work:
        temporary, snapshot = Path(work), cache.read_status()
        highwater = snapshot.last_sequence
        catalog, digest, token = _load_catalog(client, prefix, temporary)
        manifests = _index(client, prefix, catalog, temporary)
        first, last = _source(cache, catalog, highwater)
        days = dict(catalog.days)
        new = _new_chunks(cache, catalog.last_sequence + 1, highwater, client, prefix, temporary)
        for day in sorted({c.received_date for c in new}):
            prior = manifests[day].chunks if day in manifests else ()
            manifest = DayManifest(
                received_date=day, chunks=prior + tuple(c for c in new if c.received_date == day)
            )
            path = temporary / f"day-{day}"
            path.write_bytes(_bytes(manifest))
            if path.stat().st_size > MAX_MANIFEST:
                raise ArchiveError("manifest_limit")
            sha = _file_sha(path)
            key = f"{prefix}/days/{day}/manifests/{sha}.json"
            _put(client, key, path, temporary, immutable=True)
            days[day] = DayReference(
                key=key,
                sha256=sha,
                size=path.stat().st_size,
                records=sum(c.records for c in manifest.chunks),
            )
        updated = Catalog(
            last_sequence=highwater,
            source_first_batch_sha256=first,
            source_last_batch_sha256=last,
            source_observation=_observation(snapshot),
            days=dict(sorted(days.items())),
        )
        if updated != catalog:
            _index(client, prefix, updated, temporary)
            path = temporary / "catalog"
            path.write_bytes(_bytes(updated))
            _put(client, prefix + "/catalog.json", path, temporary, etag=token)
            digest = _file_sha(path)
        receipt = ArchiveReceipt(
            operation="run",
            status="archived" if updated != catalog else "unchanged",
            prefix=prefix,
            catalog_sha256=digest,
            last_sequence=highwater,
        )
        _write_receipt(state_root, receipt)
        return receipt


def check_archive(*, client: R2, prefix: str = PRODUCTION_PREFIX) -> ArchiveReceipt:
    prefix = _prefix(prefix)
    with tempfile.TemporaryDirectory(prefix="l2-archive-check-") as work:
        catalog, digest, _ = _load_catalog(client, prefix, Path(work))
        _index(client, prefix, catalog, Path(work))
        return ArchiveReceipt(
            operation="check",
            status="checked",
            prefix=prefix,
            catalog_sha256=digest,
            last_sequence=catalog.last_sequence,
            days=tuple(sorted(catalog.days)),
        )


def list_archive(*, client: R2, prefix: str = PRODUCTION_PREFIX) -> ArchiveReceipt:
    return check_archive(client=client, prefix=prefix).model_copy(
        update={"operation": "list", "status": "listed"}
    )


def fetch_archive(
    received_date: str,
    dest: Path,
    *,
    client: R2,
    prefix: str = PRODUCTION_PREFIX,
    maximum_output_bytes: int = DEFAULT_OUTPUT_BYTES,
) -> ArchiveReceipt:
    prefix, day = _prefix(prefix), _day(received_date)
    if (
        type(maximum_output_bytes) is not int
        or not 0 < maximum_output_bytes <= DEFAULT_OUTPUT_BYTES
    ):
        raise ArchiveError("output_limit")
    if dest.exists() or dest.is_symlink() or not dest.name or dest.name in (".", ".."):
        raise ArchiveError("destination_exists")
    if not dest.parent.is_dir() or dest.parent.is_symlink():
        raise ArchiveError("unsafe_destination")
    with tempfile.TemporaryDirectory(prefix=".l2-restore-", dir=dest.parent) as work:
        temporary = Path(work)
        catalog, digest, _ = _load_catalog(client, prefix, temporary)
        manifests = _index(client, prefix, catalog, temporary)
        if day not in manifests:
            raise ArchiveError("missing_date")
        output = temporary / "result"
        output.mkdir(mode=0o700)
        total, records, content = 0, 0, hashlib.sha256()
        with (output / "batches.jsonl").open("xb") as stream:
            os.chmod(stream.name, 0o600)
            for chunk in manifests[day].chunks:
                for line in _chunk_lines(client, chunk, temporary):
                    total += len(line)
                    if total > maximum_output_bytes:
                        raise ArchiveError("output_limit")
                    stream.write(line)
                    content.update(line)
                    records += 1
            stream.flush()
            os.fsync(stream.fileno())
        chunks = manifests[day].chunks
        receipt = ArchiveReceipt(
            operation="fetch",
            status="fetched",
            prefix=prefix,
            catalog_sha256=digest,
            last_sequence=catalog.last_sequence,
            days=(day,),
            records=records,
            first_sequence=chunks[0].first_sequence,
            content_sha256=content.hexdigest(),
            destination=str(dest),
            day_manifest_sha256=catalog.days[day].sha256,
            shard_sha256=tuple(c.sha256 for c in chunks),
            sequence_ranges=tuple((c.first_sequence, c.last_sequence) for c in chunks),
        )
        _write_receipt(output, receipt)
        (output / "last-run.json").rename(output / "receipt.json")
        # The parent is caller-owned; never replace an existing restore.
        if dest.exists() or dest.is_symlink():
            raise ArchiveError("destination_exists")
        os.rename(output, dest)
        directory = os.open(dest.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
        return receipt


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="operation", required=True)
    for operation in ("init", "run", "check", "list", "fetch"):
        sub = commands.add_parser(operation)
        sub.add_argument("--prefix", default=PRODUCTION_PREFIX)
        if operation in ("init", "run"):
            sub.add_argument("--cache-root", type=Path, required=True)
            sub.add_argument("--state-root", type=Path, required=True)
        if operation == "fetch":
            sub.add_argument("--date", required=True)
            sub.add_argument("--dest", type=Path, required=True)
            sub.add_argument("--maximum-output-bytes", type=int, default=DEFAULT_OUTPUT_BYTES)
    args = parser.parse_args(argv)
    try:
        prefix = _prefix(args.prefix)
        # Defer before constructing a credentialed client or accessing cache/state.
        if args.operation == "run":
            local = datetime.now(SHANGHAI)
            if local.weekday() < 5 and (7, 0) <= (local.hour, local.minute) <= (16, 30):
                result = ArchiveReceipt(
                    operation="run", status="deferred_market_hours", prefix=prefix
                )
                print(result.model_dump_json(by_alias=True))
                return 0
        client = R2.from_env()
        if args.operation in ("init", "run"):
            function = initialize if args.operation == "init" else run_archive
            result = function(
                RawL2Cache(args.cache_root), args.state_root, client=client, prefix=prefix
            )
        elif args.operation == "fetch":
            result = fetch_archive(
                args.date,
                args.dest,
                client=client,
                prefix=prefix,
                maximum_output_bytes=args.maximum_output_bytes,
            )
        else:
            result = (check_archive if args.operation == "check" else list_archive)(
                client=client, prefix=prefix
            )
        print(result.model_dump_json(by_alias=True))
        return 0
    except (OSError, ValueError, RuntimeError, SystemExit, RecursionError):
        print(
            ArchiveReceipt(
                operation=args.operation, status="failed", prefix=args.prefix, code="archive_failed"
            ).model_dump_json(by_alias=True)
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
