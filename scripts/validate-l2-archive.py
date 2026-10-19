#!/usr/bin/env python3
"""Standalone synthetic archive roundtrip using the runtime dependencies only."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import struct
import tempfile
import urllib.error
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from rquant.l2 import archive
from rquant.l2.cache import RawL2Cache
from rquant.l2.models import Binding, FeedStatus, MarketObservation, RawTable
from rquant.research import snapshot_archive


class Counted:
    """Record actual storage operations; never record their credentialed arguments."""

    def __init__(self, client: snapshot_archive.R2) -> None:
        self.client = client
        self.counts = {"put": 0, "head": 0, "get": 0, "precondition_failed": 0}

    def put_file(self, key: str, src: Path, sha: str, **conditions: str) -> None:
        self.counts["put"] += 1
        try:
            self.client.put_file(key, src, sha, **conditions)
        except urllib.error.HTTPError as error:
            if error.code == 412:
                self.counts["precondition_failed"] += 1
            error.close()
            raise

    def head(self, key: str, *, include_etag: bool = False) -> dict | None:
        self.counts["head"] += 1
        return self.client.head(key, include_etag=include_etag)

    def get_file(self, key: str, dest: Path, *, maximum_bytes: int | None = None) -> None:
        self.counts["get"] += 1
        self.client.get_file(key, dest, maximum_bytes=maximum_bytes)


def _write(path: Path, value: dict) -> None:
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, "wb") as stream:
        stream.write(json.dumps(value, sort_keys=True, separators=(",", ":")).encode() + b"\n")
        stream.flush()
        os.fsync(stream.fileno())


def _observation(index: int) -> MarketObservation:
    flags = (0x2001, 0x100A, 0x100D, 0x200C, 0x2038, 0xA561)
    values = (100 + index, 1681, 2, 1, 1792047599 + index, 1)
    row_bytes = b"".join(struct.pack("<IB", value, 0) for value in values)
    row = {
        str(flag & 0x8FFF): {
            "value": str(value) if flag & 0x7000 == 0x1000 else value,
            "raw_hex": struct.pack("<I", value).hex(),
            "style": 0,
        }
        for flag, value in zip(flags, values, strict=True)
    }
    title = "603258-逐笔委托"
    strings = struct.pack("<H", len(title)) + title.encode("utf-16le") + b"\0\0" * 6
    extended = 48 + len(strings)
    table = (
        b"tb1.0\0"
        + struct.pack("<iiiHHH", 1, -1, 0, extended, 30, 6)
        + b"".join(struct.pack("<HBB", flag, 4, 8) for flag in flags)
        + strings
        + row_bytes
    )
    payload = b"synthetic-market-only"
    body = (
        struct.pack("<HIIHIII", 32, 3, 0x1100, 10113, len(payload), 2357, 0) + b"\0" * 8 + payload
    )
    columns = [
        {
            "field_id": flag & 0x8FFF,
            "flags": flag,
            "flags_hex": hex(flag),
            "width": 4,
            "extra": 8,
            "encoding": "packed32-le" if flag & 0x7000 == 0x1000 else "unsigned-le",
        }
        for flag in flags
    ]
    return MarketObservation(
        epoch_id=f"synthetic_{index // 2}",
        binding=Binding(page_id=10113, kind="vendor4027", code="603258", market=17, side="buy"),
        received_at=datetime(2026, 10, 14 + index // 2, 7, tzinfo=UTC),
        request_id=3,
        source_fd_ordinal=index + 1,
        delivery="seed" if index % 2 == 0 else "candidate_push",
        response_body_hex=body.hex(),
        raw_table_hex=table.hex(),
        response_sha256=hashlib.sha256(body).hexdigest(),
        table=RawTable(
            title=title,
            table_version="tb1.0",
            source_codec="snappy",
            source_sha256=hashlib.sha256(table).hexdigest(),
            row_data_sha256=hashlib.sha256(row_bytes).hexdigest(),
            row_count=1,
            row_width=30,
            col_count=6,
            extended_header_length=extended,
            header_unknown_10=-1,
            header_unknown_14=0,
            columns=columns,
            rows=[row],
            interpretation_limits="synthetic vendor fields; semantics unverified",
        ),
    )


def run_probe(
    source_root: Path,
    source_tree: str,
    output_dir: Path,
    prefix: str,
    *,
    client: snapshot_archive.R2 | None = None,
) -> dict:
    if not re.fullmatch(r"[0-9a-f]{40}", source_tree) or not re.fullmatch(
        r"l2/v1/_acceptance/[0-9a-f]{32}", prefix
    ):
        raise ValueError("invalid acceptance source or prefix")
    root = source_root.resolve(strict=True)
    expected = {
        "src/rquant/l2/archive.py": Path(archive.__file__),
        "src/rquant/research/snapshot_archive.py": Path(snapshot_archive.__file__),
        "scripts/validate-l2-archive.py": Path(__file__),
    }
    for relative, actual in expected.items():
        if actual.resolve(strict=True) != root / relative or actual.is_symlink():
            raise ValueError("executed source mismatch")
    hashes = {
        name: hashlib.sha256(path.read_bytes()).hexdigest() for name, path in expected.items()
    }
    output_dir.mkdir(mode=0o700, exist_ok=False)
    _write(
        output_dir / "execution-source.json",
        {
            "schema": "rquant-l2-archive-execution-source-v1",
            "root_verified_source_tree": source_tree,
            "file_sha256": hashes,
            "whole_tree_verification": "external_root_gate",
        },
    )
    real = client is None
    raw = snapshot_archive.R2.from_env() if real else client
    responses: list[Any] = []
    if real:
        original = raw._request

        def traced(method: str, key: str, **kwargs: Any) -> Any:
            try:
                response = original(method, key, **kwargs)
            except urllib.error.HTTPError as error:
                responses.append(error)
                raise
            responses.append(response)
            return response

        raw._request = traced
    counted = Counted(raw)
    cache = RawL2Cache(output_dir / "synthetic-cache")
    with cache.writer():
        for index in range(4):
            cache.append(_observation(index))
        cache.write_status(FeedStatus(state="stopped", gap=True, epoch_id="synthetic_1"))
    highwater = cache.read_status().last_sequence
    if highwater != 4:
        raise ValueError("source count mismatch")
    source = hashlib.sha256()
    for sequence in range(1, highwater + 1):
        source.update(archive._bytes(archive._batch(cache, sequence)))
    state = output_dir / "state"
    archive.initialize(cache, state, client=counted, prefix=prefix)
    now = datetime(2026, 10, 16, 9, 20, tzinfo=UTC)
    archive.run_archive(cache, state, client=counted, prefix=prefix, now=now)
    (state / "last-run.json").unlink()
    listing = archive.list_archive(client=counted, prefix=prefix)
    restored, restored_count = hashlib.sha256(), 0
    for day in listing.days:
        archive.fetch_archive(day, output_dir / day, client=counted, prefix=prefix)
        with (output_dir / day / "batches.jsonl").open("rb") as stream:
            while line := stream.readline(archive.MAX_LINE + 1):
                restored_count += 1
                if (restored_count > highwater or len(line) > archive.MAX_LINE
                        or not line.endswith(b"\n")
                        or line != archive._bytes(archive._batch(cache, restored_count))):
                    raise ValueError("roundtrip mismatch")
                restored.update(line)
    if restored_count != 4 or restored.hexdigest() != source.hexdigest():
        raise ValueError("roundtrip mismatch")
    # No local receipt or checkpoint may be necessary to discover remote progress.
    before = counted.counts["put"]
    if (
        archive.run_archive(
            cache, output_dir / "fresh-state", client=counted, prefix=prefix, now=now
        ).status
        != "unchanged"
    ):
        raise ValueError("retry advanced catalog")
    idempotence_puts = counted.counts["put"] - before
    if idempotence_puts:
        raise ValueError("unchanged retry wrote objects")
    with tempfile.TemporaryDirectory(dir=output_dir) as work:
        temporary = Path(work)
        catalog, _, token = archive._load_catalog(counted, prefix, temporary)
        candidate = temporary / "conflict"
        candidate.write_bytes(archive._bytes(catalog))
        stale = '"' + "0" * 32 + '"'
        if stale == token:
            stale = '"' + "1" * 32 + '"'
        try:
            counted.put_file(
                prefix + "/catalog.json",
                candidate,
                hashlib.sha256(candidate.read_bytes()).hexdigest(),
                if_match=stale,
            )
        except urllib.error.HTTPError as error:
            if error.code != 412:
                raise
        else:
            raise ValueError("conditional write accepted stale token")
        after, _, _ = archive._load_catalog(counted, prefix, temporary)
        if after != catalog or catalog.source_observation.gap is not True:
            raise ValueError("conditional write or gap mismatch")
    closed = all(response.closed for response in responses)
    if not closed:
        raise ValueError("storage response left open")
    proof = {
        "schema": "rquant-l2-archive-acceptance-v1",
        "source_tree": source_tree,
        "prefix": prefix,
        "real_r2": real,
        "synthetic_only": True,
        "cases": {
            "put_head_get": True,
            "multi_date": len(listing.days) == 2,
            "stateless_fetch": True,
            "original_sequences": True,
            "gap_preserved": True,
            "conditional_conflict": True,
        },
        "roundtrip": {
            "source_count": 4,
            "restored_count": restored_count,
            "received_dates": list(listing.days),
            "source_sha256": source.hexdigest(),
            "restored_sha256": restored.hexdigest(),
            "catalog_last_sequence": listing.last_sequence,
        },
        "idempotence_puts": idempotence_puts,
        "requests": dict(counted.counts),
        "closed": closed,
    }
    if not all(proof["cases"].values()):
        raise ValueError("incomplete acceptance cases")
    _write(output_dir / "proof.json", proof)
    return proof


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-root", type=Path, required=True)
    parser.add_argument("--source-tree", required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--prefix", required=True)
    args = parser.parse_args(argv)
    try:
        proof = run_probe(args.source_root, args.source_tree, args.output_dir, args.prefix)
        print(
            json.dumps(
                {"status": "passed", "source_tree": proof["source_tree"], "closed": proof["closed"]}
            )
        )
        return 0
    except (OSError, ValueError, RuntimeError, SystemExit):
        print('{"status":"failed","code":"archive_acceptance_failed"}')
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
