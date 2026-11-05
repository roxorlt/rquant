"""Build deterministic replay packs from committed local L2 cache batches.

The receiver already persists every accepted market frame as
``response_body_hex`` plus ``response_sha256`` inside the cache batch record.
That body is exactly the frame body observed on the socket, so an existing
cache can be turned into byte-identical raw frames without new market data and
without waiting for the next trading session.

A pack is a directory with one or more ``segment-*.tap`` files in the
``rquant-l2-frame-tap-v1`` format plus ``manifest.json``. The replay engine
reads the segments with :func:`rquant.l2.frame_tap.iter_records` and gets
received wire frames with microsecond monotonic timestamps, a synthesized
bootstrap ack (the live ack never reaches the cache) and enough metadata to
rebuild request-gated or timed playback.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import struct
import sys
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .cache import CacheError, RawL2Cache
from .frame_tap import (FORMAT_NAME, KIND_RX, FrameTapError, TapFileWriter, iter_records,
                        read_manifest, write_manifest, wire_frame)

PACK_FORMAT = "rquant-l2-replay-pack-v1"
PACK_VERSION = 1
DEFAULT_SEGMENT_BYTES = 256 * 1024 * 1024
_ACK_BODY = struct.pack("<HIIHII", 26, 0, 6, 0, 0, 2201) + b"\x00" * 6


class ReplayPackError(RuntimeError):
    """Fixed local code; never carries upstream text or raw frames."""

    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code)


@dataclass(frozen=True)
class ReplayPackConfig:
    cache_root: Path
    output_dir: Path
    start_sequence: int = 1
    end_sequence: int | None = None
    segment_bytes: int = DEFAULT_SEGMENT_BYTES
    synthesize_bootstrap_ack: bool = True

    def __post_init__(self) -> None:
        object.__setattr__(self, "cache_root", Path(self.cache_root))
        object.__setattr__(self, "output_dir", Path(self.output_dir))
        if type(self.start_sequence) is not int or self.start_sequence < 1:
            raise ReplayPackError("invalid_range")
        if self.end_sequence is not None and (type(self.end_sequence) is not int
                                              or self.end_sequence < self.start_sequence):
            raise ReplayPackError("invalid_range")
        if type(self.synthesize_bootstrap_ack) is not bool:
            raise ReplayPackError("invalid_config")


def _now() -> datetime:
    return datetime.now(UTC)


def _us(moment: datetime) -> int:
    return int(moment.timestamp() * 1_000_000)


def build_replay_pack(config: ReplayPackConfig) -> dict[str, Any]:
    """Write a replay pack from committed cache batches and return its manifest."""
    if not isinstance(config, ReplayPackConfig):
        raise ReplayPackError("invalid_config")
    output_dir = config.output_dir
    try:
        output_dir.mkdir(parents=True, mode=0o700, exist_ok=True)
        if any(output_dir.iterdir()):
            raise ReplayPackError("output_not_empty")
    except OSError:
        raise ReplayPackError("output_io") from None
    cache = RawL2Cache(config.cache_root)
    try:
        status = cache.read_status()
    except CacheError:
        raise ReplayPackError("invalid_cache") from None
    total = status.last_sequence
    end_sequence = total if config.end_sequence is None else min(config.end_sequence, total)
    if end_sequence < config.start_sequence:
        raise ReplayPackError("invalid_range")
    batches = list(cache.iter_batches(after_sequence=config.start_sequence - 1,
                                      limit=end_sequence - config.start_sequence + 1))
    if not batches:
        raise ReplayPackError("empty_range")
    # A retired prefix makes iter_batches start at the floor; a pack that silently
    # relabels a different range would corrupt every replay derived from it.
    if (batches[0].local_batch_sequence != config.start_sequence
            or batches[-1].local_batch_sequence != end_sequence):
        raise ReplayPackError("range_retired")
    first_wall = batches[0].received_at
    started_wall_us = _us(first_wall)
    # Segment bytes must be reproducible: keep run time out of the tap header.
    meta = {
        "format": FORMAT_NAME,
        "source": PACK_FORMAT,
        "cache_root": str(config.cache_root),
        "start_sequence": config.start_sequence,
        "end_sequence": end_sequence,
    }
    writer = TapFileWriter(output_dir, meta=meta, started_wall_us=started_wall_us,
                           started_mono_us=0, segment_bytes=config.segment_bytes)
    frames = {"rx": 0, "tx": 0, "event": 0, "market": 0, "synthetic_control": 0}
    delivery = {"seed": 0, "backfill": 0, "candidate_push": 0}
    symbols: dict[str, set[int]] = {}
    epochs: set[str] = set()
    last_mono_us = 0
    last_wall_us = started_wall_us
    try:
        if config.synthesize_bootstrap_ack:
            writer.add(KIND_RX, 0, wire_frame(_ACK_BODY))
            frames["rx"] += 1
            frames["synthetic_control"] += 1
        for batch in batches:
            body = bytes.fromhex(batch.response_body_hex)
            if hashlib.sha256(body).hexdigest() != batch.response_sha256:
                raise ReplayPackError("invalid_cache")
            mono_us = max(last_mono_us, int((batch.received_at - first_wall).total_seconds()
                                            * 1_000_000))
            writer.add(KIND_RX, mono_us, wire_frame(body))
            last_mono_us = mono_us
            last_wall_us = _us(batch.received_at)
            frames["rx"] += 1
            frames["market"] += 1
            delivery[batch.delivery] = delivery.get(batch.delivery, 0) + 1
            symbols.setdefault(batch.binding.code, set()).add(batch.binding.page_id)
            epochs.add(batch.epoch_id)
        summary = writer.close()
    except ReplayPackError:
        writer.abort()
        _remove_partial(output_dir)
        raise
    except (FrameTapError, OSError):
        writer.abort()
        _remove_partial(output_dir)
        raise ReplayPackError("output_io") from None
    manifest = {
        "format": PACK_FORMAT,
        "version": PACK_VERSION,
        "created_at": _now().isoformat(),
        "source": {
            "kind": "committed_l2_cache",
            "cache_root": str(config.cache_root),
            "cache_last_sequence": total,
            "epoch_ids": sorted(epochs),
        },
        "range": {"start_sequence": config.start_sequence, "end_sequence": end_sequence},
        "tap_format": FORMAT_NAME,
        "timing": {
            "unit": "microseconds",
            "clock": "synthetic_monotonic_relative_to_first_cached_batch",
            "started_wall_us": started_wall_us,
            "last_wall_us": last_wall_us,
            "duration_us": last_wall_us - started_wall_us,
            "speed": "as-recorded-1x",
        },
        "frames": {
            "rx": frames["rx"], "tx": frames["tx"], "event": frames["event"],
            "market": frames["market"],
            "synthetic_control": frames["synthetic_control"],
            "total": frames["rx"] + frames["tx"] + frames["event"],
        },
        "delivery": delivery,
        "symbols": {code: sorted(pages) for code, pages in sorted(symbols.items())},
        "segments": summary["segments"],
        "bytes": summary["bytes"],
        "reader": {
            "module": "rquant.l2.frame_tap",
            "callable": "iter_records",
            "returns": "TapRecord(kind, mono_us, payload); kind 1 is received wire bytes",
        },
        "synthetic": {"bootstrap_ack": 1 if config.synthesize_bootstrap_ack else 0},
        "notes": [
            "received payloads are exact 13-byte FD frames reconstructed from cache bodies; "
            "response_sha256 of every cached batch is re-checked while building",
            "the bootstrap ack is synthetic because live control frames are not persisted in "
            "the market cache; use a live frame-tap recording when control-frame fidelity matters",
        ],
    }
    try:
        write_manifest(output_dir, manifest)
    except FrameTapError:
        _remove_partial(output_dir)
        raise ReplayPackError("output_io") from None
    return manifest


def _remove_partial(directory: Path) -> None:
    try:
        for path in directory.iterdir():
            path.unlink(missing_ok=True)
    except OSError:
        pass


def verify_replay_pack(directory: Path) -> dict[str, Any]:
    """Re-read every segment, verify footer checksums and compare the manifest."""
    manifest = read_manifest(directory)
    if manifest is None:
        raise ReplayPackError("manifest_missing")
    if manifest.get("format") != PACK_FORMAT or manifest.get("version") != PACK_VERSION:
        raise ReplayPackError("unsupported_manifest")
    counts = {"rx": 0, "tx": 0, "event": 0}
    frames: list[str] = []
    payload_bytes = 0
    last_mono = -1
    for segment in manifest["segments"]:
        path = Path(directory) / segment["file"]
        segment_records = 0
        for record in iter_records(path):
            counts[record.name] = counts.get(record.name, 0) + 1
            frames.append(record.name)
            payload_bytes += len(record.payload)
            if record.mono_us < last_mono:
                raise ReplayPackError("timing_regression")
            last_mono = record.mono_us
            segment_records += 1
        if segment_records != segment["records"]:
            raise ReplayPackError("manifest_mismatch")
    declared = manifest.get("frames", {})
    if (counts["rx"] != declared.get("rx") or counts["tx"] != declared.get("tx")
            or counts["event"] != declared.get("event")):
        raise ReplayPackError("manifest_mismatch")
    return {
        "pack": str(directory),
        "segments": len(manifest["segments"]),
        "frames": counts,
        "payload_bytes": payload_bytes,
        "duration_us": manifest.get("timing", {}).get("duration_us"),
        "symbols": len(manifest.get("symbols", {})),
        "synthetic_control": manifest.get("frames", {}).get("synthetic_control", 0),
    }


def _parse_int(value: str) -> int:
    try:
        return int(value)
    except ValueError:
        raise argparse.ArgumentTypeError("integer required") from None


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Build or verify L2 replay packs from committed local caches")
    commands = parser.add_subparsers(dest="command", required=True)
    build = commands.add_parser("build", help="build a pack from a committed L2 cache")
    build.add_argument("--cache-root", type=Path, required=True)
    build.add_argument("--out", type=Path, required=True)
    build.add_argument("--start", type=_parse_int, default=1)
    build.add_argument("--end", type=_parse_int)
    build.add_argument("--segment-bytes", type=_parse_int, default=DEFAULT_SEGMENT_BYTES)
    build.add_argument("--no-synthetic-ack", action="store_true")
    verify = commands.add_parser("verify", help="verify a pack and print its summary")
    verify.add_argument("--pack", type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        if args.command == "build":
            manifest = build_replay_pack(ReplayPackConfig(
                cache_root=args.cache_root, output_dir=args.out,
                start_sequence=args.start, end_sequence=args.end,
                segment_bytes=args.segment_bytes,
                synthesize_bootstrap_ack=not args.no_synthetic_ack))
            print(json.dumps({"output": str(args.out), "frames": manifest["frames"],
                              "segments": len(manifest["segments"]),
                              "duration_us": manifest["timing"]["duration_us"]},
                             ensure_ascii=False))
        else:
            print(json.dumps(verify_replay_pack(args.pack), ensure_ascii=False))
    except (ReplayPackError, FrameTapError, CacheError) as error:
        print(json.dumps({"error_code": error.code}))
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
