"""Raw frame tap: compact format, zero-blocking writer, receive-layer wiring."""
from __future__ import annotations

import json
import socket
import threading
import time
from pathlib import Path

import pytest

from rquant.l2 import feed as feed_module
from rquant.l2.frame_tap import (FORMAT_NAME, KIND_EVENT, KIND_RX, KIND_TX, MANIFEST_NAME,
                                 FrameTapConfig, FrameTapError, RawFrameTap, iter_records,
                                 read_manifest, split_wire_frame, verify_tap_file, wire_frame)
from tests.unit.test_l2_feed import Clock, Provider, collector


class TapClock:
    """Hands out explicit microsecond values; start() consumes the first one."""

    def __init__(self, values: list[int]) -> None:
        self._values = list(values)

    def __call__(self) -> int:
        if not self._values:
            raise AssertionError("tap clock exhausted")
        return self._values.pop(0)


class SlowSink:
    """Blocks the writer thread on its first write until the test releases it."""

    def __init__(self, started: threading.Event, release: threading.Event) -> None:
        self.started, self.release = started, release
        self.data = bytearray()
        self.closed = False

    def write(self, data: bytes) -> None:
        if not self.started.is_set():
            self.started.set()
            if not self.release.wait(10):
                raise TimeoutError("test sink not released")
        self.data.extend(data)

    def flush(self) -> None:
        return None

    def fsync(self) -> None:
        return None

    def close(self) -> None:
        self.closed = True


def test_config_from_env_switch_defaults_and_bounds(tmp_path: Path) -> None:
    assert FrameTapConfig.from_env({}) is None
    assert FrameTapConfig.from_env({"RQUANT_L2_FRAME_TAP": "0"}) is None
    assert FrameTapConfig.from_env({"RQUANT_L2_FRAME_TAP": "off"}) is None
    config = FrameTapConfig.from_env({"RQUANT_L2_FRAME_TAP": "1",
                                      "RQUANT_L2_FRAME_TAP_DIR": str(tmp_path / "tap"),
                                      "RQUANT_L2_FRAME_TAP_SEGMENT_BYTES": "4096",
                                      "RQUANT_L2_FRAME_TAP_QUEUE_BYTES": "65536",
                                      "RQUANT_L2_FRAME_TAP_FSYNC_SECONDS": "0.5"})
    assert config is not None
    assert config.directory == tmp_path / "tap"
    assert config.segment_bytes == 4096
    assert config.queue_bytes == 65536
    assert config.fsync_interval_seconds == 0.5
    # A bare directory selects recording; the flag is optional.
    assert FrameTapConfig.from_env({"RQUANT_L2_FRAME_TAP_DIR": str(tmp_path / "t")}) is not None
    for invalid in ({"RQUANT_L2_FRAME_TAP": "1"},
                    {"RQUANT_L2_FRAME_TAP": "maybe"},
                    {"RQUANT_L2_FRAME_TAP": "1", "RQUANT_L2_FRAME_TAP_DIR": str(tmp_path),
                     "RQUANT_L2_FRAME_TAP_SEGMENT_BYTES": "1"}):
        with pytest.raises(FrameTapError, match="invalid_config"):
            FrameTapConfig.from_env(invalid)


def test_tap_roundtrip_preserves_wire_frames_and_monotonic_deltas(tmp_path: Path) -> None:
    clock = TapClock([10_000, 10_500, 11_000, 12_250, 12_260, 12_500])
    config = FrameTapConfig(directory=tmp_path / "tap", segment_bytes=1 << 20,
                            queue_bytes=1 << 20)
    with RawFrameTap(config, clock_us=clock,
                     wall_clock_us=lambda: 1_700_000_000_000_000) as tap:
        assert tap.record_rx(b"abc")
        assert tap.record_rx(b"")
        assert tap.record_tx(wire_frame(b"request"))
        assert tap.record_event({"event": "epoch_start", "epoch_id": "e"})
        assert tap.record_rx(b"def")
    manifest = read_manifest(tmp_path / "tap")
    assert manifest is not None
    assert manifest["format"] == FORMAT_NAME
    assert manifest["records"] == {"rx": 3, "tx": 1, "event": 1}
    assert manifest["dropped"] == {"frames": 0, "bytes": 0}
    assert manifest["error"] is None
    assert len(manifest["segments"]) == 1
    records = list(iter_records(tmp_path / "tap" / manifest["segments"][0]["file"]))
    assert [record.kind for record in records] == [KIND_RX, KIND_RX, KIND_TX, KIND_EVENT, KIND_RX]
    assert [split_wire_frame(record.payload) for record in records[:3]] == [b"abc", b"", b"request"]
    assert json.loads(records[3].payload) == {"event": "epoch_start", "epoch_id": "e"}
    assert split_wire_frame(records[4].payload) == b"def"
    assert [record.mono_us for record in records] == [10_500, 11_000, 12_250, 12_260, 12_500]
    summary = verify_tap_file(tmp_path / "tap" / manifest["segments"][0]["file"])
    assert summary == {"file": manifest["segments"][0]["file"], "records": 5, "rx": 3, "tx": 1,
                       "event": 1, "payload_bytes": sum(len(r.payload) for r in records),
                       "first_mono_us": 10_500, "last_mono_us": 12_500}


def test_tap_heartbeat_zero_body_is_a_valid_frame(tmp_path: Path) -> None:
    config = FrameTapConfig(directory=tmp_path / "tap")
    with RawFrameTap(config) as tap:
        assert tap.record_rx(b"")
    manifest = read_manifest(tmp_path / "tap")
    assert manifest is not None
    records = list(iter_records(tmp_path / "tap" / manifest["segments"][0]["file"]))
    assert len(records) == 1
    assert records[0].payload == b"\xfd\xfd\xfd\xfd" + b"00000000" + b"\x00"


def test_tap_rotates_segments_and_reassembles_the_same_stream(tmp_path: Path) -> None:
    config = FrameTapConfig(directory=tmp_path / "tap", segment_bytes=4096,
                            queue_bytes=1 << 20)
    expected = [f"payload-{index:04d}".encode() for index in range(300)]
    with RawFrameTap(config) as tap:
        for payload in expected:
            assert tap.record_rx(payload)
    manifest = read_manifest(tmp_path / "tap")
    assert manifest is not None
    assert len(manifest["segments"]) > 1
    records = []
    for segment in manifest["segments"]:
        records.extend(iter_records(tmp_path / "tap" / segment["file"]))
    assert [split_wire_frame(record.payload) for record in records] == expected
    assert [record.mono_us for record in records] == sorted(record.mono_us for record in records)


def test_tap_detects_corruption_and_truncation(tmp_path: Path) -> None:
    config = FrameTapConfig(directory=tmp_path / "tap")
    with RawFrameTap(config) as tap:
        for index in range(4):
            assert tap.record_rx(f"frame-{index}".encode())
    manifest = read_manifest(tmp_path / "tap")
    assert manifest is not None
    path = tmp_path / "tap" / manifest["segments"][0]["file"]
    data = bytearray(path.read_bytes())
    data[-20] ^= 0xFF
    path.write_bytes(bytes(data))
    with pytest.raises(FrameTapError, match="checksum_mismatch"):
        list(iter_records(path))
    path.write_bytes(bytes(data[:-20]))
    with pytest.raises(FrameTapError, match="truncated"):
        list(iter_records(path))


def test_tap_submit_never_waits_for_a_stalled_sink(tmp_path: Path) -> None:
    started, release = threading.Event(), threading.Event()
    config = FrameTapConfig(directory=tmp_path / "tap", queue_bytes=64 * 1024,
                            queue_records=512, segment_bytes=1 << 20)
    tap = RawFrameTap(config, sink_factory=lambda path: SlowSink(started, release))
    tap.start()
    try:
        assert started.wait(10)
        began = time.perf_counter()
        accepted = sum(1 for _ in range(5000) if tap.record_rx(b"x" * 256))
        elapsed = time.perf_counter() - began
        stats = tap.stats
        assert accepted > 0
        assert stats["dropped_frames"] > 0
        assert elapsed < 1.0, f"submit blocked on the writer: {elapsed:.3f}s"
    finally:
        release.set()
        manifest = tap.close()
    written = manifest["records"].get("rx", 0)
    dropped = manifest["dropped"]["frames"]
    assert written + dropped == 5000
    assert manifest["error"] is None


def test_tap_writer_failure_is_contained(tmp_path: Path) -> None:
    class FailingSink:
        def write(self, data: bytes) -> None:
            raise OSError("simulated disk failure")

        def flush(self) -> None:
            return None

        def fsync(self) -> None:
            return None

        def close(self) -> None:
            return None

    config = FrameTapConfig(directory=tmp_path / "tap")
    tap = RawFrameTap(config, sink_factory=lambda path: FailingSink())
    tap.start()
    for index in range(20):
        tap.record_rx(f"frame-{index}".encode())
    manifest = tap.close()
    assert manifest["error"] == "writer_failed"
    assert tap.stats["error"] == "writer_failed"
    assert manifest["records"].get("rx", 0) + manifest["dropped"]["frames"] <= 20


def test_tap_rejects_malformed_tx_wire_frames(tmp_path: Path) -> None:
    config = FrameTapConfig(directory=tmp_path / "tap")
    with RawFrameTap(config) as tap:
        assert tap.record_tx(b"not-a-frame") is False
        assert tap.record_tx(b"\xfd" * 4 + b"00000003\x00abc") is True
    manifest = read_manifest(tmp_path / "tap")
    assert manifest is not None
    assert manifest["invalid_frames"] == 1
    assert manifest["records"]["tx"] == 1


def test_tap_publishes_manifest_after_clean_close(tmp_path: Path) -> None:
    config = FrameTapConfig(directory=tmp_path / "tap")
    tap = RawFrameTap(config)
    tap.start()
    assert (tmp_path / "tap").is_dir()
    assert tap.record_rx(b"abc") is True
    first = tap.close()
    second = tap.close()
    assert first == second
    assert (tmp_path / "tap" / MANIFEST_NAME).is_file()
    data = json.loads((tmp_path / "tap" / MANIFEST_NAME).read_text())
    assert data["config"]["segment_bytes"] == config.segment_bytes
    assert data["records"]["rx"] == 1


def test_receive_layer_records_tx_requests_rx_frames_and_epoch_events(tmp_path: Path) -> None:
    clock = Clock()
    provider = Provider(clock)
    config = FrameTapConfig(directory=tmp_path / "tap", segment_bytes=1 << 20,
                            queue_bytes=1 << 20)
    tap = RawFrameTap(config)
    obj, cache, calls = collector(tmp_path, provider, clock, frame_tap=tap)
    status = obj.run()
    assert status.error_code == "duration_limit"
    assert len(calls) == 1
    manifest = read_manifest(tmp_path / "tap")
    assert manifest is not None
    assert manifest["error"] is None
    assert manifest["dropped"]["frames"] == 0
    records = []
    for segment in manifest["segments"]:
        records.extend(iter_records(tmp_path / "tap" / segment["file"]))
    tx = [record.payload for record in records if record.kind == KIND_TX]
    assert tx == provider.sent
    rx = [split_wire_frame(record.payload) for record in records if record.kind == KIND_RX]
    kinds = [feed_module._control_kind(body) for body in rx]
    assert "ack" in kinds and "heartbeat" in kinds
    cached_bodies = [bytes.fromhex(batch.response_body_hex) for batch in cache.iter_batches()]
    recorded_market = [body for body in rx if feed_module._control_kind(body) is None]
    assert recorded_market == cached_bodies
    events = [json.loads(record.payload) for record in records if record.kind == KIND_EVENT]
    assert {event["event"] for event in events} >= {"epoch_start", "epoch_end"}
    assert tap.stats["active"] is False


def test_recorded_session_replays_into_a_second_collector(tmp_path: Path) -> None:
    (tmp_path / "one").mkdir()
    (tmp_path / "two").mkdir()
    clock = Clock()
    provider = Provider(clock)
    tap = RawFrameTap(FrameTapConfig(directory=tmp_path / "tap", segment_bytes=1 << 20,
                                     queue_bytes=1 << 20))
    first, first_cache, first_calls = collector(tmp_path / "one", provider, clock, frame_tap=tap)
    first_status = first.run()
    assert first_status.market_batch_count == 6
    first_bodies = [bytes.fromhex(batch.response_body_hex) for batch in first_cache.iter_batches()]
    manifest = read_manifest(tmp_path / "tap")
    assert manifest is not None
    recorded_rx = []
    for segment in manifest["segments"]:
        recorded_rx.extend(record.payload for record in iter_records(tmp_path / "tap" / segment["file"])
                           if record.kind == KIND_RX)

    groups: dict[int, list[bytes]] = {}
    current = 0
    for wire in recorded_rx:
        body = split_wire_frame(wire)
        request = int.from_bytes(body[2:6], "little") if len(body) >= 6 else 0
        if request in (0, 0xFFFFFFFD):
            groups.setdefault(current, []).append(wire)
        else:
            current = request
            groups.setdefault(current, []).append(wire)

    class ReplayProvider:
        """Request-gated replay: release recorded RX frames on the matching TX."""

        def __init__(self, grouped: dict[int, list[bytes]], clock: Clock) -> None:
            self.groups, self.clock = grouped, clock
            self.ready: list[bytes] = []
            self.sent: list[bytes] = []
            self.timeout, self.closed = 1.0, False

        def settimeout(self, value: float) -> None:
            self.timeout = value

        def sendall(self, data: bytes) -> None:
            self.sent.append(data)
            body = data[13:]
            request = int.from_bytes(body[2:6], "little") if len(body) >= 6 else 0
            self.ready.extend(self.groups.get(request, []))

        def recv(self, size: int) -> bytes:
            if not self.ready:
                self.clock.sleep(self.timeout)
                raise socket.timeout()
            wire = self.ready.pop(0)
            if len(wire) > size:
                self.ready.insert(0, wire[size:])
                wire = wire[:size]
            return wire

        def close(self) -> None:
            self.closed = True

    replay_clock = Clock()
    replay_provider = ReplayProvider(groups, replay_clock)
    second, second_cache, second_calls = collector(tmp_path / "two", replay_provider, replay_clock)
    second_status = second.run()
    assert len(second_calls) == 1 and len(first_calls) == 1
    assert len(replay_provider.sent) == len(provider.sent) == 7
    second_bodies = [bytes.fromhex(batch.response_body_hex) for batch in second_cache.iter_batches()]
    assert second_bodies == first_bodies
    assert second_status.market_batch_count == 6
    assert replay_provider.closed
