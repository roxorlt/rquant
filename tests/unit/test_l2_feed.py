"""Offline single-reader, registry and lifetime failure cases."""
from __future__ import annotations

import socket
import struct
import json
from pathlib import Path

import pytest

from tests.unit.test_l2_codec import bootstrap, frame, module, response, template


class Clock:
    def __init__(self) -> None:
        self.now = 100.0
    def __call__(self) -> float:
        return self.now
    def sleep(self, seconds: float) -> None:
        self.now += seconds


class Socket:
    def __init__(self, chunks: list[bytes | BaseException], clock: Clock) -> None:
        self.chunks, self.clock = list(chunks), clock
        self.sent: list[bytes] = []
        self.closed = False
        self.timeout = 1.0
    def settimeout(self, value: float) -> None:
        self.timeout = value
    def sendall(self, data: bytes) -> None:
        self.sent.append(data)
    def recv(self, size: int) -> bytes:
        if not self.chunks:
            self.clock.sleep(self.timeout)
            raise socket.timeout()
        item = self.chunks.pop(0)
        if isinstance(item, BaseException):
            self.clock.sleep(min(self.timeout, 0.25))
            raise item
        if len(item) > size:
            self.chunks.insert(0, item[size:])
        return item[:size]
    def close(self) -> None:
        self.closed = True


def test_frame_reader_preserves_partial_bytes_across_idle_and_zero_heartbeat() -> None:
    feed = module("feed")
    clock = Clock()
    wire = frame(b"abc") + frame(b"") + frame(b"def")
    sock = Socket([wire[:7], socket.timeout(), wire[7:]], clock)
    reader = feed.FrameReader(sock, clock=clock, partial_timeout=2.0)
    assert reader.read(deadline=110.0) is None
    assert reader.read(deadline=110.0) == b"abc"
    assert reader.read(deadline=110.0) == b""
    assert reader.read(deadline=110.0) == b"def"
    assert reader.ordinal == 3


def test_frame_reader_rejects_oversize_malformed_and_partial_assembly_timeout() -> None:
    feed = module("feed")
    for chunks, limit in [([b"\xfd" * 4 + b"ffffffff\0"], 100), ([b"x" * 13], 100)]:
        clock = Clock()
        reader = feed.FrameReader(Socket(chunks, clock), clock=clock, max_frame_bytes=limit)
        with pytest.raises(feed.FeedError):
            reader.read(deadline=110.0)
    clock = Clock()
    reader = feed.FrameReader(Socket([b"\xfd", socket.timeout()], clock), clock=clock, partial_timeout=0.1)
    with pytest.raises(feed.FeedError, match="frame_timeout"):
        reader.read(deadline=110.0)


def test_next_partial_frame_uses_its_own_receive_time() -> None:
    feed = module("feed")
    clock = Clock()
    first, second = frame(b"first"), frame(b"second")
    events = [(100.0, first[:7]), (110.0, first[7:] + second[:7]), (116.0, second[7:])]
    class TimedSocket(Socket):
        def recv(self, size):
            arrival, data = events[0]
            if clock() + self.timeout < arrival:
                clock.sleep(self.timeout)
                raise socket.timeout()
            clock.now = arrival
            events.pop(0)
            assert len(data) <= size
            return data
    reader = feed.FrameReader(TimedSocket([], clock), clock=clock)
    result = None
    while result is None:
        result = reader.read(deadline=130)
    assert result == b"first" and clock() == 110
    result = None
    while result is None:
        result = reader.read(deadline=130)
    assert result == b"second" and clock() == 116
    assert reader.ordinal == 2 and not reader.buffer


def control(auth: bool = False) -> bytes:
    size = 887 if auth else 131
    body = struct.pack("<HIIHIII", 26, 0 if auth else 0xFFFFFFFD, 6 if auth else 11,
                       0, size - 26, 2201 if auth else 0, 0) + b"xx"
    return frame(body + (b"SYNTHETIC_PRIVATE_SENTINEL" + b"x" * size)[:size - 26])


def session_fields(client_ip: str = "192.0.2.123") -> dict:
    return {"AuthCountryCodeMd5": "a" * 32, "AuthPubRSAVer": "1.2.3.456",
            "clientIp": client_ip, "clientPort": "54321", "releaseName": "SYNTHETIC-TEST",
            "serverIp": "198.51.100.9", "sessionId": "SYNTHETIC_CONTROL_ONLY_".ljust(36, "X"),
            "srvTime": 1700000000}


def session_control(*, size: int = 379, fields: dict | None = None,
                    text: bytes | None = None, carrier: int = 0x61) -> bytes:
    """Entirely synthetic session metadata; never derived from private input."""
    if text is None:
        data = session_fields("192.168.100.123" if size == 383 else "192.0.2.123") if fields is None else fields
        text = json.dumps(data, separators=(",", ":")).encode()
    decoded_size = 263 if size == 379 else 267
    assert len(text) <= decoded_size
    encoded = module("_request").encode_payload(text.ljust(decoded_size, b" "))
    payload = encoded + bytes([carrier])  # Opaque carrier outside the encoded JSON.
    assert len(payload) == size - 26
    return struct.pack("<HIIHIII", 26, 0xFFFFFFFD, 0x1000B, 0, len(payload), 0, 0) + b"xx" + payload


@pytest.mark.parametrize("size", [379, 383])
def test_exact_session_metadata_is_control_only(size: int) -> None:
    feed = module("feed")
    assert feed._control_kind(session_control(size=size)) == "control"
    assert feed._control_kind(control()[13:]) == "control"
    assert feed._control_kind(control(auth=True)[13:]) == "ack"


@pytest.mark.parametrize("size", [379, 383])
def test_session_metadata_accepts_every_opaque_carrier_value(size: int) -> None:
    feed = module("feed")
    for carrier in range(256):
        assert feed._control_kind(session_control(size=size, carrier=carrier)) == "control"


@pytest.mark.parametrize("fault", [
    "header", "request", "flags", "page", "command", "declared", "size",
    "base64", "padbits", "json", "utf8", "duplicate", "extra", "error", "missing",
    "object", "type", "bool", "ip", "port", "md5", "version", "version_bound",
    "release", "session", "time_bound",
])
def test_session_metadata_rejects_unknown_or_malformed_profiles(fault: str) -> None:
    fields = session_fields()
    fields.update(AuthPubRSAVer="1", clientIp="1.1.1.1", clientPort="1",
                  releaseName="r", serverIp="2.2.2.2", srvTime=0)
    text = None
    if fault == "duplicate":
        text = json.dumps(fields, separators=(",", ":")).encode()[:-1] + b',"clientPort":"1"}'
    if fault == "extra": fields["unknown"] = 1
    if fault == "error": fields["error"] = "refused"
    if fault == "missing": del fields["AuthPubRSAVer"]
    if fault == "object": text = b'[]'
    if fault == "type": fields["clientPort"] = 1
    if fault == "bool": fields["srvTime"] = True
    if fault == "ip": fields["clientIp"] = "999.1.1.1"
    if fault == "port": fields["clientPort"] = "00001"
    if fault == "md5": fields["AuthCountryCodeMd5"] = "z" * 32
    if fault == "version": fields["AuthPubRSAVer"] = "\n"
    if fault == "version_bound": fields["AuthPubRSAVer"] = "v" * 33
    if fault == "release": fields["releaseName"] = ""
    if fault == "session": fields["sessionId"] = "x" * 35
    if fault == "time_bound": fields["srvTime"] = 0x100000000
    if fault == "json": text = b'{broken'
    if fault == "utf8": text = b'\xff'
    body = bytearray(session_control(size=383, fields=fields, text=text))
    mutations = {"header": (0, "<H", 27), "request": (2, "<I", 0),
                 "flags": (6, "<I", 0x1000C), "page": (10, "<H", 1),
                 "command": (16, "<I", 2357), "declared": (12, "<I", 356)}
    if fault in mutations:
        offset, fmt, value = mutations[fault]
        struct.pack_into(fmt, body, offset, value)
    if fault == "size":
        body.extend(b"aaaa")
        struct.pack_into("<I", body, 12, len(body) - 26)
    if fault == "base64": body[26] = ord("!")
    if fault == "padbits":
        body = bytearray(session_control())  # 263 bytes has one '=' padding byte.
        alphabet = module("_request").IPHONE64_ALPHABET
        body[-3] = alphabet[alphabet.index(body[-3]) | 1]
    assert module("feed")._control_kind(bytes(body)) is None


class Provider(Socket):
    def __init__(self, clock: Clock, *, late: bool = False, fault: str = "", eof: bool = False,
                 repeat_history: bool = False) -> None:
        super().__init__([], clock)
        self.late, self.fault, self.eof, self.repeat_history = late, fault, eof, repeat_history
        self.business: list[tuple[float, dict]] = []
        self.last: bytes | None = None
    def sendall(self, data: bytes) -> None:
        super().sendall(data)
        if len(self.sent) == 1:
            self.chunks.append(control(auth=True))
            return
        req = module("_request")
        body = data[13:]
        command = int.from_bytes(body[16:20], "little")
        if command == 2357:
            raw = req._read_request_body(body, frame_id=2357)
            route = req._parse_ini_route(raw['plaintext'], frame_id=2357)
            is_trade = route['parameters']['id'] == '4013'
            if is_trade:
                parsed = req.parse_trade_request_body(body)
            else:
                req._validate_order_parameters(route['parameters'])
                parsed = {**raw, **route, 'kind': 'order_query'}
        else:
            is_trade = False
            parsed = req.parse_request_body(body)
        self.business.append((self.clock(), parsed))
        p = parsed["parameters"]
        page = int(p.get("currentpage", "1"))
        uid = 100 if self.repeat_history else (102 - 2 * page if is_trade else int(p["orderid"]) - 2 if p["orderid"] != "0" else 100)
        side = "sell" if self.fault == "side" else p.get("reqtype", "buy")
        answer = response(trade=is_trade, request_id=parsed["request_id"], page_id=int(parsed["page_id"]),
                          code=p["stockcode"], side=side, ids=(uid, uid + 1))
        if not is_trade and command == 2357:
            common = bytearray(answer)
            struct.pack_into('<I', common, 16, 2357)
            answer = bytes(common)  # Synthetic provider response to actual requested frame.
        if self.late and len(self.business) == 2:
            self.chunks.append(frame(self.last))
        self.last = answer
        changed = bytearray(answer)
        for fault, offset, fmt, value in [("id", 2, "<I", 0), ("page", 10, "<H", 65000),
                                          ("command", 16, "<I", 2267), ("flags", 6, "<I", 0x1150)]:
            if self.fault == fault:
                struct.pack_into(fmt, changed, offset, value)
        if self.fault in {'legacy', 'legacy_push'} and not is_trade:
            struct.pack_into('<I', changed, 16, 2267)
        actual = sentinel_push(bytes(changed)) if self.fault == 'legacy_push' and not is_trade else bytes(changed)
        self.chunks.append(frame(actual))
        if len(self.business) == 6:
            self.chunks.extend([frame(b""), control(), b""] if self.eof else [frame(b""), control()])


# Root-filtered actual issued4013 market notice; contains no auth/control body.
NO_DATA_4013 = bytes.fromhex(
    "2000030000001101000085271a000000350900000000000000ff00000c000000"
    "0400fb7cdf7ee14f6f600400e06570656e6301ff040000000000")


class NoDataProvider(Provider):
    def __init__(self, clock, *, fault="", late_notice=False, close_after_notice=False):
        super().__init__(clock, late=late_notice)
        self.notice_fault = fault
        self.close_after_notice = close_after_notice
    def sendall(self, data):
        insertion = len(self.chunks)
        super().sendall(data)
        if len(self.sent) == 1 or self.business[-1][1]['kind'] != 'vendor4013':
            return
        body = bytearray(NO_DATA_4013)
        struct.pack_into("<I", body, 2, int.from_bytes(data[15:19], "little"))
        struct.pack_into("<H", body, 10, int(self.business[-1][1]['page_id']))
        if self.notice_fault == "id": struct.pack_into("<I", body, 2, 0)
        if self.notice_fault == "sentinel": struct.pack_into("<I", body, 2, 0xFFFFFFFF)
        if self.notice_fault == "page": struct.pack_into("<H", body, 10, 65000)
        if self.notice_fault == "command": struct.pack_into("<I", body, 16, 2267)
        if self.notice_fault == "flags": struct.pack_into("<I", body, 6, 0x112)
        if self.notice_fault == "header": struct.pack_into("<H", body, 0, 31)
        if self.notice_fault == "size": struct.pack_into("<I", body, 12, 25)
        if self.notice_fault == "text": body[36] ^= 1
        if self.notice_fault == "trailer": body[-1] ^= 1
        self.last = bytes(body)
        self.chunks[insertion] = frame(self.last)
        if self.close_after_notice: self.chunks.append(b"")


def sentinel_push(body: bytes) -> bytes:
    header = bytearray(body[:26])
    struct.pack_into("<H", header, 0, 26)
    struct.pack_into("<I", header, 2, 0xFFFFFFFF)
    struct.pack_into("<I", header, 6, 0x1100)
    return bytes(header) + body[32:]


class InterleavedPushProvider(Provider):
    def __init__(self, clock):
        super().__init__(clock)
        self.seed_bodies = []
    def sendall(self, data):
        insertion = len(self.chunks)
        super().sendall(data)
        if len(self.sent) == 1: return
        self.seed_bodies.append(self.last)
        chosen = {2: [0], 4: [1, 2], 6: [3, 4, 5]}.get(len(self.business), [])
        pushes = [frame(sentinel_push(self.seed_bodies[index])) for index in chosen]
        self.chunks[insertion:insertion] = pushes


def collector(tmp_path: Path, provider: Provider, clock: Clock, **options):
    feed = module("feed")
    assert hasattr(feed, "FeedCollector"), "FeedCollector is not implemented"
    from rquant.l2.cache import RawL2Cache
    for name, data in (("bootstrap", bootstrap()), ("order", template()), ("trade", template(True))):
        path = tmp_path / name
        path.write_bytes(data)
        path.chmod(0o600)
    cache = RawL2Cache(tmp_path / "cache")
    calls = []
    def connect(endpoint, timeout):
        calls.append(endpoint)
        return provider
    obj = feed.FeedCollector(cache, bootstrap_path=tmp_path / "bootstrap",
                             order_template_path=tmp_path / "order", trade_template_path=tmp_path / "trade",
                             duration_seconds=options.pop("duration_seconds", 20), clock=clock, sleep=clock.sleep,
                             connection_factory=connect, **options)
    return obj, cache, calls


def test_explicit_resident_remains_passive_beyond_six_hours_and_stops(tmp_path: Path) -> None:
    class ResidentProvider(Provider):
        def recv(self, size):
            if not self.chunks and len(self.business) >= 6:
                clock.sleep(21601)
                obj.stop_event.set()
                raise socket.timeout()
            return super().recv(size)
    clock = Clock(); provider = ResidentProvider(clock)
    obj, cache, calls = collector(tmp_path, provider, clock, duration_seconds=None)
    status = obj.run()
    assert clock() > 100 + 21600 and obj._deadline == float("inf")
    assert len(calls) == 1 and len(provider.sent) == 7 and len(provider.business) == 6
    assert status.state == "stopped" and status.error_code == "signal_stop"
    assert status.market_batch_count == 6 and status.reconnect_attempts == 0
    assert provider.closed and obj._inputs == (b"", b"", b"")
    assert cache.read_status().gap


@pytest.mark.parametrize("duration", [float("nan"), float("inf"), -float("inf"), 0, -1, True])
def test_collector_still_rejects_invalid_numeric_duration(tmp_path: Path, duration) -> None:
    feed = module("feed")
    from rquant.l2.cache import RawL2Cache
    with pytest.raises(ValueError, match="invalid duration"):
        feed.FeedCollector(RawL2Cache(tmp_path / "cache"), bootstrap_path=tmp_path / "bootstrap",
                           order_template_path=tmp_path / "order", trade_template_path=tmp_path / "trade",
                           duration_seconds=duration)
    assert not (tmp_path / "cache").exists()


def test_session_metadata_does_not_complete_pending_or_refresh_market_or_leak(tmp_path: Path, monkeypatch) -> None:
    control_bodies = [session_control(carrier=0), session_control(size=383, carrier=255)]
    class SessionProvider(Provider):
        def sendall(self, data):
            insertion = len(self.chunks)
            super().sendall(data)
            if len(self.sent) == 2:
                self.chunks[insertion:insertion] = [frame(body) for body in control_bodies]
    clock = Clock(); provider = SessionProvider(clock)
    obj, cache, calls = collector(tmp_path, provider, clock)
    statuses = []
    write_status = cache.write_status
    def observe(status):
        statuses.append(status)
        write_status(status)
    monkeypatch.setattr(cache, "write_status", observe)
    status = obj.run()
    assert len(calls) == 1 and len(provider.sent) == 7 and provider.closed
    assert status.market_batch_count == 6 and status.control_count == 4
    assert status.error_code == "duration_limit" and status.source_freshness == "unknown"
    metadata_status = next(s for s in statuses if s.control_count == 3)
    assert metadata_status.market_batch_count == 0 and metadata_status.last_market_received_at is None
    assert metadata_status.subscription_state == "requested_unverified"
    assert cache.read_status().last_sequence == 6
    for path in cache.root.rglob("*"):
        if path.is_file():
            data = path.read_bytes()
            assert b"SYNTHETIC_CONTROL_ONLY_" not in data
            assert b"AuthCountryCodeMd5" not in data
            assert all(body.hex().encode() not in data for body in control_bodies)


@pytest.mark.parametrize("before_ack", [False, True])
def test_session_metadata_is_neither_ack_nor_validated_seed_for_retry(tmp_path: Path, before_ack: bool) -> None:
    class SessionOnlyProvider(Provider):
        def sendall(self, data):
            super().sendall(data)
            if len(self.sent) == (1 if before_ack else 2):
                self.chunks[:] = [frame(session_control(carrier=0x1f)), b""]
    clock = Clock(); provider = SessionOnlyProvider(clock)
    obj, cache, calls = collector(tmp_path, provider, clock, max_reconnects=2)
    status = obj.run()
    assert len(calls) == 1 and len(provider.sent) == (1 if before_ack else 2) and provider.closed
    assert not obj.validated_seed and status.reconnect_attempts == 0
    assert status.error_code == "peer_closed" and status.market_batch_count == 0
    assert status.last_market_received_at is None and status.source_freshness == "unknown"
    assert cache.read_status().last_sequence == 0 and status.cache_stale


@pytest.mark.parametrize("fault", ["duplicate", "extra", "error", "malformed"])
def test_invalid_session_metadata_stops_without_cache_or_replay(tmp_path: Path, fault: str) -> None:
    fields = session_fields()
    fields.update(AuthPubRSAVer="1", releaseName="r")
    text = None
    if fault == "duplicate":
        text = json.dumps(fields, separators=(",", ":")).encode()[:-1] + b',"clientPort":"1"}'
    if fault == "extra": fields["unknown"] = 1
    if fault == "error": fields["error"] = "refused"
    if fault == "malformed": text = b'{broken'
    body = session_control(size=383, fields=fields, text=text)
    class InvalidSessionProvider(Provider):
        def sendall(self, data):
            insertion = len(self.chunks)
            super().sendall(data)
            if len(self.sent) == 2: self.chunks[insertion:insertion] = [frame(body)]
    clock = Clock(); provider = InvalidSessionProvider(clock)
    obj, cache, calls = collector(tmp_path, provider, clock, max_reconnects=2)
    status = obj.run()
    assert status.error_code == "association_mismatch" and status.market_batch_count == 0
    assert len(calls) == 1 and provider.closed and not obj.validated_seed
    assert status.last_market_received_at is None and cache.read_status().last_sequence == 0


def test_six_seeds_then_passive_immutable_binding_and_late_rows_retained(tmp_path: Path) -> None:
    clock = Clock(); provider = Provider(clock, late=True)
    obj, cache, calls = collector(tmp_path, provider, clock)
    status = obj.run()
    assert len(calls) == 1 and len(provider.sent) == 7
    assert len(provider.business) == 6
    assert all(b[0] - a[0] >= 1 for a, b in zip(provider.business, provider.business[1:]))
    active = set()
    for _, parsed in provider.business:
        pages = set(parsed["frame"]["pageList"].split(","))
        assert active <= pages
        active = pages
    assert len(active) == 6
    batches = cache.read_batches(limit=8).batches
    assert len(batches) == 7
    assert batches[0].table == batches[1].table
    assert batches[1].delivery == "candidate_push"
    assert batches[0].binding == batches[1].binding
    assert status.subscription_state == "candidate_push_observed"
    assert status.heartbeat_count == 1 and status.control_count >= 1
    assert provider.closed and status.source_freshness == "unknown"
    for path in (tmp_path / "cache").rglob("*"):
        if path.is_file():
            assert b"SYNTHETIC_PRIVATE_SENTINEL" not in path.read_bytes()


def test_actual_query_spacing_includes_variable_status_persistence(tmp_path: Path, monkeypatch) -> None:
    clock = Clock(); provider = Provider(clock)
    obj, cache, calls = collector(tmp_path, provider, clock)
    write_status = cache.write_status
    last_seed_queries = 0
    def delayed_write(status):
        nonlocal last_seed_queries
        write_status(status)
        if status.seed_queries > last_seed_queries:
            clock.sleep(0.04 if status.seed_queries % 2 else 0.005)
            last_seed_queries = status.seed_queries
    monkeypatch.setattr(cache, "write_status", delayed_write)
    obj.run()
    assert len(calls) == 1 and len(provider.business) == 6 and provider.closed
    intervals = [b[0] - a[0] for a, b in zip(provider.business, provider.business[1:])]
    assert min(intervals) >= 1.0, intervals


@pytest.mark.parametrize("fault", ["id", "page", "command", "flags", "side", "legacy", "legacy_push"])
def test_unknown_association_schema_and_order_side_never_retry(tmp_path: Path, fault: str) -> None:
    clock = Clock(); provider = Provider(clock, fault=fault)
    obj, cache, calls = collector(tmp_path, provider, clock, max_reconnects=2)
    status = obj.run()
    assert status.state == "blocked" and status.gap
    assert status.error_code in {"association_mismatch", "decode_error", "order_side_mismatch"}
    assert len(calls) == 1
    assert provider.closed
    assert "SYNTHETIC_PRIVATE_SENTINEL" not in status.model_dump_json()


def test_explicit_history_is_bounded_and_nonprogress_stops_honestly(tmp_path: Path) -> None:
    clock = Clock(); provider = Provider(clock)
    obj, cache, _ = collector(tmp_path, provider, clock, trade_pages=3, order_pages=3)
    obj.run()
    assert len(provider.business) == 18
    assert len(list((tmp_path / "cache" / "batches").iterdir())) == 18
    other = tmp_path / "repeat";other.mkdir()
    clock = Clock(); provider = Provider(clock, repeat_history=True)
    obj, cache, _ = collector(other, provider, clock, trade_pages=3)
    status = obj.run()
    assert status.state == "blocked" and status.gap and status.error_code == "protocol_error"
    assert len(provider.business) == 2


def test_writer_lock_precedes_any_private_read_or_connect(tmp_path: Path, monkeypatch) -> None:
    clock = Clock(); provider = Provider(clock)
    obj, cache, calls = collector(tmp_path, provider, clock)
    from rquant.l2.cache import RawL2Cache
    with RawL2Cache(cache.root).writer():
        monkeypatch.setattr(Path, "read_bytes", lambda *args: pytest.fail("private read before lock"))
        status = obj.run()
    assert status.error_code == "writer_busy" and calls == []


def test_only_postseed_eof_can_reconnect_after_durable_gap(tmp_path: Path) -> None:
    clock = Clock(); first = Provider(clock, eof=True); second = Provider(clock)
    obj, cache, calls = collector(tmp_path, first, clock, max_reconnects=1)
    def connect(endpoint, timeout):
        if calls:
            assert cache.read_status().gap
            assert clock() >= 165
        calls.append(endpoint)
        return first if len(calls) == 1 else second
    obj.connection_factory = connect
    obj.duration_seconds = 80
    status = obj.run()
    assert len(calls) == 2 and status.reconnect_attempts == 1 and status.gap
    assert first.closed and second.closed
    assert cache.read_status().last_sequence == 12


def test_initial_ack_timeout_does_not_replay_auth(tmp_path: Path) -> None:
    clock = Clock(); provider = Provider(clock)
    obj, cache, calls = collector(tmp_path, provider, clock, max_reconnects=2)
    provider.sendall = lambda data: provider.sent.append(data)
    status = obj.run()
    assert len(calls) == 1 and status.error_code == "seed_timeout"
    assert provider.closed


def test_unsupported_order_template_header_rejected_before_connect(tmp_path: Path) -> None:
    clock = Clock(); provider = Provider(clock)
    obj, cache, calls = collector(tmp_path, provider, clock)
    body = bytearray(template())
    del body[76]
    struct.pack_into("<H", body, 0, 76)
    obj.order_template_path.write_bytes(body)
    status = obj.run()
    assert status.error_code == "input_error" and status.state == "blocked"
    assert calls == [] and provider.sent == []


def test_duration_expired_during_status_write_does_not_connect(tmp_path: Path, monkeypatch) -> None:
    clock = Clock(); provider = Provider(clock)
    obj, cache, calls = collector(tmp_path, provider, clock)
    write_status = cache.write_status
    def expire(status):
        write_status(status)
        if status.epoch_id is not None:
            clock.now = 120.0
    monkeypatch.setattr(cache, "write_status", expire)
    status = obj.run()
    assert status.error_code == "duration_limit" and status.state == "stopped"
    assert calls == [] and provider.sent == []


def test_cache_failure_closes_without_retry_or_upstream_error_text(tmp_path: Path, monkeypatch) -> None:
    clock = Clock(); provider = Provider(clock)
    obj, cache, calls = collector(tmp_path, provider, clock, max_reconnects=2)
    def fail_append(observation):
        raise RuntimeError("SYNTHETIC_PRIVATE_SENTINEL")
    monkeypatch.setattr(cache, "append", fail_append)
    status = obj.run()
    assert status.state == "blocked" and status.error_code == "cache_error" and status.gap
    assert len(calls) == 1 and provider.closed
    assert "SYNTHETIC_PRIVATE_SENTINEL" not in status.model_dump_json()


def test_exact_4013_no_data_completes_only_its_request_without_history_or_cache(tmp_path: Path, monkeypatch) -> None:
    clock = Clock(); provider = NoDataProvider(clock, late_notice=True)
    obj, cache, calls = collector(tmp_path, provider, clock, trade_pages=3)
    statuses = []
    write_status = cache.write_status
    def observe(status):
        statuses.append(status)
        write_status(status)
    monkeypatch.setattr(cache, "write_status", observe)
    status = obj.run()
    assert len(calls) == 1 and len(provider.business) == 6 and provider.closed
    assert status.error_code == "duration_limit" and status.market_batch_count == 4
    assert status.subscription_state == "requested_unverified"
    assert status.control_count == 5  # ACK, two pending notices, old notice, final control.
    first_notice = next(s for s in statuses if s.control_count == 2)
    assert first_notice.market_batch_count == 0 and first_notice.last_market_received_at is None
    batches = cache.read_batches(limit=8).batches
    assert len(batches) == 4 and all(b.binding.kind == "vendor4027" for b in batches)
    assert [p['parameters'].get('currentpage') for _, p in provider.business if p['kind'] == 'vendor4013'] == ['1', '1']
    assert all(bytes.fromhex(b.response_body_hex) != NO_DATA_4013 for b in batches)


@pytest.mark.parametrize("fault", ["id", "sentinel", "page", "command", "flags", "header", "size", "text", "trailer"])
def test_no_data_notice_rejects_wrong_association_or_exact_profile(tmp_path: Path, fault: str) -> None:
    clock = Clock(); provider = NoDataProvider(clock, fault=fault)
    obj, cache, calls = collector(tmp_path, provider, clock, max_reconnects=2)
    status = obj.run()
    assert status.state == "blocked" and status.error_code in {"association_mismatch", "decode_error"}
    assert len(calls) == 1 and provider.closed and status.market_batch_count == 0


def test_no_data_is_not_a_validated_seed_and_eof_cannot_replay_bootstrap(tmp_path: Path) -> None:
    clock = Clock(); provider = NoDataProvider(clock, close_after_notice=True)
    obj, cache, calls = collector(tmp_path, provider, clock, max_reconnects=2)
    status = obj.run()
    assert len(calls) == 1 and provider.closed and not obj.validated_seed
    assert status.error_code == "peer_closed" and status.market_batch_count == 0
    assert status.last_market_received_at is None and status.subscription_state == "requested_unverified"
    assert status.source_freshness == "unknown" and status.cache_stale


def test_interleaved_sentinel_pushes_route_immutable_streams_and_never_complete_seeds(tmp_path: Path) -> None:
    clock = Clock(); provider = InterleavedPushProvider(clock)
    obj, cache, calls = collector(tmp_path, provider, clock)
    status = obj.run()
    assert len(calls) == 1 and provider.closed and status.error_code == "duration_limit"
    first = cache.read_batches(limit=8)
    batches = first.batches + cache.read_batches(after_sequence=first.next_sequence, limit=8).batches
    seeds = [b for b in batches if b.delivery == "seed"]
    pushes = [b for b in batches if b.delivery == "candidate_push"]
    assert len(seeds) == len(pushes) == 6 and status.market_batch_count == 12
    assert [b.binding.page_id for b in seeds] == [10117, 10118, 10119, 10127, 10128, 10129]
    assert [b.binding.page_id for b in pushes] == [10117, 10118, 10119, 10127, 10128, 10129]
    assert all(b.request_id != 0xFFFFFFFF for b in seeds)
    assert all(b.request_id == 0xFFFFFFFF and bytes.fromhex(b.response_body_hex)[:2] == b"\x1a\x00" for b in pushes)
    assert [(b.binding.code, b.binding.side) for b in pushes] == [
        ("603258", None), ("603258", "buy"), ("603258", "sell"),
        ("300006", None), ("300006", "buy"), ("300006", "sell")]


def test_sentinel_push_requires_an_issued_immutable_page_not_just_registration(tmp_path: Path) -> None:
    from datetime import UTC, datetime
    from rquant.l2.models import Binding
    feed = module("feed")
    clock = Clock(); provider = Provider(clock)
    obj, cache, calls = collector(tmp_path, provider, clock)
    body = sentinel_push(response(trade=True, request_id=3, page_id=10117))
    obj._register(Binding(page_id=10117, kind="vendor4013", code="603258", market=17))
    with cache.writer():
        with pytest.raises(feed.FeedError, match="association_mismatch"):
            obj._market(body, 1, None, "seed", datetime.now(UTC))
    assert calls == [] and cache.read_status().last_sequence == 0


def test_producer_uses_one_common_frame_with_exact_cumulative_page_prefix(tmp_path: Path) -> None:
    clock = Clock(); provider = InterleavedPushProvider(clock)
    obj, cache, calls = collector(tmp_path, provider, clock)
    status = obj.run()
    assert len(calls) == 1 and provider.closed and status.error_code == 'duration_limit'
    pages = []
    for wire, (_, parsed) in zip(provider.sent[1:], provider.business):
        assert int.from_bytes(wire[29:33], 'little') == 2357
        pages.append(parsed['page_id'])
        assert parsed['frame']['id'] == '2357'
        assert parsed['frame']['pageList'] == ','.join(pages)
    assert len(provider.sent) == 7 and status.market_batch_count == 12


class ManyPushProvider(Provider):
    def __init__(self, clock, *, count=40, missing=False):
        super().__init__(clock)
        self.count, self.missing = count, missing
        self.first = None
    def sendall(self, data):
        insertion = len(self.chunks)
        super().sendall(data)
        if len(self.business) == 1: self.first = self.last
        if len(self.business) == 2:
            if self.missing: self.chunks[insertion:] = []
            self.chunks[insertion:insertion] = [frame(sentinel_push(self.first))] * self.count
    def recv(self, size):
        if self.missing: self.clock.sleep(0.25)
        return super().recv(size)


def test_more_than_32_valid_pushes_do_not_abort_pending_seed(tmp_path):
    clock = Clock(); provider = ManyPushProvider(clock)
    obj, cache, calls = collector(tmp_path, provider, clock)
    status = obj.run()
    assert status.error_code == 'duration_limit'
    assert len(provider.business) == 6 and status.market_batch_count == 46
    assert len(calls) == 1 and provider.closed


@pytest.mark.parametrize('size', [20, 40, 80, 100])
def test_explicit_symbol_capacity_uses_one_socket_and_stable_three_stream_ids(tmp_path, size):
    clock = Clock(); provider = Provider(clock)
    codes = tuple(f'600{i:03}' for i in range(size))
    obj, cache, calls = collector(tmp_path, provider, clock, symbols=codes, max_symbols=size,
                                  duration_seconds=size * 4 + 10)
    status = obj.run()
    assert status.error_code == 'duration_limit' and len(calls) == 1 and provider.closed
    assert len(provider.business) == 3 * size
    for index, (_, parsed) in enumerate(provider.business):
        assert parsed['request_id'] == index + 3
        assert parsed['parameters']['stockcode'] == codes[index // 3]
        assert int(parsed['page_id']) == 10117 + 10 * (index // 3) + index % 3
    assert all(b[0] - a[0] >= 1 for a, b in zip(provider.business, provider.business[1:]))
    receipt = cache.read_subscription_status()
    assert len(receipt.streams) == 3 * size and receipt.collector_state == 'stopped'
    assert all(s.seed_sequence is not None for s in receipt.streams)
    assert (cache.root / 'subscription-status.json').stat().st_size < 256 * 1024


def test_empty_control_waits_without_reading_private_inputs_or_connecting(tmp_path, monkeypatch):
    clock = Clock(); provider = Provider(clock)
    obj, cache, calls = collector(tmp_path, provider, clock, control_root=tmp_path / 'control', duration_seconds=3)
    def forbidden(*args): pytest.fail('empty desired set read member inputs')
    monkeypatch.setattr(module('feed'), '_private_input', forbidden)
    status = obj.run()
    assert status.error_code == 'duration_limit' and not calls and not provider.sent
    with pytest.raises(ValueError):
        collector(tmp_path, provider, clock, control_root=tmp_path / 'control', symbols=['600000'])


def test_live_additions_and_duplicate_union_do_not_reseed_or_rebind(tmp_path):
    from rquant.l2.subscriptions import SubscriptionStore
    store = SubscriptionStore(tmp_path / 'control')
    store.add(['600000'])
    clock = Clock()
    class AddProvider(Provider):
        def sendall(self, data):
            super().sendall(data)
            if len(self.business) == 3:
                store.add(['000001', '600000'])
                self.chunks.extend([frame(sentinel_push(self.last))] * 50)
    provider = AddProvider(clock)
    obj, cache, calls = collector(tmp_path, provider, clock, control_root=store.root, duration_seconds=20)
    writes = []
    original = cache.write_subscription_status
    def receipt_written(receipt):
        writes.append(receipt)
        original(receipt)
    cache.write_subscription_status = receipt_written
    status = obj.run()
    assert status.error_code == 'duration_limit' and len(calls) == 1
    assert len(provider.business) == 6
    assert [p['parameters']['stockcode'] for _, p in provider.business] == ['600000'] * 3 + ['000001'] * 3
    assert cache.read_subscription_status().desired_revision == 2
    assert sum(s.first_push_sequence is not None for s in cache.read_subscription_status().streams) == 1
    assert len(writes) < 25  # Repeated market frames do not rewrite the300-stream receipt.


def test_old_stream_pushes_are_drained_before_next_paced_send(tmp_path):
    clock = Clock()
    counts = []
    class PacingProvider(Provider):
        def sendall(self, data):
            if len(self.business) == 1: counts.append(cache._highwater)
            super().sendall(data)
            if len(self.business) == 1:
                self.chunks.extend([frame(sentinel_push(self.last))] * 5)
    provider = PacingProvider(clock)
    obj, cache, _ = collector(tmp_path, provider, clock)
    status = obj.run()
    assert status.error_code == 'duration_limit' and counts == [6]


def test_pending_deadline_and_total_local_resource_budgets_do_not_reset(tmp_path, monkeypatch):
    clock = Clock(); provider = ManyPushProvider(clock, count=200, missing=True)
    obj, cache, calls = collector(tmp_path, provider, clock, duration_seconds=100)
    status = obj.run()
    assert status.error_code == 'seed_timeout' and clock.now < 120
    assert len(calls) == 1 and provider.closed
    other = tmp_path / 'budget'; other.mkdir()
    clock = Clock(); provider = ManyPushProvider(clock)
    monkeypatch.setattr(module('feed'), 'MAX_PENDING_BYTES', 2000, raising=False)
    obj, _, calls = collector(other, provider, clock)
    assert obj.run().error_code == 'byte_limit' and provider.closed and len(calls) == 1


def test_new_epoch_never_inherits_previous_stream_seed_or_push_receipt(tmp_path):
    clock = Clock(); provider = Provider(clock)
    first, cache, _ = collector(tmp_path, provider, clock, symbols=['600000'])
    first.run()
    previous = cache.read_subscription_status()
    assert all(s.seed_sequence is not None for s in previous.streams)
    next_clock = Clock(); broken = Provider(next_clock, fault='id')
    second, cache, calls = collector(tmp_path, broken, next_clock, symbols=['600000'])
    status = second.run()
    current = cache.read_subscription_status()
    assert status.error_code == 'association_mismatch' and len(calls) == 1
    assert current.epoch_id != previous.epoch_id and current.collector_state == 'blocked'
    assert all(s.seed_sequence is None and s.first_push_sequence is None for s in current.streams)
    assert cache.read_status().last_sequence == 3


def test_pending_frame_budget_and_stop_during_large_queue_are_finite(tmp_path, monkeypatch):
    clock = Clock(); provider = ManyPushProvider(clock)
    monkeypatch.setattr(module('feed'), 'MAX_PENDING_FRAMES', 5)
    obj, _, calls = collector(tmp_path, provider, clock)
    assert obj.run().error_code == 'frame_limit' and provider.closed and len(calls) == 1
    other = tmp_path / 'stop'; other.mkdir()
    clock = Clock(); provider = Provider(clock)
    obj, cache, calls = collector(other, provider, clock, symbols=[f'600{i:03}' for i in range(100)],
                                  max_symbols=100, duration_seconds=5)
    status = obj.run()
    assert status.error_code == 'duration_limit' and len(calls) == 1 and provider.closed
    assert len(provider.business) <= 5
    assert any(s.state == 'failed' for s in cache.read_subscription_status().streams)
