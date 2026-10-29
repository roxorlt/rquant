"""One explicitly started socket reader for a bounded candidate L2 feed."""
from __future__ import annotations

import socket
import time
import math
import os
import re
import json
import stat
import struct
import threading
import uuid
from collections.abc import Callable
from collections.abc import Sequence
from datetime import UTC, datetime
from ipaddress import IPv4Address
from pathlib import Path
from typing import Any

from ._codec import (ALLOWED_ENDPOINT, MAX_BYTES, MAX_SECONDS, _validate_bootstrap,
                     _wrap_frame, build_common_order_frame, build_trade_frame,
                     decode_order_response, decode_trade_response)
from ._request import (build_order_query, build_trade_query, decode_payload,
                       parse_request_body, parse_trade_request_body)
from .cache import CacheError, RawL2Cache
from .models import Binding, FeedStatus, MarketObservation, SubscriptionReceipt, SubscriptionStream
from .subscriptions import SubscriptionError, SubscriptionStore
from .symbols import MAX_SYMBOLS, market_for_code, normalize_symbols

MAX_PENDING_FRAMES = 65536
MAX_PENDING_BYTES = 64 * 1024 * 1024
_QUANTUM_FRAMES = 32
_QUANTUM_BYTES = 4 * 1024 * 1024
_QUANTUM_SECONDS = 0.1


class FeedError(RuntimeError):
    """Fixed local code; never carry upstream text or credential bytes."""

    def __init__(self, code: str, *, retryable: bool = False) -> None:
        self.code = code
        self.retryable = retryable
        super().__init__(code)


class FrameReader:
    """Retain incomplete FD frames across ordinary socket idle timeouts."""

    def __init__(self, connection: Any, *, clock: Any = time.monotonic,
                 max_frame_bytes: int = MAX_BYTES, partial_timeout: float = 15.0) -> None:
        if type(max_frame_bytes) is not int or not 1 <= max_frame_bytes <= MAX_BYTES:
            raise ValueError("invalid frame byte limit")
        if not 0 < partial_timeout <= 15:
            raise ValueError("invalid assembly timeout")
        self.connection, self.clock = connection, clock
        self.max_frame_bytes, self.partial_timeout = max_frame_bytes, partial_timeout
        self.buffer = bytearray()
        self.partial_started: float | None = None
        self._last_received: float | None = None
        self.ordinal = 0

    def _extract(self) -> bytes | None:
        if len(self.buffer) < 13:
            return None
        prefix = self.buffer[:13]
        if (prefix[:4] != b"\xfd" * 4 or prefix[12] != 0
                or any(c not in b"0123456789abcdefABCDEF" for c in prefix[4:12])):
            raise FeedError("invalid_framing")
        length = int(prefix[4:12], 16)
        if length > self.max_frame_bytes:
            raise FeedError("frame_length_limit")
        if len(self.buffer) < 13 + length:
            return None
        body = bytes(self.buffer[13:13 + length])
        del self.buffer[:13 + length]
        # The first frame was incomplete before the last recv, so all bytes
        # following its end arrived in that recv and start a new assembly timer.
        self.partial_started = self._last_received if self.buffer else None
        self.ordinal += 1
        return body

    def read(self, *, deadline: float) -> bytes | None:
        """None means idle; b'' means the observed zero-body heartbeat."""
        while True:
            body = self._extract()
            if body is not None:
                return body
            remaining = deadline - self.clock()
            if self.partial_started is not None:
                assembly_remaining = self.partial_started + self.partial_timeout - self.clock()
                if assembly_remaining <= 0:
                    raise FeedError("frame_timeout")
                remaining = min(remaining, assembly_remaining)
            if remaining <= 0:
                return None
            self.connection.settimeout(min(1.0, remaining))
            try:
                capacity = self.max_frame_bytes + 13 - len(self.buffer)
                chunk = self.connection.recv(min(65536, capacity))
            except TimeoutError:
                if (self.partial_started is not None
                        and self.clock() >= self.partial_started + self.partial_timeout):
                    raise FeedError("frame_timeout") from None
                return None
            except ConnectionResetError:
                raise FeedError("transport_error", retryable=True) from None
            except OSError:
                raise FeedError("transport_error") from None
            if not isinstance(chunk, bytes) or len(chunk) > min(65536, capacity):
                raise FeedError("byte_limit")
            if not chunk:
                raise FeedError("peer_closed", retryable=True)
            self._last_received = self.clock()
            if not self.buffer:
                self.partial_started = self._last_received
            self.buffer.extend(chunk)


def _private_input(path: Path) -> bytes:
    """Bound explicit caller-owned files; only run under the cache writer lock."""
    try:
        info = path.lstat()
        if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid()
                or info.st_mode & 0o077 or info.st_size > MAX_BYTES):
            raise FeedError("input_error")
        with path.open("rb") as source:
            data = source.read(MAX_BYTES + 1)
        if len(data) > MAX_BYTES:
            raise FeedError("input_error")
        return data
    except OSError:
        raise FeedError("input_error") from None


def _control_kind(body: bytes) -> str | None:
    if not body:
        return "heartbeat"
    if len(body) < 26:
        return None
    h, request, flags, page, size, command = struct.unpack_from("<HIIHII", body)
    if h != 26 or size != len(body) - h or page != 0:
        return None
    # Control/ack frames use page 0. Market pages are always nonzero and
    # _market additionally requires page == binding.page_id, so a page-0 frame
    # can never be a valid market observation. Recognize the observed families
    # by header fields, not by payload length:
    #   request=0, flags=6, command in {2201,2202} -> ack
    #   request=0xFFFFFFFD, flags=11, command=0 -> subscription event
    #   request=0xFFFFFFFD, flags=0x1000B, command=0 -> session metadata
    # Any other structurally valid page-0 frame is an unknown vendor control
    # family: tolerate it as control instead of aborting the run, because the
    # 2026-10-29 outage was exactly a new ack family (2615-byte, command 2202).
    if request == 0 and flags == 6 and command in (2201, 2202):
        return "ack"
    if request != 0xFFFFFFFD or command != 0:
        return "control"
    if flags == 11:
        return "control"
    if flags != 0x1000B:
        return "control"
    try:
        # The final carrier byte is opaque. Only this independently observed
        # JSON schema is classified; no session values leave this function.
        fields = json.loads(decode_payload(body[26:-1]).decode("utf-8"),
                            object_pairs_hook=lambda pairs: dict(pairs) if len(dict(pairs)) == len(pairs) else None)
        keys = {"AuthCountryCodeMd5", "AuthPubRSAVer", "clientIp", "clientPort",
                "releaseName", "serverIp", "sessionId", "srvTime"}
        if type(fields) is not dict or set(fields) != keys:
            return None
        if any(type(fields[k]) is not str or not fields[k]
               or any(not 0x21 <= ord(c) <= 0x7e for c in fields[k]) for k in keys - {"srvTime"}):
            return None
        if (re.fullmatch(r"[0-9a-fA-F]{32}", fields["AuthCountryCodeMd5"]) is None
                or len(fields["AuthPubRSAVer"]) > 32 or len(fields["releaseName"]) > 64
                or len(fields["sessionId"]) != 36
                or re.fullmatch(r"[1-9][0-9]{0,4}", fields["clientPort"]) is None
                or int(fields["clientPort"]) > 65535
                or type(fields["srvTime"]) is not int or not 0 <= fields["srvTime"] <= 0xFFFFFFFF):
            return None
        IPv4Address(fields["clientIp"])
        IPv4Address(fields["serverIp"])
    except (ValueError, TypeError):
        return None
    return "control"


class FeedCollector:
    """Six default seed reads, bounded startup history, then passive receive only.

    A registered issued ID may deliver a later candidate observation. This is
    not proof of a subscription or exchange tick granularity. Only explicit
    post-seed EOF/reset can opt into a new epoch after a durable gap.
    """

    def __init__(self, cache: RawL2Cache, *, bootstrap_path: Path,
                 order_template_path: Path, trade_template_path: Path,
                 duration_seconds: float | None = 3600, trade_pages: int = 1, order_pages: int = 1,
                 max_reconnects: int = 0, reconnect_delay_seconds: float = 60,
                 control_root: Path | None = None, max_symbols: int = 20,
                 symbols: Sequence[str] | None = None,
                 connection_factory: Callable[..., Any] = socket.create_connection,
                 clock: Callable[[], float] = time.monotonic,
                 sleep: Callable[[float], None] = time.sleep,
                 stop_event: threading.Event | None = None) -> None:
        if duration_seconds is not None and (type(duration_seconds) not in (int, float)
                or not math.isfinite(duration_seconds) or duration_seconds <= 0):
            raise ValueError("invalid duration")
        if any(type(n) is not int or not 1 <= n <= 3 for n in (trade_pages, order_pages)):
            raise ValueError("history pages must be in1..3")
        if type(max_reconnects) is not int or not 0 <= max_reconnects <= 2:
            raise ValueError("invalid reconnect count")
        if (type(reconnect_delay_seconds) not in (int, float)
                or not math.isfinite(reconnect_delay_seconds) or reconnect_delay_seconds < 60):
            raise ValueError("reconnect delay must be at least60 seconds")
        if control_root is not None and symbols is not None:
            raise ValueError('control-root and explicit symbols are mutually exclusive')
        if type(max_symbols) is not int or not 1 <= max_symbols <= MAX_SYMBOLS:
            raise ValueError('invalid symbol capacity')
        self.targets = SubscriptionStore(control_root, max_symbols=max_symbols) if control_root is not None else None
        self._configured_symbols = (normalize_symbols(symbols) if symbols is not None else
                                    () if self.targets is not None else ('603258', '300006'))
        if len(self._configured_symbols) > max_symbols:
            raise ValueError('symbol capacity exceeded')
        self.max_symbols = max_symbols
        self.cache = cache
        self.bootstrap_path, self.order_template_path, self.trade_template_path = (
            Path(bootstrap_path), Path(order_template_path), Path(trade_template_path))
        self.duration_seconds, self.trade_pages, self.order_pages = duration_seconds, trade_pages, order_pages
        self.max_reconnects, self.reconnect_delay_seconds = max_reconnects, reconnect_delay_seconds
        self.connection_factory, self.clock, self.sleep = connection_factory, clock, sleep
        self.stop_event = stop_event if stop_event is not None else threading.Event()
        self.status = FeedStatus()
        self.connection: Any = None
        self.registered: dict[int, Binding] = {}
        self.issued: dict[int, Binding] = {}
        self.validated_seed = False
        self._epoch_seed_timeouts = 0
        self._inputs = (b"", b"", b"")
        self._last_send: float | None = None
        self._symbols: tuple[str, ...] = ()
        self._desired_revision = 0
        self._stream_receipts: dict[int, SubscriptionStream] = {}
        self._next_control_check = 0.0

    def _write_receipt(self) -> None:
        if self.status.epoch_id is None:
            return
        try:
            self.cache.write_subscription_status(SubscriptionReceipt(
                epoch_id=self.status.epoch_id, desired_revision=self._desired_revision,
                collector_state=self.status.state, streams=tuple(self._stream_receipts.values())))
        except Exception:
            raise FeedError('cache_error') from None

    def _transition(self, binding: Binding, **changes: Any) -> None:
        current = self._stream_receipts.get(binding.page_id)
        if current is None:
            return
        updated = SubscriptionStream.model_validate({**current.model_dump(mode='python'), **changes})
        if updated != current:
            self._stream_receipts[binding.page_id] = updated
            self._write_receipt()

    def _refresh_targets(self, *, force: bool = False) -> None:
        if not force and self.clock() < self._next_control_check:
            return
        self._next_control_check = self.clock() + _QUANTUM_SECONDS
        if self.targets is not None:
            try:
                targets = self.targets.read()
            except SubscriptionError:
                raise FeedError('input_error') from None
            symbols, revision = targets.symbols, targets.revision
            if (symbols[:len(self._symbols)] != self._symbols or revision < self._desired_revision
                    or revision == self._desired_revision and symbols != self._symbols):
                raise FeedError('input_error')
        else:
            symbols, revision = self._configured_symbols, 0
        changed = symbols != self._symbols or revision != self._desired_revision
        for index in range(len(self._symbols), len(symbols)):
            code, base = symbols[index], 10117 + 10 * index
            for offset, kind, side in ((0, 'vendor4013', None), (1, 'vendor4027', 'buy'), (2, 'vendor4027', 'sell')):
                binding = Binding(page_id=base + offset, kind=kind, code=code,
                                  market=market_for_code(code), side=side)
                self._stream_receipts[binding.page_id] = SubscriptionStream(binding=binding, state='queued')
        self._symbols, self._desired_revision = symbols, revision
        if changed:
            self._write_receipt()

    def _finish_receipt(self) -> None:
        for page, stream in tuple(self._stream_receipts.items()):
            if stream.state in {'queued', 'sent'}:
                self._stream_receipts[page] = stream.model_copy(update={'state': 'failed'})
        self._write_receipt()

    def _update(self, **changes: Any) -> None:
        self.status = self.status.model_copy(update=changes)
        try:
            self.cache.write_status(self.status)
        except Exception:
            raise FeedError("cache_error") from None

    def _check_end(self, deadline: float) -> None:
        if self.stop_event.is_set():
            raise FeedError("signal_stop")
        if self.clock() >= deadline:
            raise FeedError("duration_limit")

    def _pause(self, seconds: float, deadline: float) -> None:
        until = self.clock() + seconds
        while self.clock() < until:
            self._check_end(deadline)
            self.sleep(min(1.0, until - self.clock(), deadline - self.clock()))

    def _send(self, wire: bytes, deadline: float) -> None:
        self._check_end(deadline)
        try:
            self.connection.settimeout(min(MAX_SECONDS, deadline - self.clock()))
            self.connection.sendall(wire)
        except ConnectionResetError:
            raise FeedError("transport_error", retryable=True) from None
        except OSError:
            raise FeedError("transport_error") from None

    def _register(self, binding: Binding) -> None:
        if binding.page_id in self.registered:
            if self.registered[binding.page_id] != binding:
                raise FeedError("association_mismatch")
            return
        identity = (binding.kind, binding.code, binding.market, binding.side)
        if any((b.kind, b.code, b.market, b.side) == identity for b in self.registered.values()):
            raise FeedError("association_mismatch")
        self.registered[binding.page_id] = binding

    def _market(self, body: bytes, ordinal: int, pending: int | None,
                delivery: str, received_at: datetime) -> dict[str, Any] | None:
        if len(body) < 24:
            raise FeedError("protocol_error")
        request = int.from_bytes(body[2:6], "little")
        page = int.from_bytes(body[10:12], "little")
        command = int.from_bytes(body[16:20], "little")
        if request == 0xFFFFFFFF:
            binding = self.registered.get(page)
            if binding not in self.issued.values():
                binding = None
        else:
            binding = self.issued.get(request)
        if (binding is None or binding.page_id != page
                or command != 2357
                or self.registered.get(page) != binding):
            self._update(unknown_push_count=self.status.unknown_push_count + 1)
            raise FeedError("association_mismatch")
        no_data = (b"\x04\x00" + "系统信息".encode("utf-16le")
                   + b"\x04\x00" + "无数据！".encode("utf-16le") + b"\x04\x00\x00\x00\x00\x00")
        if (request != 0xFFFFFFFF and binding.kind == "vendor4013" and len(body) == 58
                and int.from_bytes(body[:2], "little") == 32
                and int.from_bytes(body[6:10], "little") == 0x111
                and int.from_bytes(body[12:16], "little") == 26 and body[32:] == no_data):
            self._update(control_count=self.status.control_count + 1)
            if request == pending and delivery == 'seed':
                self._transition(binding, state='no_data')
            return None
        try:
            if binding.kind == "vendor4013":
                result = decode_trade_response(body, request_id=request, page_id=page, code=binding.code,
                                               market=binding.market, source_fd_ordinal=ordinal)
            else:
                result = decode_order_response(body, request_id=request, page_id=page, code=binding.code,
                                               source_fd_ordinal=ordinal)
        except Exception:
            raise FeedError("decode_error") from None
        if not result["table"]["rows"]:
            raise FeedError("decode_error")
        if binding.kind == "vendor4027":
            expected = 1 if binding.side == "buy" else 2
            if any(type(r.get("12", {}).get("value")) is not int
                   or r["12"]["value"] != expected for r in result["table"]["rows"]):
                raise FeedError("order_side_mismatch")
        actual_delivery = delivery if request == pending else "candidate_push"
        try:
            observation = MarketObservation(
                epoch_id=self.status.epoch_id, binding=binding, received_at=received_at,
                request_id=request, source_fd_ordinal=ordinal, delivery=actual_delivery,
                response_body_hex=body.hex(), raw_table_hex=result["raw_table"].hex(),
                table=result["table"], response_sha256=result["response_sha256"],
            )
        except Exception:
            raise FeedError("decode_error") from None
        try:
            batch = self.cache.append(observation)
        except Exception:
            raise FeedError("cache_error") from None
        changes: dict[str, Any] = {"last_market_received_at": received_at,
                                  "market_batch_count": self.status.market_batch_count + 1}
        if actual_delivery == "candidate_push":
            changes["subscription_state"] = "candidate_push_observed"
            changes["state"] = "listening_candidate"
        self._update(**changes)
        receipt = self._stream_receipts.get(binding.page_id)
        if receipt is not None:
            if actual_delivery == 'seed' and receipt.seed_sequence is None:
                self._transition(binding, seed_sequence=batch.local_batch_sequence, seed_received_at=received_at,
                                 state='first_push_observed' if receipt.first_push_sequence else 'seeded')
            elif actual_delivery == 'candidate_push' and receipt.first_push_sequence is None:
                self._transition(binding, first_push_sequence=batch.local_batch_sequence, state='first_push_observed')
        return result

    def _receive(self, deadline: float, *, pending: int | None = None,
                 delivery: str = "seed", await_ack: bool = False) -> dict[str, Any] | None:
        frames = payload_bytes = quantum_frames = quantum_bytes = 0
        quantum_started = self.clock()
        while True:
            self._check_end(self._deadline)
            if self.clock() >= deadline:
                if pending is not None or await_ack:
                    raise FeedError("seed_timeout")
                return None
            body = self.reader.read(deadline=deadline)
            if body is None:
                if pending is None and not await_ack:
                    return None
                continue
            frames += 1
            payload_bytes += len(body) + 13
            if (await_ack and frames > 32) or pending is not None and frames > MAX_PENDING_FRAMES:
                raise FeedError("frame_limit")
            if pending is not None and payload_bytes > MAX_PENDING_BYTES:
                raise FeedError('byte_limit')
            quantum_frames += 1
            quantum_bytes += len(body) + 13
            if (quantum_frames >= _QUANTUM_FRAMES or quantum_bytes >= _QUANTUM_BYTES
                    or self.clock() - quantum_started >= _QUANTUM_SECONDS):
                if not await_ack:
                    self._refresh_targets(force=True)
                quantum_frames = quantum_bytes = 0
                quantum_started = self.clock()
            received_at = datetime.now(UTC)
            # Commit transport with the frame's outcome; a separate status fsync
            # delays pending seeds while already registered streams are active.
            self.status = self.status.model_copy(update={"last_transport_received_at": received_at})
            control_kind = _control_kind(body)
            if control_kind is not None:
                if control_kind == "heartbeat":
                    self._update(heartbeat_count=self.status.heartbeat_count + 1)
                else:
                    self._update(control_count=self.status.control_count + 1)
                if await_ack and control_kind == "ack":
                    return None
                if pending is None and not await_ack:
                    return None
                continue
            result = self._market(body, self.reader.ordinal, pending, delivery, received_at)
            if int.from_bytes(body[2:6], "little") == pending or pending is None:
                return result

    @staticmethod
    def _order_cursor(result: dict[str, Any]) -> tuple[int, int]:
        try:
            pairs = [(r["56"]["value"], r["1"]["value"]) for r in result["table"]["rows"]]
            if (any(type(t) is not int or type(uid) is not int or min(t, uid) <= 0 for t, uid in pairs)
                    or any(a >= b or a[1] >= b[1] for a, b in zip(pairs, pairs[1:]))):
                raise ValueError
            return pairs[0]
        except Exception:
            raise FeedError("protocol_error") from None

    @staticmethod
    def _older(binding: Binding, previous: dict[str, Any], current: dict[str, Any]) -> None:
        try:
            if binding.kind == "vendor4013":
                sequences = []
                for result in (previous, current):
                    values = [r["18"]["value"] for r in result["table"]["rows"]]
                    if any(not isinstance(v, str) or re.fullmatch(r"[1-9][0-9]*", v) is None for v in values):
                        raise ValueError
                    ids = [int(v) for v in values]
                    if any(a >= b for a, b in zip(ids, ids[1:])):
                        raise ValueError
                    sequences.append(ids)
                if sequences[1][-1] >= sequences[0][0]:
                    raise ValueError
            else:
                cursor = FeedCollector._order_cursor(previous)
                FeedCollector._order_cursor(current)
                if any((r["56"]["value"], r["1"]["value"]) >= cursor
                       or r["1"]["value"] >= cursor[1] for r in current["table"]["rows"]):
                    raise ValueError
        except Exception:
            raise FeedError("protocol_error") from None

    def _issue_stream(self, binding: Binding) -> None:
        self._register(binding)
        previous = None
        pages = self.trade_pages if binding.kind == 'vendor4013' else self.order_pages
        for page in range(1, pages + 1):
            if self._last_send is not None:
                until = self._last_send + 1
                while self.clock() < until:
                    self._check_end(self._deadline)
                    self._refresh_targets()
                    self._receive(min(self._deadline, until))
            self._check_end(self._deadline)
            request_id = 3 + len(self.issued)
            maximum = 2 + self.max_symbols * (self.trade_pages + 2 * self.order_pages)
            if request_id > maximum or request_id > 902:
                raise FeedError('protocol_error')
            cursor = self._order_cursor(previous) if previous is not None and binding.kind == 'vendor4027' else (0, 0)
            if binding.kind == 'vendor4013':
                query = build_trade_query(code=binding.code, market=binding.market,
                                          page_id=binding.page_id, currentpage=page)
                builder, template = build_trade_frame, self._inputs[2]
            else:
                query = build_order_query(code=binding.code, market=binding.market, side=binding.side,
                    page_id=binding.page_id, rowcount=20, endtime=cursor[0], orderid=cursor[1])
                builder, template = build_common_order_frame, self._inputs[1]
            active = ','.join(str(p) for p in self.registered)
            query = query.replace(f'pageList={binding.page_id}\r\n'.encode(), f'pageList={active}\r\n'.encode(), 1)
            wire = builder(template, query, request_id=request_id)
            self.issued[request_id] = binding
            self._update(seed_queries=self.status.seed_queries + 1)
            self._send(wire, self._deadline)
            self._last_send = self.clock()
            if page == 1:
                self._transition(binding, state='sent', request_id=request_id)
            try:
                result = self._receive(
                    min(self._deadline, self.clock() + MAX_SECONDS), pending=request_id,
                    delivery='seed' if page == 1 else 'backfill')
            except FeedError as error:
                if error.code != "seed_timeout":
                    raise
                # Keep the issued ID: a late response may still be accepted as
                # a candidate push for the same page.
                if page == 1:
                    self._epoch_seed_timeouts += 1
                receipt = self._stream_receipts.get(binding.page_id)
                if receipt is not None and receipt.seed_sequence is None:
                    self._transition(binding, state='failed')
                return
            if result is None:
                break
            self.validated_seed = True
            if previous is not None:
                self._older(binding, previous, result)
            previous = result

    def _epoch(self) -> None:
        self.registered, self.issued = {}, {}
        self.validated_seed = False
        self._epoch_seed_timeouts = 0
        self._last_send = None
        self._symbols, self._desired_revision, self._stream_receipts = (), 0, {}
        self._update(state="starting", epoch_id=uuid.uuid4().hex, error_code=None,
                     subscription_state='requested_unverified')
        self._refresh_targets(force=True)
        try:
            self._check_end(self._deadline)
            try:
                self.connection = self.connection_factory(ALLOWED_ENDPOINT,
                    timeout=min(MAX_SECONDS, self._deadline - self.clock()))
            except OSError:
                raise FeedError("transport_error") from None
            self.reader = FrameReader(self.connection, clock=self.clock)
            self._send(_wrap_frame(self._inputs[0]), self._deadline)
            self._receive(min(self._deadline, self.clock() + MAX_SECONDS), await_ack=True)
            self._update(state="connected")
            self._write_receipt()
            while True:
                self._check_end(self._deadline)
                self._refresh_targets()
                queued = next((s.binding for s in self._stream_receipts.values() if s.state == 'queued'), None)
                if queued is not None:
                    self._issue_stream(queued)
                    continue
                waiting = 'seeded_waiting' if self.validated_seed else 'connected'
                state = 'listening_candidate' if self.status.subscription_state == 'candidate_push_observed' else waiting
                if self.status.state != state:
                    self._update(state=state)
                    self._write_receipt()
                self._receive(self._deadline)
        finally:
            if self.connection is not None:
                try:
                    self.connection.close()
                except Exception:
                    pass
                self.connection = None

    def run(self) -> FeedStatus:
        self._deadline = math.inf if self.duration_seconds is None else self.clock() + self.duration_seconds
        try:
            with self.cache.writer():
                previous = self.cache.read_status()
                self.status = FeedStatus(state="starting", gap=previous.gap or previous.last_sequence > 0)
                self._update()
                try:
                    self._check_end(self._deadline)
                    if self.targets is not None:
                        while not self.targets.read().symbols:
                            self._pause(1, self._deadline)
                    self._inputs = tuple(_private_input(p) for p in (
                        self.bootstrap_path, self.order_template_path, self.trade_template_path))
                    _validate_bootstrap(self._inputs[0])
                    order_template = parse_request_body(self._inputs[1])
                    if (order_template["kind"] != "order_query"
                            or order_template["header_length"] != 77):
                        raise ValueError
                    parse_trade_request_body(self._inputs[2])
                except FeedError as error:
                    self._update(state="stopped" if error.code in {"signal_stop", "duration_limit"} else "blocked",
                                 gap=True, error_code=error.code)
                    return self.cache.read_status()
                except Exception:
                    self._update(state="blocked", gap=True, error_code="input_error")
                    return self.cache.read_status()
                while True:
                    try:
                        self._epoch()
                    except FeedError as error:
                        stopped = error.code in {"signal_stop", "duration_limit"}
                        state = "stopped" if stopped else "disconnected" if error.retryable else "blocked"
                        code = error.code
                        if (code in {"duration_limit", "signal_stop"} and not self.validated_seed
                                and self._epoch_seed_timeouts > 0):
                            # No seed ever landed in this epoch: a normal duration or
                            # stop end would hide that the subscription never worked.
                            code, state = "seed_timeout", "blocked"
                        self._update(state=state, gap=True, error_code=code)
                        self._finish_receipt()
                        if (error.retryable and self.validated_seed
                                and self.status.reconnect_attempts < self.max_reconnects):
                            try:
                                self._pause(self.reconnect_delay_seconds, self._deadline)
                            except FeedError as stop:
                                self._update(state="stopped", gap=True, error_code=stop.code)
                                break
                            self._update(reconnect_attempts=self.status.reconnect_attempts + 1)
                            continue
                        break
                    except KeyboardInterrupt:
                        if not self.validated_seed and self._epoch_seed_timeouts > 0:
                            self._update(state="blocked", gap=True, error_code="seed_timeout")
                        else:
                            self._update(state="stopped", gap=True, error_code="signal_stop")
                        self._finish_receipt()
                        break
                    except Exception:
                        self._update(state="blocked", gap=True, error_code="protocol_error")
                        self._finish_receipt()
                        break
                return self.cache.read_status()
        except (CacheError, FeedError) as error:
            code = "writer_busy" if isinstance(error, CacheError) and error.code == "writer_busy" else "cache_error"
            return self.status.model_copy(update={"state": "blocked", "gap": True, "error_code": code})
        finally:
            self._inputs = (b"", b"", b"")
