"""Optional original detector captures and events in the original alert ledger."""

from __future__ import annotations

import os
import stat
from collections.abc import Callable
from datetime import datetime, timedelta
from dataclasses import asdict
from hashlib import sha256
from pathlib import Path
import sqlite3
from typing import TYPE_CHECKING, Literal
from weakref import WeakKeyDictionary
from zoneinfo import ZoneInfo

from pydantic import Field, StrictBool, StrictInt, field_validator, model_validator

from rquant.condition_alert_runtime_contracts import (
    ConditionAlertRuntimeActivation,
    ConditionAlertProducerEventRecord,
    read_condition_activation_setting,
    require_condition_alert_activation,
    ConditionAlertSourceDescriptor,
)
from rquant.monitor_builtin_contracts import (
    BuiltinId,
    BuiltinModel,
    BuiltinQuoteFacts,
    BuiltinStockDetection,
    BuiltinWatchFacts,
    MonitorBuiltinCaptureRecord,
    MonitorBuiltinDefinition,
    MonitorBuiltinMaterial,
    BuiltinConditionAlertEventEnvelope,
    BuiltinMarketDetection,
)
from rquant.price_alert_runtime_contracts import PriceSha256, _activation_bytes
from rquant.runtime_contracts import AwareUtcDatetime, canonical_sha256, normalize_aware_utc
from rquant.manual_watchlist import OwnerId
from rquant.strict_json import canonical_json_bytes, strict_json_loads

if TYPE_CHECKING:
    from rquant.condition_alert_runtime import ConditionAlertRuntimeStore
    from rquant.notifier_operator import MonitorControlSnapshot


class MonitorBuiltinCaptureSettings(BuiltinModel):
    enabled: StrictBool = False
    capture_root: Path
    origin: Literal["watchlist_quote", "original_monitor", "original_surge", "original_pulse"]
    source_generation_id: PriceSha256
    code_contract_sha256: PriceSha256

    @field_validator("capture_root")
    @classmethod
    def absolute_root(cls, value: Path) -> Path:
        if not value.is_absolute() or ".." in value.parts or value != value.resolve():
            raise ValueError("builtin capture root must be an actual absolute private path")
        return value


class MonitorBuiltinCaptureAuthority:
    __slots__ = ("__weakref__",)

    def __init__(self) -> None:
        raise TypeError("builtin capture requires the actual immutable role manifest")


_CAPTURES: WeakKeyDictionary[MonitorBuiltinCaptureAuthority, tuple[Path, Path, bytes, MonitorBuiltinCaptureSettings]] = WeakKeyDictionary()


def builtin_source_contract_sha256() -> str:
    # The detectors remain the single owners of their existing mathematics.
    from rquant import monitor, pulse_watch, surge_watch

    sources = (monitor, pulse_watch, surge_watch)
    paths = tuple(Path(module.__file__) for module in sources) + (Path(__file__), Path(__file__).with_name("monitor_builtin_contracts.py"))
    return sha256(canonical_json_bytes({
        "contract": "monitor-builtin-original-detection/v1",
        "sources": {path.name: sha256(path.read_bytes()).hexdigest() for path in paths},
    })).hexdigest()


def verify_builtin_capture_authority(
    manifest_path: Path, *, runtime_root: Path, expected_sha256: str, expected_commit: str
) -> MonitorBuiltinCaptureAuthority:
    from rquant.runtime_service_entrypoint import RuntimeServiceKind, load_runtime_service_manifest

    path, root = Path(manifest_path), Path(runtime_root)
    payload = _activation_bytes(path, root)
    if sha256(payload).hexdigest() != expected_sha256:
        raise ValueError("builtin source manifest content changed")
    manifest = load_runtime_service_manifest(path, expected_commit=expected_commit)
    raw = manifest.model_dump(mode="json")["settings"].get("monitor_builtin_capture")
    if raw is None or manifest.service_kind not in {RuntimeServiceKind.CONDITION_ALERT_RUNTIME, RuntimeServiceKind.PRICE_ALERT_RUNTIME, RuntimeServiceKind.WATCHLIST_QUOTE_SOURCE}:
        raise ValueError("builtin source is not configured on the actual original owner")
    settings = MonitorBuiltinCaptureSettings.model_validate_json(canonical_json_bytes(raw))
    if (not settings.enabled or settings.code_contract_sha256 != builtin_source_contract_sha256()
            or (manifest.service_kind is RuntimeServiceKind.WATCHLIST_QUOTE_SOURCE) != (settings.origin == "watchlist_quote")
            or _activation_bytes(path, root) != payload):
        raise ValueError("builtin source kind, code or installation changed")
    value = object.__new__(MonitorBuiltinCaptureAuthority)
    _CAPTURES[value] = path, root, payload, settings
    return value


class MonitorWatchlistMaterial(BuiltinModel):
    watch: tuple[BuiltinWatchFacts, ...] = Field(max_length=500)
    basis_json: str = Field(min_length=1, max_length=4 * 1024 * 1024, strict=True)
    replica_identity: PriceSha256
    sidecar_sha256: PriceSha256
    updated_at: AwareUtcDatetime
    synced_at: AwareUtcDatetime
    read_at: AwareUtcDatetime


class MonitorWatchlistRead:
    __slots__ = ("__weakref__",)

    def __init__(self) -> None:
        raise TypeError("monitor watchlist requires the original verified replica read")


_WATCHLIST_READS: WeakKeyDictionary[MonitorWatchlistRead, tuple[MonitorWatchlistMaterial, object, object]] = WeakKeyDictionary()


def read_original_monitor_watchlist(source: object, *, read_at: datetime) -> MonitorWatchlistRead:
    from rquant.monitor import build_watchlist
    from rquant.screen.replica_source import VerifiedReplicaScreenSource
    from rquant.storage.duckdb import DuckDBStore

    if type(source) is not VerifiedReplicaScreenSource:
        raise TypeError("monitor scope must use the original verified read-only replica")
    connection, descriptor, generation = source._open()
    class OriginalBorrowedStore(DuckDBStore):
        def __init__(self) -> None:
            self._conn = connection
            self.path = source.replica_path
            self._owned_primary_lease = None
            self._artifact_terminal_hook = None
    try:
        original = build_watchlist(OriginalBorrowedStore())
        if len(original) > 500:
            raise ValueError("complete builtin watchlist exceeds the original 500-stock quote budget")
        values = []
        for item in sorted(original, key=lambda value: value.ts_code):
            facts = {name: asdict(item)[name] for name in BuiltinWatchFacts.model_fields}
            for name in ("limit_up_date", "entry_date", "reference_date"):
                value = facts[name]
                if isinstance(value, datetime):
                    facts[name] = value.date()
            values.append(BuiltinWatchFacts(**facts))
        now = normalize_aware_utc(read_at)
        if generation.synced_at > now:
            raise ValueError("monitor replica was not available at the actual owner read clock")
        material = MonitorWatchlistMaterial(watch=tuple(values),
            basis_json=canonical_json_bytes({"watch": [item.model_dump(mode="json") for item in values],
                "replica_identity": generation.identity, "sidecar_sha256": generation.sidecar_sha256}).decode(),
            replica_identity=generation.identity, sidecar_sha256=generation.sidecar_sha256, updated_at=generation.updated_at,
            synced_at=generation.synced_at, read_at=now)
        source._finish(descriptor, generation)
    finally:
        connection.close()
        os.close(descriptor)
    result = object.__new__(MonitorWatchlistRead)
    _WATCHLIST_READS[result] = material, source, generation
    return result


def require_monitor_watchlist_read(value: object) -> MonitorWatchlistMaterial:
    if type(value) is not MonitorWatchlistRead or value not in _WATCHLIST_READS:
        raise TypeError("builtin watch numbers require the original verified replica capability")
    material, source, generation = _WATCHLIST_READS[value]
    if source._verify() != generation:
        raise ValueError("original monitor watchlist generation changed after the same owned read")
    return material


def publish_original_quote_builtin_capture(
    authority: MonitorBuiltinCaptureAuthority, *, watchlist: MonitorWatchlistRead, quotes: object,
    sequence: int,
) -> Path:
    from rquant.price_alert_runtime_source import BuiltinQuoteRequestBinding, parse_quote_request_binding, require_price_quote_owned_read
    from rquant.strict_json import strict_canonical_json_loads

    payload, settings = require_builtin_capture_authority(authority)
    watch = require_monitor_watchlist_read(watchlist)
    original = require_price_quote_owned_read(quotes)
    source = original.snapshot
    request = parse_quote_request_binding(original.request_json)
    if (type(request) is not BuiltinQuoteRequestBinding or request.watch_basis_sha256 != sha256(watch.basis_json.encode()).hexdigest()
            or request.scope_generation_id != watch.replica_identity or request.scope_manifest_sha256 != watch.sidecar_sha256
            or settings.origin != "watchlist_quote" or source.source_generation_id != settings.source_generation_id
            or source.requested_codes != tuple(item.ts_code for item in watch.watch) or not source.quotes):
        raise ValueError("builtin quote request differs from the complete original monitor watchlist")
    raw_rows = strict_canonical_json_loads(original.original_rows_json)
    typed = tuple(sorted((BuiltinQuoteFacts(ts_code=row["ts_code"], price=row["price"], low=row["low"],
        open=row["open"], high=row["high"], pre_close=row.get("pre_close"), pct_chg=row.get("pct_chg"),
        volume=row.get("volume") if row.get("volume_unit") in {"shares", "lot100"} else None,
        amount=row.get("amount") if row.get("amount_unit") == "CNY" else None, source=row["source"],
        observed_at=datetime.fromisoformat(row["observed_at"])) for row in raw_rows), key=lambda item: item.ts_code))
    snapshot = canonical_json_bytes({"rows": raw_rows, "quotes": [item.model_dump(mode="json") for item in typed]}).decode()
    receipt = canonical_json_bytes({"quote_read": original.model_dump(mode="json"), "watch_read": watch.model_dump(mode="json")}).decode()
    if len(receipt.encode()) > 4 * 1024 * 1024:
        # The complete source receipt must fit; a hash alone cannot replace its values.
        raise ValueError("complete original builtin source receipt exceeds its bounded budget")
    capture = MonitorBuiltinCaptureRecord(origin=settings.origin, producer_manifest_sha256=sha256(payload).hexdigest(),
        producer_commit=strict_json_loads(payload)["producer_commit"], source_generation_id=source.source_generation_id,
        source_sequence=sequence, trade_date=typed[0].observed_at.astimezone(ZoneInfo("Asia/Shanghai")).date(),
        observed_at=max(item.observed_at for item in typed), available_at=source.available_at,
        universe_codes=source.requested_codes, missing_codes=tuple(sorted(set(source.requested_codes) - {item.ts_code for item in typed})),
        raw_payload_sha256=source.payload_sha256, original_snapshot_sha256=sha256(snapshot.encode()).hexdigest(),
        source_receipt_sha256=sha256(receipt.encode()).hexdigest(), original_source_receipt_json=receipt,
        basis_sha256=sha256(watch.basis_json.encode()).hexdigest(), stock_watch=watch.watch, quotes=typed,
        original_snapshot_json=snapshot, original_basis_json=watch.basis_json, original_results=(),
        source_state="ready", reason="captured")
    require_price_quote_owned_read(quotes)
    require_monitor_watchlist_read(watchlist)
    return publish_original_builtin_capture(authority, capture=capture)


def require_builtin_capture_authority(value: object) -> tuple[bytes, MonitorBuiltinCaptureSettings]:
    if type(value) is not MonitorBuiltinCaptureAuthority or value not in _CAPTURES:
        raise TypeError("builtin source requires its actual original manifest verifier")
    path, root, payload, settings = _CAPTURES[value]
    if _activation_bytes(path, root) != payload or settings.code_contract_sha256 != builtin_source_contract_sha256():
        raise ValueError("builtin source installation or detector code changed")
    return payload, settings


def next_builtin_capture_sequence(authority: MonitorBuiltinCaptureAuthority) -> int:
    _, settings = require_builtin_capture_authority(authority)
    path = settings.capture_root / (settings.origin + ".json")
    if not settings.capture_root.exists():
        return 1
    _private_root(settings.capture_root)
    if not path.exists():
        return 1
    previous = MonitorBuiltinCaptureRecord.model_validate_json(_read_capture(path)[0])
    return previous.source_sequence + 1 if previous.source_generation_id == settings.source_generation_id else 1


def publish_original_builtin_status(
    authority: MonitorBuiltinCaptureAuthority, *, observed_at: datetime,
    state: Literal["waiting", "stale", "disconnected", "unknown", "disabled"], reason: str,
    receipt_json: str, watchlist: MonitorWatchlistRead | None = None,
) -> Path:
    payload, settings = require_builtin_capture_authority(authority)
    now = normalize_aware_utc(observed_at)
    watch = None if watchlist is None else require_monitor_watchlist_read(watchlist)
    basis = canonical_json_bytes({"watch": []}).decode() if watch is None else watch.basis_json
    return publish_original_builtin_capture(authority, capture=MonitorBuiltinCaptureRecord(
        origin=settings.origin, producer_manifest_sha256=sha256(payload).hexdigest(),
        producer_commit=strict_json_loads(payload)["producer_commit"], source_generation_id=settings.source_generation_id,
        source_sequence=next_builtin_capture_sequence(authority), trade_date=now.astimezone(ZoneInfo("Asia/Shanghai")).date(),
        observed_at=now, available_at=now, universe_codes=() if watch is None else tuple(item.ts_code for item in watch.watch),
        missing_codes=() if watch is None else tuple(item.ts_code for item in watch.watch), raw_payload_sha256=None,
        source_receipt_sha256=sha256(receipt_json.encode()).hexdigest(), original_source_receipt_json=receipt_json,
        basis_sha256=sha256(basis.encode()).hexdigest(), stock_watch=() if watch is None else watch.watch, quotes=(),
        original_snapshot_json=None, original_basis_json=basis, original_results=(), source_state=state, reason=reason))


def _original_frame_json(frame: object) -> str:
    import pandas as pd
    from datetime import date

    if type(frame) is not pd.DataFrame or len(frame) > 8000:
        raise TypeError("builtin market capture requires the bounded original complete frame")
    rows = []
    for raw in frame.to_dict(orient="records"):
        row = {}
        for name, value in raw.items():
            if isinstance(value, (date, datetime)):
                value = value.isoformat()
            elif pd.isna(value):
                value = None
            elif hasattr(value, "item"):
                value = value.item()
            row[name] = value
        rows.append(row)
    result = canonical_json_bytes({"rows": rows}).decode()
    if len(result.encode()) > 64 * 1024 * 1024:
        raise ValueError("original market frame exceeds its complete input budget")
    return result


class OriginalBuiltinLogRead(BuiltinModel):
    path: Path
    raw_sha256: PriceSha256
    raw_jsonl: str = Field(max_length=4 * 1024 * 1024, strict=True)
    physical_identity: tuple[int, int, int, int, int, int, int, int]

    @model_validator(mode="after")
    def exact_log(self) -> OriginalBuiltinLogRead:
        if sha256(self.raw_jsonl.encode()).hexdigest() != self.raw_sha256:
            raise ValueError("original builtin log receipt differs from its complete bytes")
        for line in self.raw_jsonl.splitlines():
            if line.strip():
                strict_json_loads(line)
        return self


class OriginalMonitorFetchReceipt(BuiltinModel):
    requested_at: AwareUtcDatetime
    response_received_at: AwareUtcDatetime
    tushare_cache_at: AwareUtcDatetime | None
    fallback_codes: tuple[str, ...] = Field(max_length=500)

    @model_validator(mode="after")
    def actual_clock(self) -> OriginalMonitorFetchReceipt:
        if self.requested_at > self.response_received_at or self.tushare_cache_at is not None and self.tushare_cache_at > self.response_received_at:
            raise ValueError("original monitor fetch clock is inconsistent")
        return self


def _original_log_read(path: Path) -> OriginalBuiltinLogRead:
    from rquant.price_alert_runtime_source import _quote_file_identity

    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
    try:
        before = _quote_file_identity(path)
        info = os.fstat(descriptor)
        if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.geteuid() or info.st_nlink != 1
                or stat.S_IMODE(info.st_mode) & 0o022 or info.st_size > 4 * 1024 * 1024):
            raise ValueError("original builtin log is unsafe or exceeds its complete bound")
        with os.fdopen(descriptor, "rb", closefd=False) as stream:
            raw = stream.read(4 * 1024 * 1024 + 1)
        if _quote_file_identity(path) != before or len(raw) != info.st_size:
            raise ValueError("original builtin log changed during its same owned read")
        return OriginalBuiltinLogRead(path=path, raw_sha256=sha256(raw).hexdigest(), raw_jsonl=raw.decode(), physical_identity=before)
    finally:
        os.close(descriptor)


class OriginalBuiltinSourceOutlet:
    __slots__ = ("__weakref__",)

    def __init__(self) -> None:
        raise TypeError("original builtin outlet requires its exact verified source roles")

    def _authority(self, origin: str) -> MonitorBuiltinCaptureAuthority | None:
        if self not in _ORIGINAL_OUTLETS:
            raise TypeError("original builtin outlet is not an issued source capability")
        for authority in _ORIGINAL_OUTLETS[self]:
            _, settings = require_builtin_capture_authority(authority)
            if settings.origin == origin:
                return authority
        return None

    def captures(self, origin: str) -> bool:
        return self._authority(origin) is not None

    def monitor_snapshot(self, *, watchlist: tuple[object, ...], quotes: tuple[object, ...], fetch: OriginalMonitorFetchReceipt) -> None:
        from rquant.monitor import RealtimeQuote, WatchItem

        authority = self._authority("original_monitor")
        if authority is None:
            return
        if (type(fetch) is not OriginalMonitorFetchReceipt or any(type(item) is not WatchItem for item in watchlist)
                or any(type(item) is not RealtimeQuote for item in quotes)):
            raise TypeError("monitor capture requires its same original watch and fetch objects")
        fetch = OriginalMonitorFetchReceipt.model_validate(fetch)
        watches = []
        for item in sorted(watchlist, key=lambda row: row.ts_code):
            raw = asdict(item)
            for name in ("entry_date", "reference_date", "limit_up_date"):
                if isinstance(raw[name], datetime):
                    raw[name] = raw[name].date()
            watches.append(BuiltinWatchFacts(**{name: raw[name] for name in BuiltinWatchFacts.model_fields}))
        basis = canonical_json_bytes({"watch": [item.model_dump(mode="json") for item in watches]}).decode()
        raw_quotes = []
        typed = []
        for quote in sorted(quotes, key=lambda item: item.ts_code):
            raw_quotes.append(quote.model_dump(mode="json"))
            observed = fetch.response_received_at if quote.ts_code in fetch.fallback_codes else fetch.tushare_cache_at
            if observed is None:
                raise ValueError("original monitor quote has no actual source response clock")
            typed.append(BuiltinQuoteFacts(ts_code=quote.ts_code, price=quote.price, low=quote.low, open=quote.open,
                high=quote.high, pre_close=quote.pre_close, pct_chg=quote.pct_chg, volume=None, amount=None, source=quote.source, observed_at=observed))
        universe = tuple(item.ts_code for item in watches)
        missing = tuple(sorted(set(universe) - {item.ts_code for item in typed}))
        stale = any(fetch.response_received_at - item.observed_at > timedelta(seconds=15) for item in typed)
        snapshot = canonical_json_bytes({"quotes": [item.model_dump(mode="json") for item in typed], "original_quotes": raw_quotes}).decode()
        receipt = canonical_json_bytes({"capture_kind": "same_call_original_monitor", "timestamp_provenance": "response_received_at_fallback",
            "quantity_units": "unavailable", "fetch": fetch.model_dump(mode="json"), "original_quotes": raw_quotes,
            "basis_sha256": sha256(basis.encode()).hexdigest(), "snapshot_sha256": sha256(snapshot.encode()).hexdigest()}).decode()
        payload, settings = require_builtin_capture_authority(authority)
        state, reason = ("disconnected", "missing_quotes") if missing else ("stale", "source_stale") if stale else ("waiting", "empty_watchlist") if not universe else ("ready", "captured")
        publish_original_builtin_capture(authority, capture=MonitorBuiltinCaptureRecord(origin="original_monitor",
            producer_manifest_sha256=sha256(payload).hexdigest(), producer_commit=strict_json_loads(payload)["producer_commit"],
            source_generation_id=settings.source_generation_id, source_sequence=next_builtin_capture_sequence(authority),
            trade_date=fetch.response_received_at.astimezone(ZoneInfo("Asia/Shanghai")).date(), observed_at=fetch.response_received_at,
            available_at=fetch.response_received_at, universe_codes=universe, missing_codes=missing,
            raw_payload_sha256=sha256(snapshot.encode()).hexdigest(), source_receipt_sha256=sha256(receipt.encode()).hexdigest(),
            original_source_receipt_json=receipt, basis_sha256=sha256(basis.encode()).hexdigest(), stock_watch=tuple(watches),
            quotes=() if stale else tuple(typed), original_snapshot_json=None if stale else snapshot, original_basis_json=basis,
            original_results=(), source_state=state, reason=reason))

    def surge_snapshot(
        self, *, snapshot: object, detection_snapshot: object, watcher: object, result: object,
        observed_at: datetime, available_at: datetime, events_path: Path,
    ) -> None:
        import math
        from rquant.price_alert_runtime_source import _quote_file_identity
        from rquant.surge_watch import SurgeWatcher, TickResult, grid_index

        authority = self._authority("original_surge")
        if authority is None:
            return
        if type(watcher) is not SurgeWatcher or type(result) is not TickResult:
            raise TypeError("Surge capture requires the same original watcher and tick result")
        now, available = normalize_aware_utc(observed_at), normalize_aware_utc(available_at)
        snapshot_json, detection_json = _original_frame_json(snapshot), _original_frame_json(detection_snapshot)
        actual_codes = {row["ts_code"] for row in strict_json_loads(snapshot_json)["rows"]}
        universe = tuple(sorted(set(watcher.baseline.code_universe)))
        missing = tuple(sorted(set(universe) - actual_codes))
        if not universe or actual_codes - set(universe):
            raise ValueError("Surge capture lacks its actual complete original market universe")
        original_results = tuple(item.model_dump(mode="json") for item in result.confirmed)
        history = None if not original_results else _original_log_read(events_path)
        if history is not None:
            original_rows = [strict_json_loads(line) for line in history.raw_jsonl.splitlines() if line.strip()]
            if tuple(original_rows[-len(original_results):]) != original_results:
                raise ValueError("Surge results differ from the actual original appended events")
        gi = grid_index(observed_at.time())
        used_codes = {item.ts_code for item in result.confirmed}
        def numbers(values: object) -> list[float | None]:
            return [float(value) if math.isfinite(float(value)) else None for value in values]
        basis = canonical_json_bytes({"config": watcher.config.model_dump(mode="json"), "grid_index": gi,
            "baseline_origin": "actual_original_confirm_cache", "detection_snapshot_sha256": sha256(detection_json.encode()).hexdigest(),
            "detection_snapshot": strict_json_loads(detection_json), "curve": numbers(watcher.baseline.curve),
            "avg_amount_20d": dict(watcher.baseline.avg_amount_20d),
            "confirm_cache": {code: {"cum_median": numbers(watcher.confirm_cache[code].cum_median),
                "minute_median": numbers(watcher.confirm_cache[code].minute_median), "days_used": watcher.confirm_cache[code].days_used} for code in sorted(used_codes)},
            "today_cum_series": {code: numbers(watcher.today_cum_series[code]) for code in sorted(used_codes) if code in watcher.today_cum_series},
            "today_price_strength": {code: watcher.today_price_strength[code].model_dump(mode="json") for code in sorted(used_codes) if code in watcher.today_price_strength},
            "price_source_mode": "rt_min_daily" if watcher._today_cum_fetcher is not None else "snapshot_approximate",
            "push_dates_5d": {code: sorted(day.isoformat() for day in watcher._push_dates_5d.get(code, ())) for code in sorted(used_codes)}}).decode()
        detections = tuple(BuiltinStockDetection(ts_code=item.ts_code, stock_name=item.name, pool="market", kind=item.status,
            trigger_price=item.price, threshold=item.rel_cum, trigger_type="surge_confirmed", original_result_json=canonical_json_bytes(raw).decode())
            for item, raw in zip(result.confirmed, original_results, strict=True))
        receipt = canonical_json_bytes({"capture_kind": "same_call_surge_watch", "precision": "original_minute_clock",
            "observed_at": now.isoformat(), "available_at": available.isoformat(), "results": original_results,
            "history": None if history is None else history.model_dump(mode="json"), "snapshot_sha256": sha256(snapshot_json.encode()).hexdigest(),
            "basis_sha256": sha256(basis.encode()).hexdigest()}).decode()
        payload, settings = require_builtin_capture_authority(authority)
        if history is not None and _quote_file_identity(history.path) != history.physical_identity:
            raise ValueError("original Surge events changed before capture publication")
        publish_original_builtin_capture(authority, capture=MonitorBuiltinCaptureRecord(origin="original_surge",
            producer_manifest_sha256=sha256(payload).hexdigest(), producer_commit=strict_json_loads(payload)["producer_commit"],
            source_generation_id=settings.source_generation_id, source_sequence=next_builtin_capture_sequence(authority), trade_date=observed_at.date(),
            observed_at=now, available_at=available, universe_codes=universe, missing_codes=missing,
            raw_payload_sha256=sha256(snapshot_json.encode()).hexdigest(), source_receipt_sha256=sha256(receipt.encode()).hexdigest(),
            original_source_receipt_json=receipt, basis_sha256=sha256(basis.encode()).hexdigest(), stock_watch=(), quotes=(),
            original_snapshot_json=snapshot_json, original_basis_json=basis,
            original_results=() if missing else detections,
            source_state="disconnected" if missing else "ready",
            reason="incomplete_universe" if missing else "captured"))

    def pulse_snapshot(
        self, *, snapshot: object, session: object, point: object, alerts: tuple[object, ...],
        observed_at: datetime, available_at: datetime, market_universe: tuple[str, ...],
    ) -> None:
        from rquant.pulse_watch import PulseAlert, PulsePoint, PulseSession, pulse_path
        from rquant.price_alert_runtime_source import _quote_file_identity
        from rquant.surge_watch import grid_index

        authority = self._authority("original_pulse")
        if authority is None:
            return
        if type(session) is not PulseSession or type(point) is not PulsePoint or any(type(item) is not PulseAlert for item in alerts):
            raise TypeError("Pulse capture must use the same actual original session and results")
        now, available = normalize_aware_utc(observed_at), normalize_aware_utc(available_at)
        history = _original_log_read(pulse_path(session.live_dir, session.day))
        rows = [strict_json_loads(row) for row in history.raw_jsonl.splitlines() if row.strip()]
        raw_point = point.model_dump(mode="json")
        if not rows or rows[-1] != raw_point or session.day != observed_at.date() or point.t != observed_at.strftime("%H:%M"):
            raise ValueError("Pulse capture differs from the actual original appended point")
        snapshot_json = _original_frame_json(snapshot)
        actual_codes = {row["ts_code"] for row in strict_json_loads(snapshot_json)["rows"]}
        universe = tuple(sorted(set(market_universe)))
        missing = tuple(sorted(set(universe) - actual_codes))
        if (not universe or len(universe) > 8000 or actual_codes - set(universe)):
            raise ValueError("Pulse capture has no complete original market universe")
        gi = grid_index(observed_at.time())
        reference = session.watcher._reference(gi)
        basis = canonical_json_bytes({"subject": "market", "config": session.watcher.config.model_dump(mode="json"),
            "points": [{"grid_index": index, "point": row.model_dump(mode="json")} for index, row in session.watcher._points],
            "reference": None if reference is None else reference.model_dump(mode="json"),
            "cooldown": dict(session.watcher._last_alert_gi)}).decode()
        results = tuple(BuiltinMarketDetection(kind=item.kind, before=item.before, after=item.after, window_minutes=item.window_minutes,
            original_result_json=canonical_json_bytes(item.model_dump(mode="json")).decode()) for item in alerts)
        receipt = canonical_json_bytes({"capture_kind": "same_call_pulse_session", "subject": "market", "precision": "original_minute_clock",
            "observed_at": now.isoformat(), "available_at": available.isoformat(), "point": raw_point,
            "results": [item.model_dump(mode="json") for item in alerts], "history": history.model_dump(mode="json"),
            "snapshot_sha256": sha256(snapshot_json.encode()).hexdigest(), "basis_sha256": sha256(basis.encode()).hexdigest()}).decode()
        payload, settings = require_builtin_capture_authority(authority)
        if _quote_file_identity(history.path) != history.physical_identity:
            raise ValueError("original Pulse log changed before capture publication")
        publish_original_builtin_capture(authority, capture=MonitorBuiltinCaptureRecord(origin="original_pulse",
            producer_manifest_sha256=sha256(payload).hexdigest(), producer_commit=strict_json_loads(payload)["producer_commit"],
            source_generation_id=settings.source_generation_id, source_sequence=next_builtin_capture_sequence(authority),
            trade_date=session.day, observed_at=now, available_at=available, universe_codes=universe,
            missing_codes=missing, raw_payload_sha256=sha256(snapshot_json.encode()).hexdigest(),
            source_receipt_sha256=sha256(receipt.encode()).hexdigest(), original_source_receipt_json=receipt,
            basis_sha256=sha256(basis.encode()).hexdigest(), stock_watch=(), quotes=(), original_snapshot_json=snapshot_json,
            original_basis_json=basis, original_results=() if missing else results,
            pulse_point_json=canonical_json_bytes(raw_point).decode(),
            source_state="disconnected" if missing else "ready",
            reason="incomplete_universe" if missing else "captured"))

    def unavailable(self, *, origins: tuple[str, ...], observed_at: datetime, reason: str) -> None:
        for origin in origins:
            authority = self._authority(origin)
            if authority is not None:
                publish_original_builtin_status(authority, observed_at=observed_at, state="disconnected", reason=reason,
                    receipt_json=canonical_json_bytes({"actual_source_failure": reason, "observed_at": normalize_aware_utc(observed_at).isoformat()}).decode())


_ORIGINAL_OUTLETS: WeakKeyDictionary[OriginalBuiltinSourceOutlet, tuple[MonitorBuiltinCaptureAuthority, ...]] = WeakKeyDictionary()


def bind_original_builtin_source_outlet(authorities: tuple[MonitorBuiltinCaptureAuthority, ...]) -> OriginalBuiltinSourceOutlet:
    origins = [require_builtin_capture_authority(item)[1].origin for item in authorities]
    if not origins or len(set(origins)) != len(origins) or any(origin == "watchlist_quote" for origin in origins):
        raise ValueError("original builtin outlet requires distinct actual legacy source roles")
    result = object.__new__(OriginalBuiltinSourceOutlet)
    _ORIGINAL_OUTLETS[result] = authorities
    return result


def require_original_builtin_source_outlet(value: object) -> OriginalBuiltinSourceOutlet:
    if type(value) is not OriginalBuiltinSourceOutlet or value not in _ORIGINAL_OUTLETS:
        raise TypeError("builtin source requires its actual original verified outlet")
    for authority in _ORIGINAL_OUTLETS[value]:
        require_builtin_capture_authority(authority)
    return value


class OriginalBuiltinOutletSettings(BuiltinModel):
    contract: Literal["rquant.original-monitor-builtin-outlet/v1"] = "rquant.original-monitor-builtin-outlet/v1"
    sources: tuple[MonitorBuiltinCaptureReference, ...] = Field(min_length=1, max_length=3)

    @model_validator(mode="after")
    def distinct_sources(self) -> OriginalBuiltinOutletSettings:
        if (len({source.origin for source in self.sources}) != len(self.sources)
                or any(source.origin == "watchlist_quote" for source in self.sources)
                or len(self.wire_bytes()) > 64 * 1024):
            raise ValueError("original builtin outlet must bind its complete bounded actual roles")
        return self


def load_original_builtin_source_outlet(path: Path) -> OriginalBuiltinSourceOutlet:
    root = path.parent
    _private_root(root)
    raw = _activation_bytes(path, root)
    settings = OriginalBuiltinOutletSettings.model_validate_json(raw)
    if settings.wire_bytes() != raw:
        raise ValueError("original builtin source binding must retain its frozen canonical bytes")
    authorities = []
    for source in settings.sources:
        authority = verify_builtin_capture_authority(source.manifest_path, runtime_root=source.runtime_root,
            expected_sha256=source.manifest_sha256, expected_commit=source.producer_commit)
        if require_builtin_capture_authority(authority)[1].origin != source.origin:
            raise ValueError("original builtin binding source origin changed")
        authorities.append(authority)
    if _activation_bytes(path, root) != raw:
        raise ValueError("original builtin source binding changed during installation")
    return bind_original_builtin_source_outlet(tuple(authorities))


def _private_root(root: Path, *, create: bool = False) -> None:
    if create:
        root.mkdir(mode=0o700, parents=False, exist_ok=True)
    current = root.lstat()
    if not stat.S_ISDIR(current.st_mode) or current.st_uid != os.geteuid() or stat.S_IMODE(current.st_mode) != 0o700 or root.resolve() != root:
        raise ValueError("builtin capture directory must be owned, private and unsymlinked")


def _read_capture(path: Path) -> tuple[bytes, tuple[int, int, int, int]]:
    from rquant.live_spool import _secure_read_regular_file

    before = path.lstat()
    raw = _secure_read_regular_file(path, label="original builtin capture", max_bytes=64 * 1024 * 1024)
    after = path.lstat()
    identity = lambda value: (value.st_dev, value.st_ino, value.st_size, value.st_mtime_ns)
    if identity(before) != identity(after) or len(raw) != before.st_size:
        raise ValueError("builtin capture moved during the same owned read")
    return raw, identity(before)


def publish_original_builtin_capture(
    authority: MonitorBuiltinCaptureAuthority, *, capture: MonitorBuiltinCaptureRecord
) -> Path:
    payload, settings = require_builtin_capture_authority(authority)
    if type(capture) is not MonitorBuiltinCaptureRecord or (
        capture.producer_manifest_sha256 != sha256(payload).hexdigest()
        or capture.producer_commit != strict_json_loads(payload)["producer_commit"]
        or capture.source_generation_id != settings.source_generation_id or capture.origin != settings.origin
    ):
        raise ValueError("original builtin capture differs from its actual publishing owner")
    if (capture.origin in {"original_surge", "original_pulse"}
            and capture.missing_codes and capture.source_state == "ready"):
        raise ValueError("whole-market builtin capture is incomplete")
    raw = capture.wire_bytes()
    _private_root(settings.capture_root, create=True)
    target = settings.capture_root / (settings.origin + ".json")
    if target.exists():
        previous = MonitorBuiltinCaptureRecord.model_validate_json(_read_capture(target)[0])
        if capture.source_generation_id == previous.source_generation_id and (
            capture.source_sequence != previous.source_sequence + 1 or capture.available_at < previous.available_at
        ):
            raise ValueError("original builtin capture sequence or owner clock regressed")
    elif capture.source_sequence != 1:
        raise ValueError("first original builtin capture must begin at one")
    temporary = target.with_name(target.name + ".pending")
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0), 0o600)
    try:
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(raw)
            stream.flush()
            os.fsync(stream.fileno())
        require_builtin_capture_authority(authority)
        os.replace(temporary, target)
        directory = os.open(settings.capture_root, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        temporary.unlink(missing_ok=True)
    return target


class MonitorBuiltinCapturedRead:
    __slots__ = ("__weakref__",)

    def __init__(self) -> None:
        raise TypeError("builtin material requires the actual complete same-read source")


_READS: WeakKeyDictionary[MonitorBuiltinCapturedRead, tuple[MonitorBuiltinMaterial, MonitorBuiltinCaptureAuthority, Path]] = WeakKeyDictionary()


def read_original_builtin_capture(
    authority: MonitorBuiltinCaptureAuthority, *, read_at: datetime
) -> MonitorBuiltinCapturedRead:
    payload, settings = require_builtin_capture_authority(authority)
    _private_root(settings.capture_root)
    path = settings.capture_root / (settings.origin + ".json")
    raw, identity = _read_capture(path)
    capture = MonitorBuiltinCaptureRecord.model_validate_json(raw)
    if (capture.origin in {"original_surge", "original_pulse"}
            and capture.missing_codes and capture.source_state == "ready"):
        raise ValueError("whole-market builtin capture is incomplete")
    if (capture.wire_bytes() != raw or capture.producer_manifest_sha256 != sha256(payload).hexdigest()
            or capture.producer_commit != strict_json_loads(payload)["producer_commit"]
            or capture.source_generation_id != settings.source_generation_id or capture.origin != settings.origin):
        raise ValueError("builtin capture is outside its actual immutable source installation")
    material = MonitorBuiltinMaterial(capture=capture, raw_capture_json=raw.decode(), raw_capture_sha256=sha256(raw).hexdigest(),
        read_at=normalize_aware_utc(read_at), physical_identity=identity)
    require_builtin_capture_authority(authority)
    value = object.__new__(MonitorBuiltinCapturedRead)
    _READS[value] = material, authority, path
    return value


def require_builtin_captured_read(value: object) -> MonitorBuiltinMaterial:
    if type(value) is not MonitorBuiltinCapturedRead or value not in _READS:
        raise TypeError("builtin evaluation needs an original same-read capability")
    material, authority, path = _READS[value]
    require_builtin_capture_authority(authority)
    raw, identity = _read_capture(path)
    if raw != material.raw_capture_json.encode() or identity != material.physical_identity:
        raise ValueError("builtin source changed after its original owned read")
    return material


def original_stock_detections(
    kind: Literal["pool2_levels", "pool_attack"], watch: tuple[BuiltinWatchFacts, ...], quotes: tuple[BuiltinQuoteFacts, ...]
) -> tuple[BuiltinStockDetection, ...]:
    from rquant.monitor import RealtimeQuote, WatchItem, check_attack_signals, check_levels

    quote_map = {quote.ts_code: quote for quote in quotes}
    result: list[BuiltinStockDetection] = []
    for facts in watch:
        if kind == "pool2_levels" and facts.pool != "pool2":
            continue
        quote_facts = quote_map.get(facts.ts_code)
        if quote_facts is None:
            continue
        item = WatchItem(**facts.model_dump(mode="python"))
        quote = RealtimeQuote(**quote_facts.model_dump(mode="python", exclude={"observed_at"}))
        detections = check_levels(item, quote.price, quote.low) if kind == "pool2_levels" else check_attack_signals(item, quote)
        for detection in detections:
            result.append(BuiltinStockDetection(ts_code=item.ts_code, stock_name=item.name, pool=item.pool,
                kind=detection["level"], trigger_price=float(detection["trigger_price"]), threshold=float(detection["level_price"]),
                trigger_type=detection["trigger_type"], original_result_json=canonical_json_bytes(detection).decode()))
    return tuple(result)


class MonitorBuiltinRoundInput(BuiltinModel):
    evaluated_at: AwareUtcDatetime
    definitions: tuple[MonitorBuiltinDefinition, ...] = Field(max_length=128)
    material: MonitorBuiltinMaterial | None
    unavailable_reason: Literal["capture_unavailable", "source_stale", "outside_window", "source_disabled"] | None = None
    unavailable_origin: Literal["watchlist_quote", "original_monitor", "original_surge", "original_pulse"] | None = None

    @model_validator(mode="after")
    def complete_input(self) -> MonitorBuiltinRoundInput:
        keys = tuple((row.owner_id, row.builtin_id) for row in self.definitions)
        if keys != tuple(sorted(set(keys))) or len({row.owner_id for row in self.definitions}) > 32:
            raise ValueError("builtin round definitions must be the complete ordered owner domain")
        if (self.material is None) != (self.unavailable_reason is not None):
            raise ValueError("builtin unknown input must state the actual missing source")
        if (self.material is None) != (self.unavailable_origin is not None):
            raise ValueError("builtin missing source must keep its actual configured origin")
        if self.material is not None and self.material.read_at > self.evaluated_at:
            raise ValueError("builtin round input is future")
        if len(self.wire_bytes()) > 64 * 1024 * 1024:
            raise ValueError("builtin round exceeds the original condition input budget")
        return self


class MonitorBuiltinRoundReceipt(BuiltinModel):
    round_id: PriceSha256
    evaluated_at: AwareUtcDatetime
    source_high_watermark: StrictInt = Field(ge=0)
    suppressed_count: StrictInt = Field(ge=0)
    input: MonitorBuiltinRoundInput
    events: tuple[ConditionAlertProducerEventRecord, ...] = ()

    @model_validator(mode="after")
    def exact_receipt(self) -> MonitorBuiltinRoundReceipt:
        if self.round_id != self.input.sha256 or self.evaluated_at != self.input.evaluated_at:
            raise ValueError("builtin round receipt differs from its complete original input")
        sequences = tuple(row.sequence for row in self.events)
        if sequences and sequences != tuple(range(sequences[0], self.source_high_watermark + 1)):
            raise ValueError("builtin round event prefix has a gap")
        definitions = {(row.owner_id, row.builtin_id): row for row in self.input.definitions}
        material = self.input.material
        for row in self.events:
            event = row.event
            if (type(event) is not BuiltinConditionAlertEventEnvelope or material is None
                    or definitions.get((event.owner_id, event.builtin_id)) != event.definition
                    or event.material_sha256 != material.sha256 or event.decision_time != self.evaluated_at):
                raise ValueError("builtin event is not backed by its complete original round")
        return self


class MonitorBuiltinCaptureReference(BuiltinModel):
    origin: Literal["watchlist_quote", "original_monitor", "original_surge", "original_pulse"]
    manifest_path: Path
    runtime_root: Path
    manifest_sha256: PriceSha256
    producer_commit: str = Field(pattern=r"^[0-9a-f]{40}$", strict=True)

    @model_validator(mode="after")
    def private_manifest(self) -> MonitorBuiltinCaptureReference:
        if (not self.runtime_root.is_absolute() or ".." in self.runtime_root.parts
                or self.runtime_root != self.runtime_root.resolve() or not self.manifest_path.is_relative_to(self.runtime_root)):
            raise ValueError("builtin source reference must retain its actual private role root")
        return self


class MonitorBuiltinOwnerSettings(BuiltinModel):
    enabled: StrictBool = False
    definitions: tuple[MonitorBuiltinDefinition, ...] = Field(default=(), max_length=128)
    sources: tuple[MonitorBuiltinCaptureReference, ...] = Field(default=(), max_length=3)

    @model_validator(mode="after")
    def complete_owners(self) -> MonitorBuiltinOwnerSettings:
        keys = tuple((row.owner_id, row.builtin_id) for row in self.definitions)
        if keys != tuple(sorted(set(keys))) or len({row.owner_id for row in self.definitions}) > 32:
            raise ValueError("builtin installation must keep every actual owner and definition")
        if any(row.code_contract_sha256 != builtin_source_contract_sha256() for row in self.definitions):
            raise ValueError("builtin installed definition differs from the original detection code")
        origins = tuple(row.origin for row in self.sources)
        if len(set(origins)) != len(origins) or {"watchlist_quote", "original_monitor"} <= set(origins):
            raise ValueError("builtin installation must keep a single actual source for each detector")
        return self


def read_installed_builtin_settings(activation: ConditionAlertRuntimeActivation) -> MonitorBuiltinOwnerSettings | None:
    require_condition_alert_activation(activation, "evaluation")
    raw = read_condition_activation_setting(activation, "monitor_builtin")
    return None if raw is None else MonitorBuiltinOwnerSettings.model_validate_json(canonical_json_bytes(raw))


def builtin_head_definitions(
    activation: ConditionAlertRuntimeActivation, definitions: tuple[MonitorBuiltinDefinition, ...], *, evaluated_at: datetime | None = None,
    borrowed_condition: ConditionAlertRuntimeStore | None = None,
    source_connection: sqlite3.Connection | None = None,
) -> MonitorBuiltinOwnerSettings:
    actual = read_effective_builtin_settings(activation, now=evaluated_at,
        borrowed_condition=borrowed_condition, source_connection=source_connection)
    if actual is None or not actual.enabled or definitions != actual.definitions:
        raise ValueError("builtin definitions differ from the actual original owner installation")
    return actual


def read_builtin_control_snapshot(
    activation: ConditionAlertRuntimeActivation, *, now: datetime | None,
    borrowed_condition: ConditionAlertRuntimeStore | None = None,
    source_connection: sqlite3.Connection | None = None,
) -> MonitorControlSnapshot | None:
    actual = read_installed_builtin_settings(activation)
    raw = read_condition_activation_setting(activation, "monitor_control")
    if actual is None or raw is None:
        return None
    if now is None:
        raise ValueError("builtin controls need their actual owner observation time")
    from rquant.condition_alert_runtime_contracts import condition_activation_runtime_root
    from rquant.notifier_operator import MonitorControlReadSettings, read_monitor_control_state

    settings = MonitorControlReadSettings.model_validate(raw)
    snapshot = read_monitor_control_state(settings, runtime_root=condition_activation_runtime_root(activation), now=now,
        borrowed_condition=borrowed_condition, source_connection=source_connection)
    if snapshot.installation.builtin_definitions != actual.definitions:
        raise ValueError("builtin control definitions differ from the original installed owner")
    return snapshot


def read_effective_builtin_settings(activation: ConditionAlertRuntimeActivation, *, now: datetime | None,
    borrowed_condition: ConditionAlertRuntimeStore | None = None,
    source_connection: sqlite3.Connection | None = None,
) -> MonitorBuiltinOwnerSettings | None:
    actual = read_installed_builtin_settings(activation)
    snapshot = read_builtin_control_snapshot(activation, now=now,
        borrowed_condition=borrowed_condition, source_connection=source_connection)
    if actual is None or snapshot is None:
        return actual
    return MonitorBuiltinOwnerSettings.model_validate(actual.model_dump() | {
        "definitions": tuple(row.definition for row in snapshot.builtins)})


def detections_for_definition(
    definition: MonitorBuiltinDefinition, capture: MonitorBuiltinCaptureRecord
) -> tuple[BuiltinStockDetection | BuiltinMarketDetection, ...]:
    if (not definition.enabled or capture.source_state != "ready"
            or capture.origin in {"original_surge", "original_pulse"} and capture.missing_codes):
        return ()
    if definition.builtin_id in {"pool2_levels", "pool_attack"}:
        if capture.origin not in {"watchlist_quote", "original_monitor"}:
            return ()
        return original_stock_detections(definition.builtin_id, capture.stock_watch, capture.quotes)
    if (definition.builtin_id, capture.origin) not in {("surge", "original_surge"), ("pulse", "original_pulse")}:
        return ()
    return capture.original_results


class MonitorBuiltinRuntimeHead(BuiltinModel):
    owner_id: OwnerId
    builtin_id: BuiltinId
    definition: MonitorBuiltinDefinition
    evaluated_at: AwareUtcDatetime
    round_id: PriceSha256
    source_state: Literal["ready", "waiting", "stale", "disconnected", "unknown", "disabled"]
    reason: str = Field(min_length=1, max_length=80, strict=True)
    material_sha256: PriceSha256 | None
    source_generation_id: PriceSha256 | None
    source_sequence: StrictInt | None = Field(default=None, ge=0)
    observed_at: AwareUtcDatetime | None
    available_at: AwareUtcDatetime | None
    basis_sha256: PriceSha256 | None
    source_receipt_sha256: PriceSha256 | None
    scope_version: PriceSha256 | None
    member_digest: PriceSha256 | None
    matched_count: StrictInt | None = Field(ge=0)
    last_triggered_at: AwareUtcDatetime | None
    actual_generation_id: PriceSha256
    applied_revision: StrictInt | None = Field(default=None, ge=0, le=2**63 - 1)
    applied_command_id: str | None = Field(default=None, min_length=1, max_length=128, strict=True)
    monitor_installation_sha256: PriceSha256 | None = None

    @model_validator(mode="after")
    def complete_application(self) -> MonitorBuiltinRuntimeHead:
        if self.applied_revision is None:
            if self.applied_command_id is not None or self.monitor_installation_sha256 is not None:
                raise ValueError("builtin application has no actual owner revision")
        elif (self.monitor_installation_sha256 is None
                or (self.applied_revision == 0) != (self.applied_command_id is None)):
            raise ValueError("builtin application requires its original installation and command")
        return self


class BuiltinDeliveryHead(BuiltinModel):
    head: MonitorBuiltinRuntimeHead
    current_source_ready: StrictBool


class BuiltinDeliveryEventRef(BuiltinModel):
    event_id: PriceSha256
    payload_sha256: PriceSha256
    owner_id: OwnerId
    builtin_id: BuiltinId
    material_sha256: PriceSha256
    scope_version: PriceSha256
    member_digest: PriceSha256
    expires_at: AwareUtcDatetime


class MonitorBuiltinDeliveryState(BuiltinModel):
    source: ConditionAlertSourceDescriptor
    inspected_at: AwareUtcDatetime
    heads: tuple[BuiltinDeliveryHead, ...] = Field(max_length=128)
    events: tuple[BuiltinDeliveryEventRef, ...] = Field(max_length=100000)

    @model_validator(mode="after")
    def complete_owned_facts(self) -> MonitorBuiltinDeliveryState:
        keys = tuple((row.head.owner_id, row.head.builtin_id) for row in self.heads)
        if keys != tuple(sorted(set(keys))) or len({key[0] for key in keys}) > 32:
            raise ValueError("builtin delivery omits or repeats an owner head")
        if any(row.head.actual_generation_id != self.source.generation_id
               or row.head.evaluated_at > self.inspected_at for row in self.heads):
            raise ValueError("builtin delivery heads differ from their original owner generation")
        if len({row.event_id for row in self.events}) != len(self.events):
            raise ValueError("builtin delivery repeats a sealed original event")
        if len(self.wire_bytes()) > 8 * 1024 * 1024:
            raise ValueError("builtin delivery exceeds the original authority capacity")
        return self


class MonitorBuiltinDeliveryInspection:
    __slots__ = ("__weakref__",)

    def __init__(self) -> None:
        raise TypeError("builtin delivery inspection requires the original owner and captured reads")


_BUILTIN_DELIVERY: WeakKeyDictionary[
    MonitorBuiltinDeliveryInspection, tuple[object, tuple[MonitorBuiltinCapturedRead, ...], MonitorBuiltinDeliveryState]
] = WeakKeyDictionary()


def _builtin_current_policy(capture: MonitorBuiltinCaptureRecord) -> str:
    # Minute points and cumulative series change while the original installed rule stays the same.
    basis = strict_json_loads(capture.original_basis_json)
    stable = (
        tuple(row.model_dump(mode="json") for row in capture.stock_watch)
        if capture.origin in {"watchlist_quote", "original_monitor"}
        else {key: basis[key] for key in ("config", "curve", "avg_amount_20d") if key in basis}
    )
    return canonical_sha256({"origin": capture.origin, "date": capture.trade_date,
        "source": capture.source_generation_id, "producer": capture.producer_manifest_sha256,
        "universe": capture.universe_codes, "rule_basis": stable})


def inspect_original_builtin_delivery(
    owner: object, *, captured: tuple[MonitorBuiltinCapturedRead, ...], inspected_at: datetime,
) -> MonitorBuiltinDeliveryInspection:
    from rquant.condition_alert_runtime import ConditionAlertRuntimeStore, verify_condition_runtime_namespace
    from rquant.condition_alert_runtime_contracts import parse_condition_alert_event

    if type(owner) is not ConditionAlertRuntimeStore:
        raise TypeError("builtin delivery inspection requires the exact original condition owner")
    now = normalize_aware_utc(inspected_at)
    installed = read_effective_builtin_settings(owner.activation, now=now)
    if installed is None or not installed.enabled:
        raise ValueError("builtin original owner is not installed")
    materials = tuple(require_builtin_captured_read(item) for item in captured)
    current = {item.capture.origin: item for item in materials}
    if len(current) != len(materials) or any(item.read_at > now for item in materials):
        raise ValueError("builtin delivery capture set is duplicate or future")
    with owner.ledger._connection() as connection:
        source = verify_condition_runtime_namespace(connection)
        verify_builtin_metadata(connection)
        rows = connection.execute("SELECT body FROM monitor_builtin_head ORDER BY owner_id,builtin_id").fetchall()
        if len(rows) > 128:
            raise ValueError("builtin original owner head capacity exceeded")
        heads = tuple(MonitorBuiltinRuntimeHead.model_validate_json(bytes(row[0])) for row in rows)
        expected = {(row.owner_id, row.builtin_id): row for row in installed.definitions}
        if {(row.owner_id, row.builtin_id): row.definition for row in heads} != expected:
            raise ValueError("builtin delivery must inspect every installed original owner head")
        head_facts, by_key = [], {}
        for head in heads:
            row = connection.execute("SELECT body FROM condition_alert_round_receipt WHERE round_id=?", (head.round_id,)).fetchone()
            receipt = None if row is None else MonitorBuiltinRoundReceipt.model_validate_json(bytes(row[0]))
            original = None if receipt is None else receipt.input.material
            material = None if original is None else current.get(original.capture.origin)
            ready = (head.source_state == "ready" and head.definition.enabled and original is not None
                and material is not None and material.capture == original.capture
                and material.raw_capture_sha256 == original.raw_capture_sha256
                and material.physical_identity == original.physical_identity
                and head.material_sha256 == original.sha256 and head.round_id == receipt.round_id
                and material.capture.source_state == "ready" and not material.capture.missing_codes
                and material.capture.available_at <= now
                and now - material.capture.available_at <= timedelta(seconds=15 if material.capture.origin in {"original_monitor", "watchlist_quote"} else 90))
            head_facts.append(BuiltinDeliveryHead(head=head, current_source_ready=bool(ready)))
            by_key[(head.owner_id, head.builtin_id)] = (head, material if ready else None)
        event_rows = connection.execute("SELECT sequence,payload,payload_sha256 FROM condition_alert_event_log "
            "WHERE json_extract(CAST(payload AS TEXT),'$.envelope_schema')='rquant.builtin-condition-alert-event/v1' "
            "ORDER BY sequence").fetchall()
        if len(event_rows) > 100000:
            raise ValueError("builtin original event prefix capacity exceeded")
        proofs = []
        receipts: dict[str, MonitorBuiltinRoundReceipt] = {}
        for sequence, raw, payload_hash in event_rows:
            event = parse_condition_alert_event(bytes(raw))
            if type(event) is not BuiltinConditionAlertEventEnvelope or event.sha256 != payload_hash or sequence > source.high_watermark:
                raise ValueError("builtin original event prefix changed")
            if not event.available_at <= now < event.expires_at:
                continue
            head, material = by_key.get((event.owner_id, event.builtin_id), (None, None))
            if head is None or material is None or head.definition != event.definition:
                continue
            if event.material_sha256 not in receipts:
                # Material identity also includes its actual read receipt, so use the exact event-owning round.
                found = connection.execute("SELECT body FROM condition_alert_round_receipt WHERE "
                    "json_extract(CAST(body AS TEXT),'$.input.material.raw_capture_sha256') IS NOT NULL "
                    "AND EXISTS(SELECT 1 FROM json_each(CAST(body AS TEXT),'$.events') "
                    "WHERE json_extract(value,'$.event.event_id')=?)", (event.event_id,)).fetchall()
                if len(found) != 1:
                    raise ValueError("builtin event has no unique original commit receipt")
                receipts[event.material_sha256] = MonitorBuiltinRoundReceipt.model_validate_json(bytes(found[0][0]))
            receipt = receipts[event.material_sha256]
            original = receipt.input.material
            if (original is None or not any(row.sequence == sequence and row.event == event for row in receipt.events)
                    or event.detection not in detections_for_definition(event.definition, original.capture)
                    or (event.producer_manifest_sha256, event.source_epoch, event.producer_commit)
                    != (owner.binding.producer_manifest_sha256, source.source_epoch, owner.binding.producer_commit)):
                raise ValueError("builtin event numbers or provenance differ from its original commit")
            if _builtin_current_policy(original.capture) != _builtin_current_policy(material.capture):
                continue
            proofs.append(BuiltinDeliveryEventRef(event_id=event.event_id, payload_sha256=event.sha256,
                owner_id=event.owner_id, builtin_id=event.builtin_id, material_sha256=event.material_sha256,
                scope_version=event.scope_version, member_digest=event.member_digest, expires_at=event.expires_at))
        facts = MonitorBuiltinDeliveryState(source=source, inspected_at=now, heads=tuple(head_facts), events=tuple(proofs))
    for item in captured:
        require_builtin_captured_read(item)
    token = object.__new__(MonitorBuiltinDeliveryInspection)
    _BUILTIN_DELIVERY[token] = owner, captured, facts
    return token


def require_builtin_delivery_inspection(value: object) -> MonitorBuiltinDeliveryState:
    if type(value) is not MonitorBuiltinDeliveryInspection or value not in _BUILTIN_DELIVERY:
        raise TypeError("builtin authority needs its actual same-read owner inspection")
    owner, captured, facts = _BUILTIN_DELIVERY[value]
    for item in captured:
        require_builtin_captured_read(item)
    # Check the complete retained owner heads and prefix before granting the notifier authority.
    if owner.source_descriptor() != facts.source or owner.builtin_heads() != tuple(row.head for row in facts.heads):
        raise ValueError("builtin original owner changed after inspection")
    actual = read_effective_builtin_settings(owner.activation, now=facts.inspected_at)
    if actual is None or tuple(row.head.definition for row in facts.heads) != actual.definitions:
        raise ValueError("builtin original control revision changed after inspection")
    return facts


BUILTIN_METADATA_SQL = (
    "CREATE TABLE monitor_builtin_head(owner_id TEXT,builtin_id TEXT,body BLOB NOT NULL,PRIMARY KEY(owner_id,builtin_id))",
    "CREATE TABLE monitor_builtin_day_dedupe(owner_id TEXT,builtin_id TEXT,trade_date TEXT,ts_code TEXT,detection_key TEXT,event_id TEXT NOT NULL,PRIMARY KEY(owner_id,builtin_id,trade_date,ts_code,detection_key))",
)


def verify_builtin_metadata(connection: object, *, install: bool = False) -> None:
    actual = {row[0] for row in connection.execute("SELECT sql FROM sqlite_master WHERE name LIKE 'monitor_builtin_%' AND sql IS NOT NULL")}
    if not actual and install:
        for sql in BUILTIN_METADATA_SQL:
            connection.execute(sql)
        actual = set(BUILTIN_METADATA_SQL)
    if actual != set(BUILTIN_METADATA_SQL):
        raise ValueError("builtin metadata is not installed in the original condition ledger")


def commit_original_builtin_round(
    store: object, *, captured: MonitorBuiltinCapturedRead, definitions: tuple[MonitorBuiltinDefinition, ...],
    evaluated_at: datetime, current_scope: Callable[[], bool] | None = None,
) -> MonitorBuiltinRoundReceipt:
    from rquant.condition_alert_runtime import ConditionAlertRuntimeStore

    if type(store) is not ConditionAlertRuntimeStore:
        raise TypeError("builtin facts must borrow the original condition state owner")
    require_condition_alert_activation(store.activation, "event_write")
    material = require_builtin_captured_read(captured)
    return store.commit_builtin_round(captured=captured, definitions=definitions,
        evaluated_at=evaluated_at, current_scope=current_scope)


class MonitorBuiltinServingWindow(BuiltinModel):
    protocol: Literal["rquant.monitor-builtin-serving-window/v1"] = "rquant.monitor-builtin-serving-window/v1"
    state: Literal["ready", "unavailable"]
    reason: str = Field(min_length=1, max_length=80)
    observed_at: AwareUtcDatetime
    source: ConditionAlertSourceDescriptor | None = None
    source_receipt_sha256: PriceSha256 | None = None
    head_count: StrictInt | None = Field(default=None, ge=0, le=128)
    history_count: StrictInt | None = Field(default=None, ge=0, le=100000)
    returned_history_count: StrictInt = Field(default=0, ge=0, le=1000)
    truncated: StrictBool = False

    @model_validator(mode="after")
    def complete_original_window(self) -> MonitorBuiltinServingWindow:
        if self.state == "ready":
            if any(value is None for value in (self.source, self.source_receipt_sha256, self.head_count, self.history_count)):
                raise ValueError("builtin serving needs its complete original owner receipt")
            if self.returned_history_count > self.history_count or self.truncated != (self.returned_history_count < self.history_count):
                raise ValueError("builtin history coverage differs from the original retained event prefix")
        elif any(value is not None for value in (self.source, self.source_receipt_sha256, self.head_count, self.history_count)) or self.returned_history_count or self.truncated:
            raise ValueError("unavailable builtin source cannot fabricate original counts")
        return self


class MonitorBuiltinServingHead(BuiltinModel):
    head: MonitorBuiltinRuntimeHead
    current_source_ready: StrictBool
    source_receipt_sha256: PriceSha256
    inspected_at: AwareUtcDatetime
    source_valid_until: AwareUtcDatetime | None = None

    @model_validator(mode="after")
    def exact_original_head(self) -> MonitorBuiltinServingHead:
        head = self.head
        if ((head.owner_id, head.builtin_id) != (head.definition.owner_id, head.definition.builtin_id)
                or head.evaluated_at > self.inspected_at
                or any(value is not None and value > self.inspected_at for value in
                       (head.available_at, head.observed_at, head.last_triggered_at))):
            raise ValueError("builtin serving head has another identity or a future original cutoff")
        deadline = None if head.available_at is None else head.available_at + timedelta(
            seconds=90 if head.builtin_id in {"surge", "pulse"} else 15)
        if self.source_valid_until != deadline or self.current_source_ready and (
                deadline is None or self.inspected_at > deadline or head.source_state != "ready"):
            raise ValueError("builtin serving head changed its original validity boundary")
        return self


class MonitorBuiltinServingEvent(BuiltinModel):
    sequence: StrictInt = Field(ge=1)
    event: BuiltinConditionAlertEventEnvelope
    payload_sha256: PriceSha256
    source_receipt_sha256: PriceSha256
    inspected_at: AwareUtcDatetime

    @model_validator(mode="after")
    def exact_original_event(self) -> MonitorBuiltinServingEvent:
        if self.event.sha256 != self.payload_sha256 or self.event.available_at > self.inspected_at:
            raise ValueError("builtin history differs from its exact original sealed event")
        return self


class MonitorBuiltinServingSnapshot(BuiltinModel):
    window: MonitorBuiltinServingWindow
    heads: tuple[MonitorBuiltinServingHead, ...] = Field(max_length=128)
    events: tuple[MonitorBuiltinServingEvent, ...] = Field(max_length=1000)

    @model_validator(mode="after")
    def original_owner_rows(self) -> MonitorBuiltinServingSnapshot:
        if self.window.state != "ready" or self.window.source is None:
            raise ValueError("builtin serving snapshot needs its complete original owner source")
        if (self.window.head_count, self.window.returned_history_count) != (len(self.heads), len(self.events)):
            raise ValueError("builtin serving omits an original owner row")
        if any(row.inspected_at != self.window.observed_at or row.source_receipt_sha256 != self.window.source_receipt_sha256 for row in (*self.heads, *self.events)):
            raise ValueError("builtin serving mixes source receipts or cutoffs")
        if any(row.head.actual_generation_id != self.window.source.generation_id for row in self.heads):
            raise ValueError("builtin serving mixes original owner generations")
        heads = {(row.head.owner_id, row.head.builtin_id) for row in self.heads}
        if len(heads) != len(self.heads) or len({row.sequence for row in self.events}) != len(self.events) or len({row.event.event_id for row in self.events}) != len(self.events):
            raise ValueError("builtin serving repeats an original owner row")
        source = self.window.source
        if any((row.event.owner_id, row.event.builtin_id) not in heads
               or row.sequence > source.high_watermark
               or (row.event.producer_manifest_sha256, row.event.source_epoch,
                   row.event.evaluation_contract_sha256, row.event.frequency_policy_sha256)
               != (source.producer_manifest_sha256, source.source_epoch,
                   source.evaluation_contract_sha256, source.frequency_policy_sha256)
               for row in self.events):
            raise ValueError("builtin serving history differs from its current original owner source")
        return self


class MonitorBuiltinServingRead:
    __slots__ = ("__weakref__",)

    def __init__(self) -> None:
        raise TypeError("builtin serving requires the original owner read")


_BUILTIN_SERVING_READS: WeakKeyDictionary[MonitorBuiltinServingRead, tuple[object, object, MonitorBuiltinServingSnapshot]] = WeakKeyDictionary()


def require_builtin_serving_read(value: object) -> MonitorBuiltinServingSnapshot:
    if type(value) is not MonitorBuiltinServingRead or value not in _BUILTIN_SERVING_READS:
        raise TypeError("builtin serving needs the actual original owner capability")
    owner, inspection, facts = _BUILTIN_SERVING_READS[value]
    inspected = require_builtin_delivery_inspection(inspection)
    if inspected.source != facts.window.source:
        raise ValueError("builtin serving original source changed after its read")
    with owner.ledger._connection() as connection:
        for item in facts.events:
            row = connection.execute("SELECT payload,payload_sha256 FROM condition_alert_event_log WHERE sequence=?", (item.sequence,)).fetchone()
            if row is None or (bytes(row[0]), row[1]) != (item.event.wire_bytes(), item.payload_sha256):
                raise ValueError("builtin serving event changed after its original read")
    return facts


def read_installed_builtin_captures(owner: object, *, observed_at: datetime) -> tuple[MonitorBuiltinCapturedRead, ...]:
    from rquant.condition_alert_runtime import ConditionAlertRuntimeStore

    if type(owner) is not ConditionAlertRuntimeStore:
        raise TypeError("builtin capture set requires its original composed owner")
    installed = read_installed_builtin_settings(owner.activation)
    if installed is None or not installed.enabled:
        return ()
    result = []
    for reference in installed.sources:
        if reference.producer_commit != owner.binding.producer_commit:
            raise ValueError("builtin configured capture differs from its actual owner commit")
        authority = verify_builtin_capture_authority(reference.manifest_path, runtime_root=reference.runtime_root,
            expected_sha256=reference.manifest_sha256, expected_commit=reference.producer_commit)
        result.append(read_original_builtin_capture(authority, read_at=observed_at))
    return tuple(result)


def read_builtin_serving_facts(owner: object, *, captured: tuple[MonitorBuiltinCapturedRead, ...],
    observed_at: datetime, history_limit: int,
) -> MonitorBuiltinServingRead:
    from rquant.condition_alert_runtime import ConditionAlertRuntimeStore, verify_condition_runtime_namespace
    from rquant.condition_alert_runtime_contracts import parse_condition_alert_event

    if type(owner) is not ConditionAlertRuntimeStore or type(history_limit) is not int or not 1 <= history_limit <= 10000:
        raise ValueError("builtin serving requires the original bounded condition owner")
    now = normalize_aware_utc(observed_at)
    inspection = inspect_original_builtin_delivery(owner, captured=captured, inspected_at=now)
    inspected = require_builtin_delivery_inspection(inspection)
    with owner.ledger._connection() as connection:
        source = verify_condition_runtime_namespace(connection)
        if source != inspected.source:
            raise ValueError("builtin owner changed during its same-read serving window")
        tag = "rquant.builtin-condition-alert-event/v1"
        count = connection.execute("SELECT COUNT(*) FROM condition_alert_event_log WHERE json_extract(CAST(payload AS TEXT),'$.envelope_schema')=?", (tag,)).fetchone()[0]
        if count > 100000:
            raise ValueError("builtin history exceeds its original retained owner capacity")
        rows = connection.execute("SELECT sequence,payload,payload_sha256 FROM condition_alert_event_log WHERE "
            "json_extract(CAST(payload AS TEXT),'$.envelope_schema')=? ORDER BY sequence DESC LIMIT ?",
            (tag, min(history_limit, 1000))).fetchall()
        source_sha = canonical_sha256({"contract": "rquant.monitor-builtin-serving-receipt/v1", "source": source,
            "heads": inspected.heads, "history_count": count, "cutoff": now})
        events = []
        for sequence, raw, payload_hash in rows:
            event = parse_condition_alert_event(bytes(raw))
            if type(event) is not BuiltinConditionAlertEventEnvelope or event.sha256 != payload_hash or sequence > source.high_watermark:
                raise ValueError("builtin history original event prefix changed")
            receipts = connection.execute("SELECT body FROM condition_alert_round_receipt WHERE EXISTS(SELECT 1 FROM "
                "json_each(CAST(body AS TEXT),'$.events') WHERE json_extract(value,'$.event.event_id')=?)", (event.event_id,)).fetchall()
            if len(receipts) != 1:
                raise ValueError("builtin history lacks its unique original commit receipt")
            receipt = MonitorBuiltinRoundReceipt.model_validate_json(bytes(receipts[0][0]))
            original = receipt.input.material
            if original is None or not any(row.sequence == sequence and row.event == event for row in receipt.events) or event.detection not in detections_for_definition(event.definition, original.capture):
                raise ValueError("builtin history numbers differ from their original owner receipt")
            events.append(MonitorBuiltinServingEvent(sequence=sequence, event=event, payload_sha256=payload_hash,
                source_receipt_sha256=source_sha, inspected_at=now))
    if require_builtin_delivery_inspection(inspection) != inspected:
        raise ValueError("builtin owner changed after its original serving read")
    facts = MonitorBuiltinServingSnapshot(window=MonitorBuiltinServingWindow(state="ready", reason="original_owner_receipt",
        observed_at=now, source=source, source_receipt_sha256=source_sha, head_count=len(inspected.heads), history_count=count,
        returned_history_count=len(events), truncated=len(events)<count),
        heads=tuple(MonitorBuiltinServingHead(head=row.head, current_source_ready=row.current_source_ready,
            source_receipt_sha256=source_sha, inspected_at=now,
            source_valid_until=None if row.head.available_at is None else row.head.available_at + timedelta(seconds=90 if row.head.builtin_id in {"surge", "pulse"} else 15))
            for row in inspected.heads), events=tuple(events))
    result = object.__new__(MonitorBuiltinServingRead)
    _BUILTIN_SERVING_READS[result] = owner, inspection, facts
    return result


def builtin_serving_projections(facts: object | None, *, observed_at: datetime) -> tuple[object, ...]:
    from rquant.serving_read_models import ServingProjectionPayload

    now = normalize_aware_utc(observed_at)
    if facts is None:
        window = MonitorBuiltinServingWindow(state="unavailable", reason="original_owner_unavailable", observed_at=now)
        heads, events = (), ()
    else:
        original = require_builtin_serving_read(facts)
        if original.window.observed_at != now:
            raise ValueError("builtin serving needs its exact same-cutoff original owner facts")
        window, heads, events = original.window, original.heads, original.events
    return (ServingProjectionPayload(table_name="monitor_builtin_state", available_at=now,
        rows=({"owner_id": "", "builtin_id": "", "body_json": window.model_dump_json()},
            *({"owner_id": row.head.owner_id, "builtin_id": row.head.builtin_id, "body_json": row.model_dump_json()} for row in heads))),
        ServingProjectionPayload(table_name="monitor_builtin_event", available_at=now,
        rows=tuple({"event_id": row.event.event_id, "sequence": row.sequence, "owner_id": row.event.owner_id,
            "builtin_id": row.event.builtin_id, "body_json": row.model_dump_json()} for row in events)))
