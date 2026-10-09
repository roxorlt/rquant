"""Additive builtin facts; stock and market subjects have separate contracts."""

from __future__ import annotations

from datetime import date, datetime, timedelta
from hashlib import sha256
from typing import Annotated, Literal, Self

from pydantic import BaseModel, ConfigDict, Field, StrictBool, StrictFloat, StrictInt, StrictStr, model_validator

from rquant.delivery_contracts import DeliveryChannel
from rquant.manual_watchlist import OwnerId, TsCode
from rquant.price_alert_runtime_contracts import PriceCommit, PriceSha256
from rquant.runtime_contracts import AwareUtcDatetime
from rquant.strict_json import canonical_json_bytes, strict_canonical_json_loads, strict_json_loads

BuiltinId = Literal["pool2_levels", "pool_attack", "surge", "pulse"]
Finite = Annotated[StrictFloat, Field(allow_inf_nan=False)]
PositivePrice = Annotated[StrictFloat, Field(gt=0, allow_inf_nan=False)]


class BuiltinModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True, revalidate_instances="always")

    def wire_bytes(self) -> bytes:
        return canonical_json_bytes(self.model_dump(mode="json"))

    @property
    def sha256(self) -> str:
        return sha256(self.wire_bytes()).hexdigest()


class MonitorBuiltinDefinition(BuiltinModel):
    owner_id: OwnerId
    builtin_id: BuiltinId
    version: StrictInt = Field(default=1, ge=1)
    enabled: StrictBool = False
    channels: tuple[DeliveryChannel, ...] = Field(min_length=1, max_length=2)
    code_contract_sha256: PriceSha256

    @model_validator(mode="after")
    def exact_channels(self) -> Self:
        if len(set(self.channels)) != len(self.channels):
            raise ValueError("builtin definition has repeated channels")
        return self

    @property
    def rule_id(self) -> str:
        return "builtin." + self.builtin_id

    @property
    def name(self) -> str:
        return {"pool2_levels": "档位提醒", "pool_attack": "攻击提醒", "surge": "爆量提醒", "pulse": "异动提醒"}[self.builtin_id]


class BuiltinWatchFacts(BuiltinModel):
    ts_code: TsCode
    pool: Literal["pool1", "pool2"]
    name: StrictStr = Field(max_length=80)
    limit_up_date: date
    entry_date: date | None
    reference_date: date | None
    body_upper: PositivePrice
    body_lower: PositivePrice
    body: Finite
    level_40: PositivePrice
    level_30: PositivePrice
    level_20: PositivePrice
    stop_strong: PositivePrice
    stop_weak: PositivePrice
    t_high: PositivePrice | None
    t_close: PositivePrice | None
    limit_up_price_next: PositivePrice | None


class BuiltinQuoteFacts(BuiltinModel):
    ts_code: TsCode
    price: PositivePrice
    low: PositivePrice
    open: PositivePrice | None
    high: PositivePrice | None
    pre_close: PositivePrice | None
    pct_chg: Finite | None
    volume: Finite | None
    amount: Finite | None
    source: StrictStr = Field(min_length=1, max_length=128)
    observed_at: AwareUtcDatetime


class BuiltinStockDetection(BuiltinModel):
    subject: Literal["stock"] = "stock"
    ts_code: TsCode
    stock_name: StrictStr = Field(max_length=80)
    pool: Literal["pool1", "pool2", "market"]
    kind: StrictStr = Field(min_length=1, max_length=80)
    trigger_price: Finite
    threshold: Finite | None
    trigger_type: StrictStr = Field(min_length=1, max_length=80)
    original_result_json: StrictStr = Field(min_length=1, max_length=4096)

    @model_validator(mode="after")
    def raw_values(self) -> Self:
        raw = strict_canonical_json_loads(self.original_result_json)
        if not isinstance(raw, dict):
            raise ValueError("builtin stock result requires the original complete result")
        if self.trigger_type in ("realtime", "daily_low", "open_strength", "break_t_high", "strong_carry", "near_limit_up"):
            expected = (raw.get("level"), raw.get("trigger_price"), raw.get("level_price"), raw.get("trigger_type"))
            actual = (self.kind, self.trigger_price, self.threshold, self.trigger_type)
        elif self.trigger_type == "surge_confirmed":
            expected = (raw.get("status"), raw.get("price"), raw.get("rel_cum"), raw.get("ts_code"), raw.get("name"))
            actual = (self.kind, self.trigger_price, self.threshold, self.ts_code, self.stock_name)
        else:
            raise ValueError("builtin stock detection is outside the original detectors")
        if actual != expected:
            raise ValueError("builtin stock values differ from the actual original result")
        return self


class BuiltinMarketDetection(BuiltinModel):
    subject: Literal["market"] = "market"
    kind: Literal["limit_up_surge", "broken_surge", "limit_down_surge", "ratio_jump"]
    before: Finite
    after: Finite
    window_minutes: Literal[10] = 10
    original_result_json: StrictStr = Field(min_length=1, max_length=4096)

    @model_validator(mode="after")
    def original_values(self) -> Self:
        raw = strict_canonical_json_loads(self.original_result_json)
        if not isinstance(raw, dict) or (raw.get("kind"), raw.get("before"), raw.get("after"), raw.get("window_minutes")) != (
                self.kind, self.before, self.after, self.window_minutes):
            raise ValueError("market builtin differs from the actual original PulseAlert")
        return self


BuiltinDetection = Annotated[BuiltinStockDetection | BuiltinMarketDetection, Field(discriminator="subject")]


class MonitorBuiltinCaptureRecord(BuiltinModel):
    contract: Literal["rquant.monitor-builtin-capture/v1"] = "rquant.monitor-builtin-capture/v1"
    origin: Literal["watchlist_quote", "original_monitor", "original_surge", "original_pulse"]
    producer_manifest_sha256: PriceSha256
    producer_commit: PriceCommit
    source_generation_id: PriceSha256
    source_sequence: StrictInt = Field(ge=0)
    trade_date: date
    observed_at: AwareUtcDatetime
    available_at: AwareUtcDatetime
    universe_codes: tuple[TsCode, ...] = Field(max_length=8000)
    missing_codes: tuple[TsCode, ...] = Field(max_length=8000)
    raw_payload_sha256: PriceSha256 | None
    original_snapshot_sha256: PriceSha256 | None = None
    source_receipt_sha256: PriceSha256
    original_source_receipt_json: StrictStr = Field(min_length=1, max_length=4 * 1024 * 1024)
    basis_sha256: PriceSha256
    price_unit: Literal["CNY"] = "CNY"
    amount_unit: Literal["CNY"] = "CNY"
    stock_watch: tuple[BuiltinWatchFacts, ...] = Field(max_length=500)
    quotes: tuple[BuiltinQuoteFacts, ...] = Field(max_length=500)
    original_snapshot_json: StrictStr | None = Field(default=None, max_length=64 * 1024 * 1024)
    original_basis_json: StrictStr = Field(min_length=1, max_length=4 * 1024 * 1024)
    original_results: tuple[BuiltinDetection, ...] = Field(max_length=1000)
    pulse_point_json: StrictStr | None = Field(default=None, max_length=4096)
    source_state: Literal["ready", "waiting", "stale", "disconnected", "unknown", "disabled"]
    reason: StrictStr = Field(min_length=1, max_length=80)

    @model_validator(mode="after")
    def complete_capture(self) -> Self:
        if self.source_state == "ready" and self.raw_payload_sha256 is None:
            raise ValueError("ready builtin source requires its actual original payload")
        if self.observed_at > self.available_at or self.available_at - self.observed_at > timedelta(seconds=90):
            raise ValueError("builtin capture time is future or outside the original source window")
        if self.universe_codes != tuple(sorted(set(self.universe_codes))) or self.missing_codes != tuple(sorted(set(self.missing_codes))):
            raise ValueError("builtin capture universe must be complete sorted unique codes")
        if not set(self.missing_codes) <= set(self.universe_codes):
            raise ValueError("builtin missing codes are outside the actual universe")
        if self.source_state != "ready" and self.original_results:
            raise ValueError("unavailable builtin capture cannot assert detections")
        if sha256(self.original_basis_json.encode()).hexdigest() != self.basis_sha256:
            raise ValueError("builtin basis differs from the actual original material")
        strict_canonical_json_loads(self.original_basis_json)
        receipt = strict_canonical_json_loads(self.original_source_receipt_json)
        if sha256(self.original_source_receipt_json.encode()).hexdigest() != self.source_receipt_sha256 or not isinstance(receipt, dict):
            raise ValueError("builtin source receipt differs from the original owned read")
        if self.original_snapshot_json is not None:
            strict_canonical_json_loads(self.original_snapshot_json)
            if sha256(self.original_snapshot_json.encode()).hexdigest() != (self.original_snapshot_sha256 or self.raw_payload_sha256):
                raise ValueError("builtin snapshot differs from the actual raw capture")
        if self.origin in ("watchlist_quote", "original_monitor"):
            codes = tuple(item.ts_code for item in self.stock_watch)
            quoted = tuple(item.ts_code for item in self.quotes)
            if codes != tuple(sorted(set(codes))) or quoted != tuple(sorted(set(quoted))):
                raise ValueError("builtin stock input is not complete and unique")
            if set(codes) - set(self.universe_codes) or set(quoted) - set(self.universe_codes):
                raise ValueError("builtin stock scope differs from the actual requested universe")
            if self.source_state == "ready" and set(codes) - set(quoted):
                raise ValueError("ready builtin source is missing a requested watch member")
            if any(item.observed_at > self.available_at or self.available_at - item.observed_at > timedelta(seconds=15) for item in self.quotes):
                raise ValueError("builtin stock observation is outside the original 15-second window")
            basis = strict_canonical_json_loads(self.original_basis_json)
            snapshot = None if self.original_snapshot_json is None else strict_canonical_json_loads(self.original_snapshot_json)
            if not isinstance(basis, dict) or basis.get("watch") != [item.model_dump(mode="json") for item in self.stock_watch] or (
                self.source_state == "ready" and (snapshot is None or snapshot.get("quotes") != [item.model_dump(mode="json") for item in self.quotes])
            ):
                raise ValueError("builtin stock numbers differ from its complete original watch/quote material")
            if self.origin == "watchlist_quote" and self.source_state == "ready":
                from rquant.price_alert_runtime_source import PriceQuoteFullReadMaterial

                original = PriceQuoteFullReadMaterial.model_validate_json(canonical_json_bytes(receipt["quote_read"]))
                from rquant.monitor_builtin_runtime import MonitorWatchlistMaterial

                watch_read = MonitorWatchlistMaterial.model_validate_json(canonical_json_bytes(receipt["watch_read"]))
                rows = strict_canonical_json_loads(original.original_rows_json)
                row_map = {row["ts_code"]: row for row in rows}
                if (original.snapshot.payload_sha256 != self.raw_payload_sha256
                        or original.snapshot.source_generation_id != self.source_generation_id
                        or set(original.snapshot.requested_codes) != set(self.universe_codes)
                        or snapshot.get("rows") != rows or watch_read.watch != self.stock_watch
                        or watch_read.basis_json != self.original_basis_json):
                    raise ValueError("builtin quote material differs from the actual original owned batch")
                for quote in self.quotes:
                    row = row_map[quote.ts_code]
                    names = ("price", "low", "open", "high", "pre_close", "pct_chg")
                    if any(getattr(quote, name) != row.get(name) for name in names) or quote.observed_at != datetime.fromisoformat(row["observed_at"]):
                        raise ValueError("builtin quote numbers differ from the actual original complete quote read")
            if self.origin == "original_monitor" and self.source_state == "ready":
                from rquant.monitor import RealtimeQuote
                from rquant.monitor_builtin_runtime import OriginalMonitorFetchReceipt

                if receipt.get("capture_kind") != "same_call_original_monitor":
                    raise ValueError("monitor facts lack the actual original fetch receipt")
                fetch = OriginalMonitorFetchReceipt.model_validate_json(canonical_json_bytes(receipt.get("fetch")))
                rows = receipt.get("original_quotes")
                if not isinstance(rows, list) or snapshot.get("original_quotes") != rows or len(rows) != len(self.quotes):
                    raise ValueError("monitor numbers differ from the original fetched member set")
                originals = {row["ts_code"]: RealtimeQuote.model_validate_json(canonical_json_bytes(row)) for row in rows}
                if len(originals) != len(rows):
                    raise ValueError("monitor fetch receipt repeats a member")
                for quote in self.quotes:
                    original = originals[quote.ts_code]
                    observed = fetch.response_received_at if quote.ts_code in fetch.fallback_codes else fetch.tushare_cache_at
                    if (any(getattr(quote, name) != getattr(original, name) for name in ("price", "low", "open", "high", "pre_close", "pct_chg", "source"))
                            or quote.observed_at != observed or quote.volume is not None or quote.amount is not None):
                        raise ValueError("monitor values differ from the original response clock or quantities")
        elif self.stock_watch or self.quotes or self.original_snapshot_json is None and self.source_state == "ready":
            raise ValueError("market builtin cannot borrow a fabricated stock/quote domain")
        if self.origin == "original_pulse":
            if self.source_state == "ready" and self.pulse_point_json is None:
                raise ValueError("Pulse builtin requires the actual point")
            if any(type(result) is not BuiltinMarketDetection for result in self.original_results):
                raise ValueError("Pulse builtin must use the market variant")
            if self.source_state == "ready":
                from zoneinfo import ZoneInfo
                from rquant.monitor_builtin_runtime import OriginalBuiltinLogRead
                from rquant.pulse_watch import PulseConfig, PulsePoint

                if receipt.get("capture_kind") != "same_call_pulse_session":
                    raise ValueError("Pulse facts lack the actual original session receipt")
                point = PulsePoint.model_validate_json(self.pulse_point_json)
                history = OriginalBuiltinLogRead.model_validate_json(canonical_json_bytes(receipt.get("history")))
                raw_points = [strict_json_loads(line) for line in history.raw_jsonl.splitlines() if line.strip()]
                basis = strict_canonical_json_loads(self.original_basis_json)
                config = PulseConfig.model_validate_json(canonical_json_bytes(basis.get("config")))
                if (not raw_points or raw_points[-1] != point.model_dump(mode="json") or receipt.get("point") != point.model_dump(mode="json")
                        or point.t != self.observed_at.astimezone(ZoneInfo("Asia/Shanghai")).strftime("%H:%M")
                        or receipt.get("results") != [strict_canonical_json_loads(result.original_result_json) for result in self.original_results]
                        or receipt.get("snapshot_sha256") != self.raw_payload_sha256 or receipt.get("basis_sha256") != self.basis_sha256
                        or config.window_minutes != 10 or config.cooldown_minutes != 30):
                    raise ValueError("Pulse values differ from the original log, window or same-call material")
        elif any(type(result) is not BuiltinStockDetection for result in self.original_results):
            raise ValueError("stock builtin cannot use a market result")
        if self.origin == "original_surge" and self.source_state == "ready":
            from rquant.monitor_builtin_runtime import OriginalBuiltinLogRead
            from rquant.surge_watch import SurgeConfig

            if receipt.get("capture_kind") != "same_call_surge_watch":
                raise ValueError("Surge facts lack the original same-call source receipt")
            basis = strict_canonical_json_loads(self.original_basis_json)
            SurgeConfig.model_validate_json(canonical_json_bytes(basis.get("config")))
            original_results = [strict_canonical_json_loads(item.original_result_json) for item in self.original_results]
            if receipt.get("results") != original_results or receipt.get("snapshot_sha256") != self.raw_payload_sha256 or receipt.get("basis_sha256") != self.basis_sha256:
                raise ValueError("Surge values differ from the actual original same-call tick")
            if original_results:
                history = OriginalBuiltinLogRead.model_validate_json(canonical_json_bytes(receipt.get("history")))
                logged = [strict_json_loads(line) for line in history.raw_jsonl.splitlines() if line.strip()]
                if logged[-len(original_results):] != original_results or any(row.ts_code not in basis.get("confirm_cache", {}) for row in self.original_results):
                    raise ValueError("Surge values differ from the original appended events and used basis")
        if len(self.wire_bytes()) > 64 * 1024 * 1024:
            raise ValueError("builtin capture exceeds the original complete input budget")
        return self


class MonitorBuiltinMaterial(BuiltinModel):
    schema_version: Literal[1] = 1
    capture: MonitorBuiltinCaptureRecord
    raw_capture_json: StrictStr = Field(min_length=1, max_length=64 * 1024 * 1024)
    raw_capture_sha256: PriceSha256
    read_at: AwareUtcDatetime
    physical_identity: tuple[StrictInt, StrictInt, StrictInt, StrictInt]

    @model_validator(mode="after")
    def same_read_values(self) -> Self:
        raw = self.raw_capture_json.encode()
        if (len(raw) > 64 * 1024 * 1024 or sha256(raw).hexdigest() != self.raw_capture_sha256
                or MonitorBuiltinCaptureRecord.model_validate_json(raw) != self.capture or self.capture.wire_bytes() != raw
                or self.capture.available_at > self.read_at or len(self.physical_identity) != 4
                or any(value < 0 for value in self.physical_identity)):
            raise ValueError("builtin material differs from the complete captured original values")
        return self


class BuiltinConditionAlertEventFacts(BuiltinModel):
    envelope_schema: Literal["rquant.builtin-condition-alert-event/v1"] = "rquant.builtin-condition-alert-event/v1"
    owner_id: OwnerId
    builtin_id: BuiltinId
    definition: MonitorBuiltinDefinition
    rule_id: StrictStr = Field(min_length=1, max_length=128)
    rule_name: StrictStr = Field(min_length=1, max_length=80)
    priority: Literal["P1"] = "P1"
    channels: tuple[DeliveryChannel, ...] = Field(min_length=1, max_length=2)
    rule_version: StrictInt = Field(ge=1)
    rule_body_hash: PriceSha256
    scope_version: PriceSha256
    member_digest: PriceSha256
    detection: BuiltinDetection
    trigger_kind: Literal["matched"] = "matched"
    previous_truth: Literal["unknown", "false"] = "unknown"
    truth: Literal["true"] = "true"
    source_identity: PriceSha256
    raw_batch_id: PriceSha256
    feature_snapshot_id: PriceSha256
    material_sha256: PriceSha256
    trade_date: date
    event_time: AwareUtcDatetime
    decision_time: AwareUtcDatetime
    available_at: AwareUtcDatetime
    expires_at: AwareUtcDatetime
    evaluation_contract_sha256: PriceSha256
    frequency_policy_sha256: PriceSha256
    frequency_bucket: StrictStr = Field(min_length=1, max_length=128)
    producer_manifest_sha256: PriceSha256
    producer_commit: PriceCommit
    source_epoch: PriceSha256

    @model_validator(mode="after")
    def original_definition(self) -> Self:
        definition = self.definition
        if (self.owner_id, self.builtin_id, self.rule_id, self.rule_name, self.rule_version, self.rule_body_hash, self.channels) != (
                definition.owner_id, definition.builtin_id, definition.rule_id, definition.name,
                definition.version, definition.sha256, definition.channels) or not definition.enabled:
            raise ValueError("builtin event differs from its actual enabled installed definition")
        if not self.event_time <= self.decision_time <= self.available_at < self.expires_at or self.expires_at - self.available_at > timedelta(minutes=2):
            raise ValueError("builtin event violates the original condition event lifetime")
        if (self.builtin_id == "pulse") != (type(self.detection) is BuiltinMarketDetection):
            raise ValueError("builtin subject differs from its actual detector")
        return self

    @property
    def ts_code(self) -> str | None:
        return self.detection.ts_code if type(self.detection) is BuiltinStockDetection else None

    @property
    def stock_name(self) -> str | None:
        return self.detection.stock_name if type(self.detection) is BuiltinStockDetection else None


class BuiltinConditionAlertEventEnvelope(BuiltinConditionAlertEventFacts):
    event_id: PriceSha256

    @model_validator(mode="after")
    def sealed_identity(self) -> Self:
        if sha256(canonical_json_bytes(self.model_dump(mode="json", exclude={"event_id"}))).hexdigest() != self.event_id:
            raise ValueError("builtin event identity differs from all original typed facts")
        if len(self.wire_bytes()) > 16 * 1024:
            raise ValueError("builtin event exceeds the original event budget")
        return self

    @classmethod
    def create(cls, **facts: object) -> BuiltinConditionAlertEventEnvelope:
        value = BuiltinConditionAlertEventFacts(**facts)
        return cls(**value.model_dump(mode="python"), event_id=value.sha256)


def parse_builtin_condition_alert_event(value: object) -> BuiltinConditionAlertEventEnvelope:
    payload = value.wire_bytes() if type(value) is BuiltinConditionAlertEventEnvelope else value.encode() if type(value) is str else value
    if type(payload) is not bytes or len(payload) > 16 * 1024:
        raise ValueError("builtin event requires exact bounded canonical bytes")
    strict_canonical_json_loads(payload)
    event = BuiltinConditionAlertEventEnvelope.model_validate_json(payload)
    if event.wire_bytes() != payload:
        raise ValueError("builtin event must keep its exact canonical bytes")
    return event
