"""Bounded original spool reader for signal-owned intraday page projections."""

from __future__ import annotations

import hashlib
import json
import os
import stat
from datetime import datetime
from pathlib import Path
from typing import Literal, Self
from zoneinfo import ZoneInfo

import pandas as pd
from pydantic import Field, field_validator, model_validator

from rquant.definition_registry import ImmutableDefinitionRegistry
from rquant.feature_contracts import FeatureAvailability
from rquant.feature_spool import FeatureBatchSpool
from rquant.intraday_feature_engine import (
    MARKET_MINUTE_FEATURE_MAX_DELAY_SECONDS,
    feature_columns_for_version,
)
from rquant.live_contracts import (
    BatchEnvelope,
    BatchQualityStatus,
    LiveChannel,
    LiveSourceDescriptor,
)
from rquant.live_spool import LiveBatchSpool
from rquant.market_minute_gateway import MarketMinuteGateway
from rquant.runtime_contracts import (
    AwareUtcDatetime,
    RuntimeContractModel,
    canonical_sha256,
    normalize_aware_utc,
)
from rquant.schema_compatibility import ProductionConsumerRegistry, RolloutPhase, SchemaRolloutStore
from rquant.screen.intraday_contracts import (
    INTRADAY_FIELD_LABELS,
    IntradayClosedBarSnapshot,
    IntradayFieldFact,
    IntradayMarketRow,
    IntradayRawInputReceipt,
    IntradaySchemaEvidence,
    IntradayScreenSnapshot,
    IntradayScreenSource,
    IntradayStockSnapshot,
    encode_intraday_source_wire,
    encode_intraday_stock_rows,
)
from rquant.screen.intraday_reference import (
    Code,
    IntradayReferenceObservation,
    IntradayReferenceSnapshot,
    Sha256,
    read_intraday_reference,
    visible_intraday_reference,
)
from rquant.serving_read_models import ServingProjectionPayload
from rquant.strategy_evaluators import BuiltinStrategyEvaluatorRegistry
from rquant.watchlist_quote_gateway import decode_watchlist_quote_payload

_SHANGHAI = ZoneInfo("Asia/Shanghai")
_MAX_PREFIX = 4_096
_MAX_INPUT_BATCHES = 512
_MAX_INPUT_BYTES = 64 * 1024 * 1024
_FEATURE_FIELDS = {
    "INTRADAY_PRICE[0]": "latest_close",
    "INTRADAY_OPEN[0]": "session_open",
    "INTRADAY_HIGH[0]": "session_high",
    "INTRADAY_LOW[0]": "session_low",
    "INTRADAY_VOLUME[0]": "cumulative_volume",
    "INTRADAY_AMOUNT[0]": "cumulative_amount",
    "INTRADAY_SPEED_5M[0]": "speed_5m_pct",
    "INTRADAY_REL_AMOUNT[0]": "rel_cumulative",
    "INTRADAY_SAME_MINUTE_AMOUNT[0]": "rel_same_minute",
    "INTRADAY_VOLUME_RATIO[0]": "cumulative_volume_ratio",
}


class IntradaySchemaGate(RuntimeContractModel):
    store_path: Path
    plan_id: Sha256
    registry: ProductionConsumerRegistry
    consumer_id: str
    consumer_commit: str = Field(pattern=r"^[0-9a-f]{40}$")
    installed_generation_id: Sha256
    declaration_fingerprint: Sha256
    declaration_version: int = Field(ge=1)
    dataset_id: Literal[
        "runtime.intraday_feature.batch-envelope", "runtime.watchlist_quote.batch-envelope"
    ] = "runtime.intraday_feature.batch-envelope"


class IntradayQuoteSourceConfig(RuntimeContractModel):
    spool_root: Path
    producer_commit: str = Field(pattern=r"^[0-9a-f]{40}$")
    expected_generation_id: Sha256
    units_contract_id: Sha256
    schema_gate: IntradaySchemaGate


class IntradayQuoteRow(RuntimeContractModel):
    ts_code: Code
    observed_at: AwareUtcDatetime
    price: float = Field(gt=0, allow_inf_nan=False)
    open: float = Field(gt=0, allow_inf_nan=False)
    high: float = Field(gt=0, allow_inf_nan=False)
    low: float = Field(gt=0, allow_inf_nan=False)
    volume: float = Field(ge=0, allow_inf_nan=False)
    amount: float = Field(ge=0, allow_inf_nan=False)
    pre_close: float | None = Field(default=None, gt=0, allow_inf_nan=False)
    pct_chg: float | None = Field(default=None, allow_inf_nan=False)
    turnover_rate: float | None = Field(default=None, ge=0, allow_inf_nan=False)
    float_shares: float | None = Field(default=None, gt=0, allow_inf_nan=False)
    up_limit: float | None = Field(default=None, gt=0, allow_inf_nan=False)
    no_price_limit: bool | None = None


class IntradayQuoteSnapshot(RuntimeContractModel):
    generation_id: Sha256
    envelope: BatchEnvelope
    schema_evidence: IntradaySchemaEvidence
    rows: tuple[IntradayQuoteRow, ...] = Field(max_length=8000)
    source_descriptor: LiveSourceDescriptor


class _HistoricalFileEvidence(RuntimeContractModel):
    sha256: Sha256
    device: int
    inode: int
    size: int
    modified_ns: int
    changed_ns: int


def _file_identity(value: os.stat_result) -> tuple[int, ...]:
    return (value.st_dev, value.st_ino, value.st_size, value.st_mtime_ns, value.st_ctime_ns)


def _read_history_digest(path: Path) -> _HistoricalFileEvidence:
    descriptor = -1
    try:
        descriptor = os.open(
            path, os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
        )
        before = os.fstat(descriptor)
        maximum = 512 * 1024 * 1024
        if not stat.S_ISREG(before.st_mode) or before.st_size > maximum:
            raise ValueError("historical input is not a bounded regular file")
        digest = hashlib.sha256()
        count = 0
        while chunk := os.read(descriptor, 1024 * 1024):
            count += len(chunk)
            if count > maximum:
                raise ValueError("historical input exceeds its byte bound")
            digest.update(chunk)
        after = os.fstat(descriptor)
        current = path.stat(follow_symlinks=False)
        if (
            count != before.st_size
            or not stat.S_ISREG(current.st_mode)
            or _file_identity(before) != _file_identity(after)
            or _file_identity(before) != _file_identity(current)
        ):
            raise ValueError("historical input changed while reading")
        return _HistoricalFileEvidence(
            sha256=digest.hexdigest(),
            device=before.st_dev,
            inode=before.st_ino,
            size=before.st_size,
            modified_ns=before.st_mtime_ns,
            changed_ns=before.st_ctime_ns,
        )
    except OSError as error:
        raise ValueError("historical input is unavailable") from error
    finally:
        if descriptor >= 0:
            os.close(descriptor)


class IntradaySourceConfig(RuntimeContractModel):
    raw_spool_root: Path
    feature_spool_root: Path
    cursor_root: Path
    definition_root: Path
    feature_definition_fingerprint: Sha256
    expected_feature_generation_id: Sha256
    expected_raw_generation_id: Sha256
    feature_producer_commit: str = Field(pattern=r"^[0-9a-f]{40}$")
    raw_producer_commit: str = Field(pattern=r"^[0-9a-f]{40}$")
    reference_snapshot_path: Path
    reference_snapshot_sha256: Sha256
    historical_snapshot_path: Path
    historical_snapshot_id: Sha256
    units_contract_id: Sha256
    minute_volume_unit: Literal["shares", "lot100"]
    minute_amount_unit: Literal["CNY"]
    schema_gate: IntradaySchemaGate
    quote_source: IntradayQuoteSourceConfig | None = None

    @field_validator(
        "raw_spool_root",
        "feature_spool_root",
        "cursor_root",
        "definition_root",
        "reference_snapshot_path",
        "historical_snapshot_path",
    )
    @classmethod
    def require_absolute_path(cls, value: Path) -> Path:
        if not value.is_absolute() or ".." in value.parts:
            raise ValueError("intraday source path must be absolute and normalized")
        return value

    @model_validator(mode="after")
    def require_owned_cursor_root(self) -> Self:
        for root in (self.raw_spool_root, self.feature_spool_root):
            if self.cursor_root == root or root in self.cursor_root.parents:
                raise ValueError("intraday cursors cannot be inside producer roots")
        if not self.schema_gate.store_path.is_absolute():
            raise ValueError("intraday schema store path must be absolute")
        if self.schema_gate.dataset_id != "runtime.intraday_feature.batch-envelope":
            raise ValueError("intraday feature schema gate uses another dataset")
        if self.quote_source is not None and (
            not self.quote_source.spool_root.is_absolute()
            or ".." in self.quote_source.spool_root.parts
            or self.quote_source.schema_gate.dataset_id != "runtime.watchlist_quote.batch-envelope"
            or self.cursor_root == self.quote_source.spool_root
            or self.quote_source.spool_root in self.cursor_root.parents
        ):
            raise ValueError("intraday quote source path or schema gate changed")
        return self


def read_intraday_schema_evidence(
    gate: IntradaySchemaGate, *, cutoff: datetime
) -> IntradaySchemaEvidence:
    store = SchemaRolloutStore(
        gate.store_path, production_consumer_registry=gate.registry, read_only=True
    )
    state = store.get_state(gate.plan_id)
    if (
        state.phase not in {RolloutPhase.CUTOVER, RolloutPhase.RETIRE}
        or state.updated_at > cutoff
        or state.authority_declaration_fingerprint != gate.declaration_fingerprint
    ):
        raise ValueError("intraday schema is not installed")
    receipts = store.consumer_capability_receipts(gate.plan_id)
    if len(receipts) > 256:
        raise ValueError("intraday schema consumer evidence exceeds bound")
    receipt = next((item for item in receipts if item.consumer_id == gate.consumer_id), None)
    trusted = next(
        (item for item in gate.registry.consumers if item.consumer_id == gate.consumer_id), None
    )
    if (
        receipt is None
        or trusted is None
        or not (
            receipt.dataset_id == trusted.dataset_id == gate.dataset_id
            and receipt.service_id == trusted.service_id
            and receipt.code_commit == trusted.code_commit == gate.consumer_commit
            and receipt.observed_generation_id == gate.installed_generation_id
            and receipt.available_at <= cutoff
            and receipt.min_readable_schema_version
            <= gate.declaration_version
            <= receipt.max_readable_schema_version
        )
    ):
        raise ValueError("intraday schema consumer capability is missing or changed")
    return IntradaySchemaEvidence(
        plan_id=gate.plan_id,
        revision=state.revision,
        phase=state.phase.value,
        consumer_receipt=receipt,
    )


def read_intraday_quotes(
    config: IntradayQuoteSourceConfig,
    *,
    cursor_root: Path,
    reference: IntradayReferenceSnapshot,
    cutoff: datetime,
) -> IntradayQuoteSnapshot:
    schema = read_intraday_schema_evidence(config.schema_gate, cutoff=cutoff)
    spool = LiveBatchSpool(config.spool_root, cursor_root=cursor_root, source_read_only=True)
    descriptor = spool.source_descriptor(LiveChannel.WATCHLIST_QUOTE)
    if (
        descriptor.generation_id != config.expected_generation_id
        or not 0 <= descriptor.high_watermark < _MAX_PREFIX
    ):
        raise ValueError("quote source generation changed")
    records = spool.list_after(LiveChannel.WATCHLIST_QUOTE, sequence=-1)
    visible = [item for item in records if item.envelope.available_at <= cutoff]
    if not visible:
        raise ValueError("quote source is unavailable")
    record = visible[-1]
    envelope = record.envelope
    if (
        envelope.schema_version != 3
        or envelope.producer_commit != config.producer_commit
        or envelope.quality_status is not BatchQualityStatus.PUBLISHED
        or not 1 <= envelope.row_count <= 8000
        or max(
            envelope.event_time_end,
            envelope.source_time,
            envelope.received_at,
            envelope.available_at,
        )
        > cutoff
    ):
        raise ValueError("quote contract or source time is unavailable")
    payload = spool.read_payload(record)
    if len(payload) > 16 * 1024 * 1024:
        raise ValueError("quote source byte budget exceeded")
    frame = decode_watchlist_quote_payload(payload)
    if (
        len(frame) != envelope.row_count
        or frame.ts_code.duplicated().any()
        or set(frame.ts_code) - set(reference.universe_codes)
    ):
        raise ValueError("quote candidate coverage changed")
    rows: list[IntradayQuoteRow] = []
    for row in frame.to_dict(orient="records"):
        digest = canonical_sha256(
            {
                "codes": reference.universe_codes,
                "as_of": row["universe_as_of"].to_pydatetime(),
                "trade_date": reference.trade_date,
            }
        )
        if (
            row["trade_date"] != reference.trade_date
            or row["schema_version"] != 3
            or row["producer_commit"] != config.producer_commit
            or row["units_contract_id"] != config.units_contract_id
            or row["volume_unit"] not in {"shares", "lot100"}
            or row["amount_unit"] != "CNY"
            or row["requested_universe_digest"] != digest
            or row["requested_universe_count"] != len(reference.universe_codes)
            or max(
                row[name]
                for name in (
                    "observed_at",
                    "universe_as_of",
                    "scheduled_at",
                    "requested_at",
                    "response_received_at",
                    "fetched_at",
                )
            )
            > cutoff
        ):
            raise ValueError("quote request universe, units or observation changed")
        if row["source_timestamp_provenance"] != "provider_source_timestamp":
            continue
        values = {
            name: None if pd.isna(row[name]) else row[name]
            for name in IntradayQuoteRow.model_fields
        }
        values["volume"] = float(row["volume"]) * (100 if row["volume_unit"] == "lot100" else 1)
        rows.append(IntradayQuoteRow.model_validate(values))
    if not rows or spool.source_descriptor(LiveChannel.WATCHLIST_QUOTE) != descriptor:
        raise ValueError("quote source timestamp or prefix is unavailable")
    return IntradayQuoteSnapshot(
        generation_id=descriptor.generation_id,
        envelope=envelope,
        schema_evidence=schema,
        rows=tuple(rows),
        source_descriptor=descriptor,
    )


def _derive_reference_fields(
    fields: dict[str, IntradayFieldFact],
    reference: IntradayReferenceObservation | None,
    *,
    exact_event_time: datetime | None = None,
) -> None:
    def derived(name: str, value: float | None, dependency: str) -> None:
        original = fields[dependency]
        if (
            value is None
            or reference is None
            or original.status is not FeatureAvailability.AVAILABLE
        ):
            return
        fields[name] = IntradayFieldFact(
            name=name,
            value=value,
            status=FeatureAvailability.AVAILABLE,
            source_event_time=original.source_event_time,
            available_at=max(original.available_at, reference.available_at),
            source_id=canonical_sha256((original.source_id, reference.source_id)),
        )

    if reference is None:
        return
    price, volume = fields["INTRADAY_PRICE[0]"].value, fields["INTRADAY_VOLUME[0]"].value
    if fields["INTRADAY_PCT_CHG[0]"].status is not FeatureAvailability.AVAILABLE:
        derived(
            "INTRADAY_PCT_CHG[0]",
            100 * (price / reference.pre_close - 1)
            if price is not None and reference.pre_close
            else None,
            "INTRADAY_PRICE[0]",
        )
    derived(
        "INTRADAY_TURNOVER_RATE[0]",
        reference.turnover_rate
        if reference.turnover_rate is not None
        and (exact_event_time is None or reference.source_event_time == exact_event_time)
        else 100 * volume / reference.float_shares
        if volume is not None and reference.float_shares
        else None,
        "INTRADAY_VOLUME[0]",
    )
    derived(
        "INTRADAY_LIMIT_DISTANCE[0]",
        100 * (reference.up_limit - price) / reference.up_limit
        if price is not None and reference.up_limit and reference.no_price_limit is False
        else None,
        "INTRADAY_PRICE[0]",
    )


class IntradayScreenProjectionSource:
    def __init__(self, config: IntradaySourceConfig) -> None:
        self.config = IntradaySourceConfig.model_validate(config)

    def __call__(self, observed_at: datetime, /) -> IntradayScreenSnapshot:
        config = self.config
        cutoff = normalize_aware_utc(observed_at)
        schema = read_intraday_schema_evidence(config.schema_gate, cutoff=cutoff)
        reference = read_intraday_reference(
            config.reference_snapshot_path,
            expected_sha256=config.reference_snapshot_sha256,
            cutoff=cutoff,
        )
        if reference.trade_date != cutoff.astimezone(_SHANGHAI).date():
            raise ValueError("intraday reference date differs from cutoff")
        quote_snapshot = (
            None
            if config.quote_source is None
            else read_intraday_quotes(
                config.quote_source,
                cursor_root=config.cursor_root / "quotes",
                reference=reference,
                cutoff=cutoff,
            )
        )
        quotes_by_code = (
            {} if quote_snapshot is None else {row.ts_code: row for row in quote_snapshot.rows}
        )
        anchors = [day for day in reference.open_dates if day < reference.trade_date]
        if not anchors:
            raise ValueError("intraday previous closed session is unavailable")
        historical = _read_history_digest(config.historical_snapshot_path)
        if historical.sha256 != config.historical_snapshot_id:
            raise ValueError("intraday historical input changed")
        definitions = ImmutableDefinitionRegistry(
            config.definition_root,
            execution_registry=BuiltinStrategyEvaluatorRegistry(
                producer_commit=config.feature_producer_commit
            ).trusted_executable_registry(),
        )
        definition = definitions.read_feature_contract(
            config.feature_definition_fingerprint, as_of=cutoff
        )
        if definition is None:
            raise ValueError("intraday feature definition is not installed")
        raw = LiveBatchSpool(
            config.raw_spool_root, cursor_root=config.cursor_root / "raw", source_read_only=True
        )
        features = FeatureBatchSpool(
            config.feature_spool_root, cursor_root=config.cursor_root / "features", read_only=True
        )
        raw_descriptor = raw.source_descriptor(LiveChannel.MARKET_MINUTE)
        feature_descriptor = features.source_descriptor()
        if (
            raw_descriptor.generation_id != config.expected_raw_generation_id
            or feature_descriptor.generation_id != config.expected_feature_generation_id
            or not 0 <= raw_descriptor.high_watermark < _MAX_PREFIX
            or not 0 <= feature_descriptor.high_watermark < _MAX_PREFIX
        ):
            raise ValueError("intraday source generation or prefix changed")
        feature_records = features.list_after(
            sequence=-1, through_sequence=feature_descriptor.high_watermark
        )
        visible = [item for item in feature_records if item.envelope.available_at <= cutoff]
        if not visible:
            raise ValueError("intraday feature source is unavailable")
        record = visible[-1]
        envelope = record.envelope
        if (
            envelope.contract_id != "intraday-pit"
            or envelope.contract_version not in {3, 4}
            or envelope.producer_commit != config.feature_producer_commit
            or envelope.contract_id != definition.contract.contract_id
            or envelope.contract_version != definition.contract.version
            or max(envelope.event_time, envelope.decision_cutoff) > cutoff
            or envelope.row_count > 8_000
            or len(envelope.input_batch_ids) > _MAX_INPUT_BATCHES + 1
            or envelope.event_time.astimezone(_SHANGHAI).date() != reference.trade_date
        ):
            raise ValueError("intraday feature contract or time changed")
        payload = features.read_payload(record)
        if len(payload) > 16 * 1024 * 1024:
            raise ValueError("intraday feature payload exceeds bound")
        decoded = json.loads(payload)
        expected_columns = set(feature_columns_for_version(envelope.contract_version))
        rows = decoded["rows"]
        if len(rows) != envelope.row_count or any(set(row) != expected_columns for row in rows):
            raise ValueError("intraday feature fields changed")
        by_code = {str(row["ts_code"]): row for row in rows}
        if len(by_code) != len(rows) or set(by_code) - set(reference.universe_codes):
            raise ValueError("intraday feature members exceed their universe")
        raw_records = raw.list_after(LiveChannel.MARKET_MINUTE, sequence=-1)
        inputs = set(envelope.input_batch_ids) - {config.historical_snapshot_id}
        selected = [item for item in raw_records if item.envelope.batch_id in inputs]
        if (
            config.historical_snapshot_id not in envelope.input_batch_ids
            or not selected
            or {item.envelope.batch_id for item in selected} != inputs
        ):
            raise ValueError("intraday feature raw input prefix is incomplete")
        raw_receipts = tuple(
            IntradayRawInputReceipt(
                source_generation_id=raw_descriptor.generation_id, envelope=item.envelope
            )
            for item in selected
        )
        raw_codes: set[str] = set()
        byte_count = 0
        fact_count = 0
        for item in selected:
            source = item.envelope
            if (
                source.producer_commit != config.raw_producer_commit
                or source.quality_status
                not in {BatchQualityStatus.PUBLISHED, BatchQualityStatus.DEGRADED}
                or max(
                    source.event_time_end,
                    source.source_time,
                    source.received_at,
                    source.available_at,
                )
                > envelope.available_at
            ):
                raise ValueError("intraday raw input is not visible or published")
            fact_count += source.row_count
            if fact_count > 1_000_000:
                raise ValueError("intraday raw fact budget exceeded")
            raw_payload = raw.read_payload(item)
            byte_count += len(raw_payload)
            if byte_count > _MAX_INPUT_BYTES:
                raise ValueError("intraday raw byte budget exceeded")
            frame = MarketMinuteGateway.decode_payload(raw_payload)
            dates = frame.trade_time.dt.tz_convert(_SHANGHAI).dt.date
            raw_codes.update(
                str(code) for code in frame.loc[dates == reference.trade_date, "ts_code"]
            )
        if set(by_code) - raw_codes or not by_code:
            raise ValueError("intraday feature candidate raw coverage is unavailable")
        reference_by_code = {item.ts_code: item for item in reference.observations}
        stocks: list[IntradayStockSnapshot] = []
        market: list[IntradayMarketRow] = []
        multiplier = 100 if config.minute_volume_unit == "lot100" else 1
        for code in reference.universe_codes:
            row = by_code.get(code)
            quote = quotes_by_code.get(code)
            observed = visible_intraday_reference(
                reference_by_code.get(code), trade_date=reference.trade_date, cutoff=cutoff
            )
            facts: dict[str, IntradayFieldFact] = {}
            for name in INTRADAY_FIELD_LABELS:
                original_name = _FEATURE_FIELDS.get(name)
                status = (
                    envelope.field_status(original_name, candidate_id=code)
                    if original_name
                    else None
                )
                value = row.get(original_name) if row and original_name else None
                if value is not None and name == "INTRADAY_VOLUME[0]":
                    value = float(value) * multiplier
                if row is None or status is None or value is None:
                    facts[name] = IntradayFieldFact(
                        name=name,
                        status=FeatureAvailability.UNAVAILABLE,
                        reason="missing_source_field",
                    )
                    continue
                availability = status.status
                reason = status.reason
                if (
                    cutoff - status.source_event_time
                ).total_seconds() > MARKET_MINUTE_FEATURE_MAX_DELAY_SECONDS:
                    availability, reason = FeatureAvailability.STALE, "source_event_late"
                facts[name] = IntradayFieldFact(
                    name=name,
                    value=value,
                    status=availability,
                    source_event_time=status.source_event_time,
                    available_at=status.available_at,
                    source_id=envelope.content_hash,
                    reason=reason,
                    original_feature_status=status,
                )
            feature_time = pd.Timestamp(row["feature_time"]).to_pydatetime() if row else None
            closed_bar = None
            if feature_time is not None and feature_time.second == 0 and feature_time.microsecond == 0:
                closed_fields = dict(facts)
                closed_reference = visible_intraday_reference(reference_by_code.get(code), trade_date=reference.trade_date, cutoff=feature_time)
                _derive_reference_fields(
                    closed_fields, closed_reference, exact_event_time=feature_time
                )
                closed_bar = IntradayClosedBarSnapshot(bar_end=feature_time, fields=tuple(closed_fields[name] for name in sorted(closed_fields)))
            if quote is not None and quote_snapshot is not None:
                availability = (
                    FeatureAvailability.AVAILABLE
                    if (cutoff - quote.observed_at).total_seconds()
                    <= MARKET_MINUTE_FEATURE_MAX_DELAY_SECONDS
                    else FeatureAvailability.STALE
                )
                for name, column in (
                    ("INTRADAY_PRICE[0]", "price"),
                    ("INTRADAY_OPEN[0]", "open"),
                    ("INTRADAY_HIGH[0]", "high"),
                    ("INTRADAY_LOW[0]", "low"),
                    ("INTRADAY_VOLUME[0]", "volume"),
                    ("INTRADAY_AMOUNT[0]", "amount"),
                    ("INTRADAY_PCT_CHG[0]", "pct_chg"),
                ):
                    quote_value = getattr(quote, column)
                    if quote_value is not None:
                        facts[name] = IntradayFieldFact(
                            name=name,
                            value=quote_value,
                            status=availability,
                            source_event_time=quote.observed_at,
                            available_at=quote_snapshot.envelope.available_at,
                            source_id=quote_snapshot.envelope.content_sha256,
                            reason=None
                            if availability is FeatureAvailability.AVAILABLE
                            else "source_event_late",
                        )
                observed = IntradayReferenceObservation(
                    ts_code=code,
                    trade_date=reference.trade_date,
                    source_id=canonical_sha256(
                        (
                            None if observed is None else observed.source_id,
                            quote_snapshot.envelope.content_sha256,
                        )
                    ),
                    source_event_time=max(quote.observed_at, observed.source_event_time)
                    if observed
                    else quote.observed_at,
                    available_at=max(quote_snapshot.envelope.available_at, observed.available_at)
                    if observed
                    else quote_snapshot.envelope.available_at,
                    pre_close=quote.pre_close
                    if quote.pre_close is not None
                    else observed.pre_close
                    if observed
                    else None,
                    up_limit=quote.up_limit
                    if quote.up_limit is not None
                    else observed.up_limit
                    if observed
                    else None,
                    no_price_limit=quote.no_price_limit
                    if quote.no_price_limit is not None
                    else observed.no_price_limit
                    if observed
                    else None,
                    float_shares=quote.float_shares
                    if quote.float_shares is not None
                    else observed.float_shares
                    if observed
                    else None,
                    turnover_rate=quote.turnover_rate
                    if quote.turnover_rate is not None
                    else observed.turnover_rate
                    if observed
                    else None,
                )

            _derive_reference_fields(facts, observed)
            stock = IntradayStockSnapshot(
                ts_code=code,
                feature_time=feature_time,
                fields=tuple(facts[name] for name in sorted(facts)),
                closed_bar=closed_bar,
            )
            stocks.append(stock)
            market_time = quote.observed_at if quote else feature_time
            if market_time is not None:

                def known(name: str, fields: dict[str, IntradayFieldFact] = facts) -> float | None:
                    fact = fields[name]
                    return fact.value if fact.status is FeatureAvailability.AVAILABLE else None

                market.append(
                    IntradayMarketRow(
                        as_of=market_time,
                        ts_code=code,
                        price=known("INTRADAY_PRICE[0]"),
                        open=known("INTRADAY_OPEN[0]"),
                        high=known("INTRADAY_HIGH[0]"),
                        low=known("INTRADAY_LOW[0]"),
                        volume=known("INTRADAY_VOLUME[0]"),
                        amount=known("INTRADAY_AMOUNT[0]"),
                        pre_close=observed.pre_close if observed else None,
                        pct_chg=known("INTRADAY_PCT_CHG[0]"),
                    )
                )
        missing = tuple(
            code
            for code in reference.universe_codes
            if code not in by_code and code not in quotes_by_code
        )
        source = IntradayScreenSource(
            trade_date=reference.trade_date,
            cutoff=cutoff,
            published_at=cutoff,
            daily_anchor_date=max(anchors),
            reference_identity=reference.identity,
            reference_file_sha256=config.reference_snapshot_sha256,
            universe_source_id=reference.universe_source_id,
            universe_codes=reference.universe_codes,
            universe_digest=canonical_sha256(reference.universe_codes),
            feature_generation_id=feature_descriptor.generation_id,
            feature_sequence=envelope.sequence,
            feature_batch_id=envelope.batch_id,
            feature_payload_sha256=envelope.content_hash,
            feature_contract_version=envelope.contract_version,
            feature_contract_fingerprint=definition.contract.contract_fingerprint,
            feature_definition_fingerprint=definition.fingerprint,
            feature_producer_commit=envelope.producer_commit,
            raw_input_receipts=raw_receipts,
            raw_prefix_digest=canonical_sha256(raw_receipts),
            historical_snapshot_id=config.historical_snapshot_id,
            quote_kind="minute_snapshot" if quote_snapshot is None else "quote_snapshot",
            quote_generation_id=None if quote_snapshot is None else quote_snapshot.generation_id,
            quote_envelope=None if quote_snapshot is None else quote_snapshot.envelope,
            quote_schema_evidence=None
            if quote_snapshot is None
            else quote_snapshot.schema_evidence,
            quote_units_contract_id=None
            if config.quote_source is None
            else config.quote_source.units_contract_id,
            units_contract_id=config.units_contract_id,
            missing_codes=missing,
            coverage_digest=canonical_sha256(
                {"universe": reference.universe_codes, "missing": missing}
            ),
            market_digest=canonical_sha256(tuple(market)),
            stock_digest=canonical_sha256(tuple(stocks)),
            schema_evidence=schema,
        )
        if (
            raw.source_descriptor(LiveChannel.MARKET_MINUTE) != raw_descriptor
            or features.source_descriptor() != feature_descriptor
        ):
            raise ValueError("intraday source changed during publication")
        if quote_snapshot is not None and config.quote_source is not None:
            quote_reader = LiveBatchSpool(
                config.quote_source.spool_root,
                cursor_root=config.cursor_root / "quotes",
                source_read_only=True,
            )
            if (
                quote_reader.source_descriptor(LiveChannel.WATCHLIST_QUOTE)
                != quote_snapshot.source_descriptor
            ):
                raise ValueError("intraday quote source changed during publication")
        history_identity = config.historical_snapshot_path.stat(follow_symlinks=False)
        if _file_identity(history_identity) != (
            historical.device,
            historical.inode,
            historical.size,
            historical.modified_ns,
            historical.changed_ns,
        ):
            raise ValueError("historical input changed during publication")
        return IntradayScreenSnapshot(
            source=source, stocks=tuple(stocks), market_rows=tuple(market)
        )


def intraday_projections(snapshot: IntradayScreenSnapshot) -> tuple[ServingProjectionPayload, ...]:
    cutoff = snapshot.source.cutoff
    return (
        ServingProjectionPayload(
            table_name="intraday_screen_source",
            available_at=cutoff,
            rows=(
                {
                    "source_identity": snapshot.source.source_identity,
                    "trade_date": snapshot.source.trade_date.isoformat(),
                    "cutoff": cutoff.isoformat(),
                    "payload_json": encode_intraday_source_wire(snapshot.source),
                },
            ),
        ),
        ServingProjectionPayload(
            table_name="intraday_feature_snapshot",
            available_at=cutoff,
            rows=tuple(
                row.model_dump(mode="json")
                for row in encode_intraday_stock_rows(snapshot.source.source_identity, snapshot.stocks)
            ),
        ),
        ServingProjectionPayload(
            table_name="market_snapshot",
            available_at=cutoff,
            rows=tuple(row.model_dump(mode="json") for row in snapshot.market_rows),
        ),
    )
