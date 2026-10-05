"""Bounded, same-generation rule scope and published quote request evidence."""

from __future__ import annotations

import math
import os
from collections import Counter
from datetime import date, datetime, timedelta
from decimal import Decimal
from hashlib import sha256
from io import BytesIO
from pathlib import Path
from typing import Literal, Self

from pydantic import Field, StrictInt, StrictStr, field_serializer, model_validator

from rquant.alert_price_rule import PriceAlertRule
from rquant.live_contracts import BatchEnvelope, BatchQualityStatus, CurrentPointer, LiveChannel
from rquant.live_spool import LiveBatchSpool, _secure_read_regular_file
from rquant.price_alert_runtime_contracts import (
    PriceRuntimeModel,
    PriceSha256,
    PriceTimestampProvenance,
    decimal_text,
    utc_text,
)
from rquant.runtime_contracts import AwareUtcDatetime, canonical_sha256, normalize_aware_utc
from rquant.serving_manual_watchlist_projection import (
    ManualWatchlistAuthoritySnapshot,
    ManualWatchlistProjectionRow,
    validate_manual_watchlist_projections,
)
from rquant.serving_price_alert_rule_projection import (
    PriceAlertRuleAuthoritySnapshot,
    PriceAlertRuleProjectionRow,
    validate_price_alert_rule_projections,
)
from rquant.serving_publisher import ServingGenerationLease
from rquant.serving_read_models import PAGE_PROJECTION_CONTRACTS, ServingProjectionInput
from rquant.watchlist_quote_gateway import decode_watchlist_quote_payload

_SCOPE_TABLES = (
    "manual_watchlist",
    "manual_watchlist_state",
    "price_alert_rule",
    "price_alert_rule_state",
)


class PriceAlertScopeSnapshot(PriceRuntimeModel):
    availability: Literal["ready"] = "ready"
    generation_id: PriceSha256
    manifest_sha256: PriceSha256
    source_generation_id: PriceSha256
    source_sequence: StrictInt = Field(ge=0)
    built_at: AwareUtcDatetime
    available_at: AwareUtcDatetime
    inspected_at: AwareUtcDatetime
    rule_rows_sha256: PriceSha256
    member_rows_sha256: PriceSha256
    rules: tuple[PriceAlertRuleProjectionRow, ...]
    members: tuple[ManualWatchlistProjectionRow, ...]

    @model_validator(mode="after")
    def bounded_scope(self) -> Self:
        if not (
            self.available_at
            <= self.built_at
            <= self.inspected_at
            <= self.built_at + timedelta(seconds=30)
        ):
            raise ValueError("price scope generation is not current")
        if any(type(item) is not PriceAlertRuleProjectionRow for item in self.rules) or any(
            type(item) is not ManualWatchlistProjectionRow for item in self.members
        ):
            raise TypeError("price scope rows must use the exact original projection types")
        owners = {item.owner_id for item in (*self.rules, *self.members)}
        if len(owners) > 32:
            raise ValueError("price scope exceeds 32 owners")
        identities = tuple((row.owner_id, row.rule_id) for row in self.rules)
        member_ids = tuple((row.owner_id, row.ts_code) for row in self.members)
        if identities != tuple(sorted(set(identities))) or member_ids != tuple(
            sorted(set(member_ids))
        ):
            raise ValueError("price scope rows are not sorted and unique")
        if (
            PriceAlertRuleAuthoritySnapshot.digest(self.rules) != self.rule_rows_sha256
            or ManualWatchlistAuthoritySnapshot.digest(self.members) != self.member_rows_sha256
            or any(
                row.updated_at is not None and row.updated_at > self.available_at
                for row in (*self.rules, *self.members)
            )
        ):
            raise ValueError("price scope row hashes or visibility differ")
        effective = self.effective_rules
        if len(effective) > 1000 or any(
            value > 100
            for value in Counter(row.owner_id for row in self.rules if not row.deleted).values()
        ):
            raise ValueError("price scope exceeds rule capacity")
        if len(self.codes) > 500:
            raise ValueError("price scope exceeds 500 distinct quote codes")
        if len(self.wire_bytes()) > 1024 * 1024:
            raise ValueError("price scope exceeds the complete 1 MiB domain")
        return self

    @property
    def effective_rules(self) -> tuple[PriceAlertRuleProjectionRow, ...]:
        members = {(row.owner_id, row.ts_code): row for row in self.members}
        return tuple(
            row
            for row in self.rules
            if not row.deleted
            and row.enabled
            and (row.owner_id, row.ts_code) in members
            and not members[row.owner_id, row.ts_code].deleted
            and row.membership_version == members[row.owner_id, row.ts_code].version
            and (
                members[row.owner_id, row.ts_code].expires_at is None
                or members[row.owner_id, row.ts_code].expires_at > self.inspected_at
            )
        )

    @property
    def codes(self) -> tuple[str, ...]:
        return tuple(sorted({row.ts_code for row in self.effective_rules}))


class UnavailablePriceAlertScope(PriceRuntimeModel):
    availability: Literal["unavailable"] = "unavailable"
    reason: Literal["scope_unavailable", "scope_expired", "capacity_exceeded"] = "scope_unavailable"
    inspected_at: AwareUtcDatetime


def read_price_alert_scope(
    lease: ServingGenerationLease | None,
    *,
    evaluated_at: datetime,
) -> PriceAlertScopeSnapshot | UnavailablePriceAlertScope:
    now = normalize_aware_utc(evaluated_at)
    if lease is None or lease.closed or lease.pointer is None:
        return UnavailablePriceAlertScope(inspected_at=now)
    manifest = lease.manifest
    if (
        lease.pointer.generation_id != manifest.generation_id
        or manifest.built_at > now
        or now - manifest.built_at > timedelta(seconds=30)
    ):
        return UnavailablePriceAlertScope(reason="scope_expired", inspected_at=now)
    source = manifest.source_generations.get("signals")
    watermark = next((mark for mark in manifest.watermarks if mark.dataset_id == "signals"), None)
    if source is None or watermark is None or watermark.generation_id != source:
        return UnavailablePriceAlertScope(inspected_at=now)
    cursor = lease.connection.cursor()
    try:
        marks = cursor.execute(
            "SELECT table_name,available,row_count,owner_dataset_id, "
            "owner_generation_id,available_at FROM projection_status WHERE table_name IN (?,?,?,?) "
            "ORDER BY table_name LIMIT 5",
            _SCOPE_TABLES,
        ).fetchall()
        if tuple(row[0] for row in marks) != _SCOPE_TABLES:
            raise ValueError("price scope projection status is incomplete")
        projections = {}
        for name, available, count, owner, generation, at in marks:
            contract = PAGE_PROJECTION_CONTRACTS[name]
            if (
                available is not True
                or type(count) is not int
                or owner != "signals"
                or generation != source
                or count != manifest.row_counts.get(name)
                or not 0 <= count <= contract.max_rows
                or not isinstance(at, datetime)
                or normalize_aware_utc(at) > manifest.built_at
            ):
                raise ValueError("price scope projection identity is invalid")
            rows = cursor.execute(
                f"SELECT {', '.join(contract.column_names)} FROM {name} "
                f"ORDER BY {', '.join(contract.sort_keys)} LIMIT ?",
                [contract.max_rows + 1],
            ).fetchall()
            if len(rows) != count:
                raise ValueError("price scope row count differs from manifest")
            projections[name] = ServingProjectionInput(
                table_name=name,
                available_at=at,
                owner_dataset_id="signals",
                owner_generation_id=source,
                rows=tuple(
                    dict(
                        zip(
                            contract.column_names,
                            (
                                normalize_aware_utc(value).isoformat()
                                if isinstance(value, datetime)
                                else value
                                for value in row
                            ),
                            strict=True,
                        )
                    )
                    for row in rows
                ),
            )
        validate_price_alert_rule_projections(projections)
        validate_manual_watchlist_projections(projections)
        rule_state = projections["price_alert_rule_state"].rows[0]
        member_state = projections["manual_watchlist_state"].rows[0]
        if rule_state["state"] != "ready" or member_state["state"] != "ready":
            raise ValueError("price scope is not activated")
        return PriceAlertScopeSnapshot(
            generation_id=manifest.generation_id,
            manifest_sha256=lease.pointer.manifest_sha256,
            source_generation_id=source,
            source_sequence=watermark.sequence,
            built_at=manifest.built_at,
            available_at=max(item.available_at for item in projections.values()),
            inspected_at=now,
            rule_rows_sha256=rule_state["rows_sha256"],
            member_rows_sha256=member_state["rows_sha256"],
            rules=tuple(
                PriceAlertRuleProjectionRow.model_validate(row)
                for row in projections["price_alert_rule"].rows
            ),
            members=tuple(
                ManualWatchlistProjectionRow.model_validate(row)
                for row in projections["manual_watchlist"].rows
            ),
        )
    except (KeyError, TypeError, ValueError) as exc:
        reason = "capacity_exceeded" if "exceed" in str(exc) else "scope_unavailable"
        return UnavailablePriceAlertScope(reason=reason, inspected_at=now)
    finally:
        cursor.close()


def original_price_rule(row: PriceAlertRuleProjectionRow) -> PriceAlertRule:
    from datetime import time

    if row.deleted:
        raise ValueError("deleted rule has no executable body")
    return PriceAlertRule(
        rule_id=row.rule_id,
        name=row.name,
        priority=row.priority,
        enabled=row.enabled,
        comparison=row.comparison,
        threshold=Decimal(row.threshold),
        valid_from=time.fromisoformat(row.valid_from),
        valid_until=time.fromisoformat(row.valid_until),
    )


class PriceQuoteRequestBinding(PriceRuntimeModel):
    binding_schema: Literal["price-alert-quote-request/v1"] = "price-alert-quote-request/v1"
    request_id: PriceSha256
    source: StrictStr = Field(min_length=1, max_length=128)
    quote_source_generation_id: PriceSha256
    scope_generation_id: PriceSha256
    scope_manifest_sha256: PriceSha256
    codes: tuple[StrictStr, ...] = Field(max_length=500)
    scheduled_at: AwareUtcDatetime
    universe_as_of: AwareUtcDatetime
    trade_date: date
    schema_version: StrictInt = Field(ge=2)

    @property
    def expected_request_id(self) -> str:
        return canonical_sha256(
            {
                name: getattr(self, name)
                for name in (
                    "source",
                    "codes",
                    "scheduled_at",
                    "universe_as_of",
                    "trade_date",
                    "schema_version",
                )
            }
        )

    @model_validator(mode="after")
    def valid_request(self) -> Self:
        from pydantic import TypeAdapter

        from rquant.manual_watchlist import TsCode

        if self.codes != tuple(sorted(set(self.codes))):
            raise ValueError("price quote request codes must be sorted and unique")
        for code in self.codes:
            TypeAdapter(TsCode).validate_python(code, strict=True)
        if self.universe_as_of > self.scheduled_at or self.request_id != self.expected_request_id:
            raise ValueError("price request binding differs from the actual gateway preimage")
        return self

    @field_serializer("scheduled_at", "universe_as_of")
    def time_text(self, value: datetime) -> str:
        return utc_text(value)

    @classmethod
    def create(cls, **facts: object) -> PriceQuoteRequestBinding:
        facts = dict(facts)
        if "request_id" in facts:
            raise ValueError("request identity is computed from facts")
        facts["request_id"] = canonical_sha256(
            {
                name: facts[name]
                for name in (
                    "source",
                    "codes",
                    "scheduled_at",
                    "universe_as_of",
                    "trade_date",
                    "schema_version",
                )
            }
        )
        return cls.model_validate(facts)


def freeze_price_quote_request(root: Path, binding: PriceQuoteRequestBinding) -> Path:
    from rquant.price_alert_runtime_contracts import _activation_bytes

    if type(binding) is not PriceQuoteRequestBinding:
        raise TypeError("quote binding must have the exact request type")
    binding = PriceQuoteRequestBinding.model_validate(binding)
    path = root / f"{binding.request_id}.json"
    from rquant.price_alert_runtime_store import _private_parent

    _private_parent(path)
    payload = binding.wire_bytes()
    try:
        descriptor = os.open(
            path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0), 0o600
        )
    except FileExistsError:
        if _activation_bytes(path, root) != payload:
            raise ValueError("actual request identity already binds another scope") from None
        return path
    try:
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        directory = os.open(
            root, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)
        )
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
        if _activation_bytes(path, root) != payload:
            raise ValueError("quote request binding changed while sealing")
        return path
    except BaseException:
        # A failed durability acknowledgement must remain visible for exact recovery.
        raise


class PriceQuoteFact(PriceRuntimeModel):
    ts_code: StrictStr
    price: StrictStr
    observed_at: AwareUtcDatetime
    trade_date: date
    source_timestamp_provenance: PriceTimestampProvenance

    @model_validator(mode="after")
    def canonical_price(self) -> Self:
        if decimal_text(self.price) != self.price:
            raise ValueError("quote price must be canonical decimal text")
        return self


class PriceQuoteSnapshot(PriceRuntimeModel):
    scope_generation_id: PriceSha256
    scope_manifest_sha256: PriceSha256
    source_generation_id: PriceSha256
    batch_id: PriceSha256
    sequence: StrictInt = Field(ge=0)
    revision: StrictInt = Field(ge=1)
    payload_sha256: PriceSha256
    request_binding_sha256: PriceSha256
    available_at: AwareUtcDatetime
    inspected_at: AwareUtcDatetime
    requested_codes: tuple[StrictStr, ...]
    quotes: tuple[PriceQuoteFact, ...]


def read_latest_price_quote_snapshot(
    spool: LiveBatchSpool,
    *,
    request_root: Path,
    binding: PriceQuoteRequestBinding,
    evaluated_at: datetime,
    expected_producer_commit: str,
) -> PriceQuoteSnapshot:
    from zoneinfo import ZoneInfo

    from pyarrow.parquet import ParquetFile

    from rquant.price_alert_runtime_contracts import _activation_bytes
    from rquant.watchlist_quote_gateway import _COLUMNS

    if type(binding) is not PriceQuoteRequestBinding:
        raise TypeError("price quote binding must be exact")
    binding = PriceQuoteRequestBinding.model_validate(binding)
    request_payload = _activation_bytes(request_root / f"{binding.request_id}.json", request_root)
    if request_payload != binding.wire_bytes():
        raise ValueError("price quotes lack the original pre-call request binding")
    channel = LiveChannel.WATCHLIST_QUOTE
    pointer_path = spool._current_path(channel)
    before = _secure_read_regular_file(
        pointer_path, label="price quote pointer", max_bytes=64 * 1024
    )
    pointer = CurrentPointer.model_validate_json(before)
    if (
        pointer.channel is not channel
        or pointer.source_generation_id != binding.quote_source_generation_id
        or spool._source_generation(channel) != pointer.source_generation_id
    ):
        raise ValueError("price quote source generation differs from the actual request")
    manifest_payload = _secure_read_regular_file(
        spool._manifest_path(channel, pointer.sequence),
        label="price quote batch envelope",
        max_bytes=64 * 1024,
    )
    envelope = BatchEnvelope.model_validate_json(manifest_payload)
    if (
        envelope.channel is not channel
        or envelope.quality_status is not BatchQualityStatus.PUBLISHED
        or envelope.source != binding.source
        or envelope.source_request_id != binding.request_id
        or envelope.producer_commit != expected_producer_commit
        or envelope.schema_version != binding.schema_version
        or envelope.dataset_id != "watchlist_quote"
        or envelope.row_count > 500
        or (
            envelope.batch_id,
            envelope.sequence,
            envelope.revision,
            envelope.content_sha256,
            envelope.available_at,
        )
        != (
            pointer.batch_id,
            pointer.sequence,
            pointer.revision,
            pointer.content_sha256,
            pointer.published_at,
        )
    ):
        raise ValueError("price quote envelope does not match the authoritative original request")
    payload = _secure_read_regular_file(
        spool._payload_path(channel, pointer.sequence),
        label="price quote payload",
        max_bytes=4 * 1024 * 1024,
    )
    if sha256(payload).hexdigest() != envelope.content_sha256:
        raise ValueError("price quote payload hash differs")
    parquet = ParquetFile(BytesIO(payload))
    if (
        parquet.metadata.num_rows != envelope.row_count
        or parquet.metadata.num_columns != len(_COLUMNS)
        or tuple(parquet.schema_arrow.names) != _COLUMNS
        or sum(
            parquet.metadata.row_group(i).total_byte_size
            for i in range(parquet.metadata.num_row_groups)
        )
        > 4 * 1024 * 1024
    ):
        raise ValueError("price quote payload exceeds the decoded domain budget")
    frame = decode_watchlist_quote_payload(payload)
    now = normalize_aware_utc(evaluated_at)
    if envelope.available_at > now or now - envelope.available_at > timedelta(seconds=15):
        raise ValueError("price quote batch is future or stale")
    if len(frame) != envelope.row_count or frame["ts_code"].duplicated().any():
        raise ValueError("price quote rows or codes are inconsistent")
    quotes = []
    for row in frame.itertuples(index=False):
        observed = row.observed_at.to_pydatetime()
        response_at = row.response_received_at.to_pydatetime()
        requested_at = row.requested_at.to_pydatetime()
        provenance = str(row.source_timestamp_provenance)
        if (
            str(row.ts_code) not in binding.codes
            or str(row.source) != binding.source
            or str(row.producer_commit) != expected_producer_commit
            or int(row.schema_version) != binding.schema_version
            or row.scheduled_at.to_pydatetime() != binding.scheduled_at
            or row.universe_as_of.to_pydatetime() != binding.universe_as_of
            or not binding.scheduled_at <= requested_at <= response_at <= envelope.available_at
            or row.fetched_at.to_pydatetime() != response_at
            or not observed <= response_at
            or not 0 <= (now - observed).total_seconds() <= 15
            or observed.astimezone(ZoneInfo("Asia/Shanghai")).date() != binding.trade_date
            or str(row.trade_date) != binding.trade_date.isoformat()
            or provenance not in ("provider_source_timestamp", "response_received_at_fallback")
            or (provenance == "response_received_at_fallback" and observed != response_at)
            or not math.isfinite(row.price)
        ):
            raise ValueError("price quote observation facts are unavailable or inconsistent")
        quotes.append(
            PriceQuoteFact(
                ts_code=str(row.ts_code),
                price=decimal_text(Decimal(str(row.price))),
                observed_at=observed,
                trade_date=binding.trade_date,
                source_timestamp_provenance=provenance,
            )
        )
    if (
        _secure_read_regular_file(pointer_path, label="price quote pointer", max_bytes=64 * 1024)
        != before
    ):
        raise ValueError("price quote current pointer changed during inspection")
    if spool._source_generation(channel) != pointer.source_generation_id:
        raise ValueError("price quote source changed during inspection")
    return PriceQuoteSnapshot(
        scope_generation_id=binding.scope_generation_id,
        scope_manifest_sha256=binding.scope_manifest_sha256,
        source_generation_id=pointer.source_generation_id,
        batch_id=envelope.batch_id,
        sequence=envelope.sequence,
        revision=envelope.revision,
        payload_sha256=envelope.content_sha256,
        request_binding_sha256=sha256(request_payload).hexdigest(),
        available_at=envelope.available_at,
        inspected_at=now,
        requested_codes=binding.codes,
        quotes=tuple(sorted(quotes, key=lambda item: item.ts_code)),
    )
