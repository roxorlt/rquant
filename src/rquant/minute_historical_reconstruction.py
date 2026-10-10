"""Pure preparation of modeled historical inputs; never a trusted source publication."""

from __future__ import annotations

import json
import re
from collections import defaultdict
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from typing import Literal, Self
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, ValidationError, model_validator

from rquant.minute_backtest_publication_contracts import (
    MAX_INPUT_BYTES,
    MAX_WORK_UNITS,
    MinuteDerivation,
    MinuteOriginMaterial,
    MinuteVisibilityPolicy,
    Sha256,
)
from rquant.runtime_contracts import canonical_sha256

FactKind = Literal[
    "eligibility",
    "risk_warning",
    "suspension",
    "price_limits",
    "quotes",
    "warm_history",
    "prior_reference",
    "prior_state",
]
REQUIRED_FACT_KINDS: tuple[FactKind, ...] = (
    "eligibility",
    "risk_warning",
    "suspension",
    "price_limits",
    "quotes",
    "warm_history",
    "prior_reference",
    "prior_state",
)


class _Model(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class HistoricalOriginFieldReference(_Model):
    field_name: str
    value_json_pointer: str
    column_name_json_pointer: str | None = None

    @model_validator(mode="after")
    def pointer_syntax(self) -> Self:
        _pointer_parts(self.value_json_pointer)
        if self.column_name_json_pointer is not None:
            _pointer_parts(self.column_name_json_pointer)
        return self


class HistoricalOriginReference(_Model):
    object_key: str
    json_pointer: str
    fields: tuple[HistoricalOriginFieldReference, ...] = ()


class HistoricalRowsArchive(_Model):
    origin_object_key: str
    rows_pointer: str = ""
    columns_pointer: str | None = None

    @model_validator(mode="after")
    def pointer_syntax(self) -> Self:
        _pointer_parts(self.rows_pointer)
        if self.columns_pointer is not None:
            _pointer_parts(self.columns_pointer)
            if self.columns_pointer == self.rows_pointer:
                raise ValueError("historical rows and column header pointers must differ")
        return self


class HistoricalCandidateArchive(HistoricalRowsArchive):
    kind: Literal["completed_screen", "intraday", "pool"]
    completed_trade_dates: tuple[date, ...] = ()


class HistoricalTimestampBasis(_Model):
    provider_label: str = Field(min_length=1)
    semantics: Literal["minute_start", "bar_end", "unknown"]
    source_timezone: str | None
    explanation: str = Field(min_length=1)

    @model_validator(mode="after")
    def known_timezone(self) -> Self:
        if self.source_timezone is not None:
            try:
                ZoneInfo(self.source_timezone)
            except ZoneInfoNotFoundError as exc:
                raise ValueError("historical source timezone is unknown") from exc
        return self


class HistoricalSessionWindow(_Model):
    opens_at: time
    closes_at: time

    @model_validator(mode="after")
    def minute_local_bounds(self) -> Self:
        if (
            any(
                t.tzinfo is not None or t.second or t.microsecond
                for t in (self.opens_at, self.closes_at)
            )
            or self.opens_at >= self.closes_at
        ):
            raise ValueError("historical session bounds must be ordered whole local minutes")
        return self


class HistoricalCalendarDay(_Model):
    trade_date: date
    is_open: bool
    windows: tuple[HistoricalSessionWindow, ...]

    @model_validator(mode="after")
    def complete_session_basis(self) -> Self:
        if self.is_open != bool(self.windows):
            raise ValueError(
                "open calendar day requires explicit sessions; closed day forbids them"
            )
        if any(
            a.closes_at >= b.opens_at for a, b in zip(self.windows, self.windows[1:], strict=False)
        ):
            raise ValueError("historical session windows overlap or are unordered")
        return self


class HistoricalReplayDay(_Model):
    trade_date: date
    initialized_at: AwareDatetime


class HistoricalNativeRegistration(_Model):
    strategy_id: str = Field(min_length=1)
    strategy_version: str = Field(min_length=1)
    definition_fingerprint: Sha256
    configuration_origin_key: str
    configuration_sha256: Sha256
    registered_at: AwareDatetime


class HistoricalReconstructionRequest(_Model):
    policy: MinuteVisibilityPolicy
    origin_materials: tuple[MinuteOriginMaterial, ...] = Field(min_length=1)
    minute_archives: tuple[HistoricalRowsArchive, ...] = Field(min_length=1)
    candidate_archives: tuple[HistoricalCandidateArchive, ...]
    fact_archives: tuple[HistoricalRowsArchive, ...]
    timestamp_bases: tuple[HistoricalTimestampBasis, ...]
    calendar_timezone: Literal["Asia/Shanghai"]
    calendar: tuple[HistoricalCalendarDay, ...] = Field(min_length=1)
    replay_days: tuple[HistoricalReplayDay, ...] = Field(min_length=1)
    registration: HistoricalNativeRegistration
    prepared_at: AwareDatetime

    @model_validator(mode="after")
    def explicit_bases(self) -> Self:
        if (self.policy.policy_id, self.policy.version, self.policy.timestamp_semantics) != (
            "retained-minute-research",
            1,
            "bar_end",
        ):
            raise ValueError(
                "historical preparation requires the confirmed minute-end research policy"
            )
        origins = {item.object_key: item for item in self.origin_materials}
        if len(origins) != len(self.origin_materials):
            raise ValueError("historical original object keys repeat")
        if sum(len(item.payload()) for item in self.origin_materials) > MAX_INPUT_BYTES:
            raise ValueError("historical preparation original bytes exceed the input capacity")
        for archives in (self.minute_archives, self.candidate_archives, self.fact_archives):
            keys = [(item.origin_object_key, item.rows_pointer) for item in archives]
            if len(keys) != len(set(keys)) or any(key not in origins for key, _ in keys):
                raise ValueError("historical archive parents are missing or repeated")
        if len({item.provider_label for item in self.timestamp_bases}) != len(self.timestamp_bases):
            raise ValueError("historical provider timestamp interpretations repeat")
        dates = tuple(item.trade_date for item in self.calendar)
        if any(b - a != timedelta(days=1) for a, b in zip(dates, dates[1:], strict=False)):
            raise ValueError(
                "historical calendar must be ordered and contiguous, including closed days"
            )
        replay_dates = tuple(item.trade_date for item in self.replay_days)
        if replay_dates != tuple(sorted(set(replay_dates))):
            raise ValueError("historical replay dates repeat or are unordered")
        calendar = {item.trade_date: item for item in self.calendar}
        zone = ZoneInfo(self.calendar_timezone)
        for item in self.replay_days:
            day = calendar.get(item.trade_date)
            if day is None or not day.is_open:
                raise ValueError("historical replay day lacks an explicit open session")
            bootstrap = item.initialized_at.astimezone(zone)
            if bootstrap.date() != item.trade_date or bootstrap > datetime.combine(
                item.trade_date, day.windows[0].opens_at, zone
            ):
                raise ValueError("historical bootstrap must be on its day before the first session")
        config = origins.get(self.registration.configuration_origin_key)
        if config is None or config.content_sha256 != self.registration.configuration_sha256:
            raise ValueError("historical exact configuration differs from the retained original")
        if self.registration.registered_at > self.prepared_at:
            raise ValueError("historical actual registration cannot be backfilled from the future")
        return self


class HistoricalPreparedMinute(_Model):
    ts_code: str
    trade_date: date
    bar_end: AwareDatetime
    modeled_available_at: AwareDatetime
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal
    amount: Decimal
    origins: tuple[HistoricalOriginReference, ...]


class HistoricalPreparedCandidate(_Model):
    ts_code: str
    preset_name: str
    screen_trade_date: date
    replay_trade_date: date
    modeled_available_at: AwareDatetime
    time_basis: Literal["modeled"] = "modeled"
    origin: HistoricalOriginReference


class HistoricalPreparedFact(_Model):
    kind: FactKind
    ts_code: str
    trade_date: date
    available_at: AwareDatetime
    origins: tuple[HistoricalOriginReference, ...]


class HistoricalNativeDefinitionVisibility(_Model):
    replay_trade_date: date
    modeled_available_at: AwareDatetime
    time_basis: Literal["modeled"] = "modeled"


class HistoricalPreparationGap(_Model):
    code: str
    category: Literal["material", "fact"]
    detail: str
    blocking: bool = True
    trade_date: date | None = None
    ts_code: str | None = None
    fact_kind: FactKind | None = None
    origins: tuple[HistoricalOriginReference, ...] = ()


class HistoricalReconstructionPreparation(_Model):
    policy: MinuteVisibilityPolicy
    origin_materials: tuple[MinuteOriginMaterial, ...]
    timestamp_bases: tuple[HistoricalTimestampBasis, ...]
    registration: HistoricalNativeRegistration
    native_definition_visibility: tuple[HistoricalNativeDefinitionVisibility, ...]
    replay_days: tuple[HistoricalReplayDay, ...]
    prepared_at: AwareDatetime
    minutes: tuple[HistoricalPreparedMinute, ...]
    candidates: tuple[HistoricalPreparedCandidate, ...]
    facts: tuple[HistoricalPreparedFact, ...]
    derivations: tuple[MinuteDerivation, ...]
    unavailable_reasons: tuple[HistoricalPreparationGap, ...]
    materials_ready: bool
    facts_ready: bool
    preparation_status: Literal["complete", "unavailable"]
    source_kind: Literal["reconstructed"] = "reconstructed"
    display_label: Literal["历史重建"] = "历史重建"
    original_units_preserved: Literal[True] = True
    ready_for_original_executor: Literal[False] = False
    formal_authorization: Literal["not_assessed"] = "not_assessed"
    policy_installation: Literal["not_assessed"] = "not_assessed"
    formal_source_published: Literal[False] = False

    @property
    def fingerprint(self) -> str:
        return canonical_sha256(self.model_dump(mode="json"))


class _RawModel(BaseModel):
    model_config = ConfigDict(extra="ignore")


class _MinuteRow(_RawModel):
    ts_code: str = Field(min_length=1)
    trade_time: str
    freq: Literal["1min"]
    source: str
    open: Decimal = Field(allow_inf_nan=False)
    high: Decimal = Field(allow_inf_nan=False)
    low: Decimal = Field(allow_inf_nan=False)
    close: Decimal = Field(allow_inf_nan=False)
    vol: Decimal = Field(allow_inf_nan=False)
    amount: Decimal = Field(allow_inf_nan=False)


class _CandidateRow(_RawModel):
    ts_code: str = Field(min_length=1)
    trade_date: date
    preset_name: str = ""
    published_at: AwareDatetime | None = None
    historical_version: str | None = None


class _FactRow(_RawModel):
    kind: FactKind
    ts_code: str
    trade_date: date
    status: Literal["complete", "incomplete", "conflict"]
    value: object
    available_at: AwareDatetime | None
    reference_trade_date: date | None = None


def _pointer_parts(pointer: str) -> tuple[str, ...]:
    if pointer and (not pointer.startswith("/") or re.search(r"~(?![01])", pointer)):
        raise ValueError("historical row locator must be a valid JSON pointer")
    return (
        tuple(part.replace("~1", "/").replace("~0", "~") for part in pointer[1:].split("/"))
        if pointer
        else ()
    )


def _unique_pairs(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("historical JSON original has ambiguous duplicate object keys")
        result[key] = value
    return result


def _nonfinite_json(value: str) -> object:
    raise ValueError("historical JSON original contains nonfinite numbers: " + value)


def _resolve_json_pointer(value: object, pointer: str) -> object:
    for part in _pointer_parts(pointer):
        if isinstance(value, dict) and part in value:
            value = value[part]
        elif (
            isinstance(value, list)
            and part.isdigit()
            and str(int(part)) == part
            and int(part) < len(value)
        ):
            value = value[int(part)]
        else:
            raise ValueError("historical rows pointer does not address the original archive")
    return value


def _archive_rows(
    archive: HistoricalRowsArchive,
    origins: dict[str, MinuteOriginMaterial],
) -> tuple[tuple[HistoricalOriginReference, object], ...]:
    material = origins[archive.origin_object_key]
    if material.format != "json":
        raise ValueError(
            "historical preparation accepts explicit JSON row archives, not database opens"
        )
    document = json.loads(
        material.payload(),
        parse_float=Decimal,
        parse_constant=_nonfinite_json,
        object_pairs_hook=_unique_pairs,
    )
    rows = _resolve_json_pointer(document, archive.rows_pointer)
    if not isinstance(rows, list) or len(rows) > MAX_WORK_UNITS:
        raise ValueError("historical rows pointer must name a bounded original row array")
    columns = None
    if archive.columns_pointer is not None:
        columns = _resolve_json_pointer(document, archive.columns_pointer)
        if (
            not isinstance(columns, list)
            or not columns
            or any(not isinstance(column, str) or not column for column in columns)
            or len(columns) != len(set(columns))
        ):
            raise ValueError("historical column headers must be unique nonempty original names")
        if any(not isinstance(row, list) or len(row) != len(columns) for row in rows):
            raise ValueError(
                "historical columnar row width or type differs from its original header"
            )
    result = []
    for index, row in enumerate(rows):
        row_pointer = archive.rows_pointer + "/" + str(index)
        if columns is not None:
            projected = dict(zip(columns, row, strict=True))
            fields = tuple(
                HistoricalOriginFieldReference(
                    field_name=name,
                    value_json_pointer=row_pointer + "/" + str(column_index),
                    column_name_json_pointer=archive.columns_pointer + "/" + str(column_index),
                )
                for column_index, name in enumerate(columns)
            )
        else:
            projected = row
            fields = (
                tuple(
                    HistoricalOriginFieldReference(
                        field_name=name,
                        value_json_pointer=row_pointer
                        + "/"
                        + name.replace("~", "~0").replace("/", "~1"),
                    )
                    for name in row
                )
                if isinstance(row, dict)
                else ()
            )
        result.append(
            (
                HistoricalOriginReference(
                    object_key=material.object_key, json_pointer=row_pointer, fields=fields
                ),
                projected,
            )
        )
    return tuple(result)


def _archive_rows_or_unavailable(
    archive: HistoricalRowsArchive,
    origins: dict[str, MinuteOriginMaterial],
    gaps: list[HistoricalPreparationGap],
    category: Literal["material", "fact"],
) -> tuple[tuple[HistoricalOriginReference, object], ...]:
    try:
        return _archive_rows(archive, origins)
    except ValueError as exc:
        gaps.append(
            HistoricalPreparationGap(
                code="archive_original_layout_invalid",
                category=category,
                detail=str(exc),
                origins=(
                    HistoricalOriginReference(
                        object_key=archive.origin_object_key,
                        json_pointer=archive.rows_pointer,
                    ),
                ),
            )
        )
        return ()


def _minute_slots(day: HistoricalCalendarDay, zone: ZoneInfo) -> tuple[datetime, ...]:
    result = []
    for window in day.windows:
        current = datetime.combine(day.trade_date, window.opens_at, zone) + timedelta(minutes=1)
        end = datetime.combine(day.trade_date, window.closes_at, zone)
        while current <= end:
            result.append(current)
            current += timedelta(minutes=1)
    return tuple(result)


def _prepare_minutes(
    request: HistoricalReconstructionRequest,
    origins: dict[str, MinuteOriginMaterial],
    gaps: list[HistoricalPreparationGap],
) -> tuple[HistoricalPreparedMinute, ...]:
    zone = ZoneInfo(request.calendar_timezone)
    bases = {item.provider_label: item for item in request.timestamp_bases}
    days = {item.trade_date: item for item in request.calendar}
    replay_dates = {item.trade_date for item in request.replay_days}
    groups: dict[tuple[str, datetime], list[tuple[_MinuteRow, HistoricalOriginReference]]] = (
        defaultdict(list)
    )
    for archive in request.minute_archives:
        for origin, raw in _archive_rows_or_unavailable(archive, origins, gaps, "material"):
            reason = None
            row = None
            bar_end = None
            try:
                row = _MinuteRow.model_validate(raw)
                basis = bases.get(row.source)
                if basis is None or basis.semantics == "unknown":
                    reason = "minute_timestamp_basis_unknown"
                else:
                    raw_time = datetime.fromisoformat(row.trade_time)
                    if raw_time.tzinfo is None:
                        if basis.source_timezone is None:
                            reason = "minute_timezone_unknown"
                        else:
                            raw_time = raw_time.replace(tzinfo=ZoneInfo(basis.source_timezone))
                    if reason is None:
                        bar_end = raw_time.astimezone(zone)
                        if basis.semantics == "minute_start":
                            bar_end += timedelta(minutes=1)
                        if bar_end.second or bar_end.microsecond:
                            reason = "minute_not_aligned"
                        elif bar_end.date() not in replay_dates:
                            reason = "minute_not_requested_replay_day"
                        elif bar_end not in _minute_slots(days[bar_end.date()], zone):
                            reason = "minute_outside_session"
            except (ValueError, ValidationError):
                reason = "minute_original_record_invalid"
            if reason is not None:
                gaps.append(
                    HistoricalPreparationGap(
                        code=reason,
                        category="material",
                        detail="Original minute unavailable; no timestamp or numeric replacement.",
                        blocking=reason != "minute_not_requested_replay_day",
                        trade_date=bar_end.date() if bar_end is not None else None,
                        ts_code=row.ts_code if row is not None else None,
                        origins=(origin,),
                    )
                )
            else:
                assert row is not None and bar_end is not None
                groups[(row.ts_code, bar_end)].append((row, origin))
    result = []
    for (code, end), entries in sorted(groups.items(), key=lambda item: (item[0][1], item[0][0])):
        first = entries[0][0]
        fields = ("open", "high", "low", "close", "vol", "amount")
        parents = tuple(item[1] for item in entries)
        if any(
            any(getattr(row, name) != getattr(first, name) for name in fields) for row, _ in entries
        ):
            gaps.append(
                HistoricalPreparationGap(
                    code="minute_value_conflict",
                    category="material",
                    detail="Original OHLC/volume/amount differ; no winner selected.",
                    trade_date=end.date(),
                    ts_code=code,
                    origins=parents,
                )
            )
            continue
        result.append(
            HistoricalPreparedMinute(
                ts_code=code,
                trade_date=end.date(),
                bar_end=end,
                modeled_available_at=end,
                open=first.open,
                high=first.high,
                low=first.low,
                close=first.close,
                volume=first.vol,
                amount=first.amount,
                origins=parents,
            )
        )
    return tuple(result)


def _previous_open_days(
    request: HistoricalReconstructionRequest,
    gaps: list[HistoricalPreparationGap],
) -> dict[date, date]:
    result = {}
    for replay in request.replay_days:
        previous = [
            day.trade_date
            for day in request.calendar
            if day.is_open and day.trade_date < replay.trade_date
        ]
        if previous:
            result[replay.trade_date] = previous[-1]
        else:
            gaps.append(
                HistoricalPreparationGap(
                    code="previous_open_day_unknown",
                    category="material",
                    detail="No previous completed open day is present in the explicit calendar.",
                    trade_date=replay.trade_date,
                )
            )
    return result


def _prepare_candidates(
    request: HistoricalReconstructionRequest,
    origins: dict[str, MinuteOriginMaterial],
    previous: dict[date, date],
    gaps: list[HistoricalPreparationGap],
) -> tuple[HistoricalPreparedCandidate, ...]:
    result = []
    for archive in request.candidate_archives:
        for origin, raw in _archive_rows_or_unavailable(archive, origins, gaps, "material"):
            try:
                row = _CandidateRow.model_validate(raw)
            except ValidationError:
                gaps.append(
                    HistoricalPreparationGap(
                        code="candidate_original_record_invalid",
                        category="material",
                        detail="Original candidate invalid; retained without replacement.",
                        origins=(origin,),
                    )
                )
                continue
            matching = [
                replay
                for replay in request.replay_days
                if previous.get(replay.trade_date) == row.trade_date
            ]
            reason = None
            if not matching:
                reason = "candidate_not_previous_completed_day"
            elif archive.kind == "intraday":
                reason = (
                    "candidate_intraday_publication_unknown"
                    if row.published_at is None
                    else "candidate_not_completed_screen"
                )
            elif archive.kind == "pool":
                reason = (
                    "candidate_pool_version_unknown"
                    if not row.historical_version
                    else "candidate_not_completed_screen"
                )
            elif row.trade_date not in archive.completed_trade_dates:
                reason = "candidate_screen_completion_unknown"
            if reason is not None:
                gaps.append(
                    HistoricalPreparationGap(
                        code=reason,
                        category="material",
                        detail="Only completed previous-day screens receive modeled visibility.",
                        blocking=reason != "candidate_not_previous_completed_day",
                        trade_date=row.trade_date,
                        ts_code=row.ts_code,
                        origins=(origin,),
                    )
                )
                continue
            for replay in matching:
                result.append(
                    HistoricalPreparedCandidate(
                        ts_code=row.ts_code,
                        preset_name=row.preset_name,
                        screen_trade_date=row.trade_date,
                        replay_trade_date=replay.trade_date,
                        modeled_available_at=replay.initialized_at,
                        origin=origin,
                    )
                )
    for replay in request.replay_days:
        if not any(item.replay_trade_date == replay.trade_date for item in result):
            gaps.append(
                HistoricalPreparationGap(
                    code="candidate_universe_unavailable",
                    category="material",
                    detail="Previous completed-day candidates unavailable; no replacement.",
                    trade_date=replay.trade_date,
                )
            )
    return tuple(result)


def _prepare_facts(
    request: HistoricalReconstructionRequest,
    origins: dict[str, MinuteOriginMaterial],
    candidates: tuple[HistoricalPreparedCandidate, ...],
    previous: dict[date, date],
    gaps: list[HistoricalPreparationGap],
) -> tuple[HistoricalPreparedFact, ...]:
    groups: dict[tuple[date, str, FactKind], list[tuple[_FactRow, HistoricalOriginReference]]] = (
        defaultdict(list)
    )
    for archive in request.fact_archives:
        for origin, raw in _archive_rows_or_unavailable(archive, origins, gaps, "fact"):
            try:
                row = _FactRow.model_validate(raw)
            except ValidationError:
                gaps.append(
                    HistoricalPreparationGap(
                        code="fact_original_record_invalid",
                        category="fact",
                        detail="Original fact incomplete; created_at is not publication evidence.",
                        origins=(origin,),
                    )
                )
                continue
            groups[(row.trade_date, row.ts_code, row.kind)].append((row, origin))
    scopes = sorted({(item.replay_trade_date, item.ts_code) for item in candidates})
    bootstraps = {item.trade_date: item.initialized_at for item in request.replay_days}
    result = []
    if not scopes:
        gaps.append(
            HistoricalPreparationGap(
                code="fact_scope_unavailable",
                category="fact",
                detail="Facts cannot be declared complete without the original candidate scope.",
            )
        )
    for day, code in scopes:
        for kind in REQUIRED_FACT_KINDS:
            entries = groups.get((day, code, kind), [])
            parents = tuple(item[1] for item in entries)
            reason = None
            if not entries:
                reason = "fact_missing"
            else:
                first = entries[0][0]
                if any(
                    canonical_sha256(item.model_dump(mode="json"))
                    != canonical_sha256(first.model_dump(mode="json"))
                    for item, _ in entries
                ):
                    reason = "fact_value_conflict"
                elif first.status != "complete":
                    reason = "fact_incomplete"
                elif first.value is None:
                    reason = "fact_value_unknown"
                elif first.available_at is None:
                    reason = "fact_publication_unknown"
                elif first.available_at > bootstraps[day]:
                    reason = "fact_not_visible_at_bootstrap"
                elif kind in {
                    "prior_reference",
                    "prior_state",
                } and first.reference_trade_date != previous.get(day):
                    reason = "fact_reference_day_mismatch"
            if reason is not None:
                gaps.append(
                    HistoricalPreparationGap(
                        code=reason,
                        category="fact",
                        detail=f"{kind} unavailable for {code} on {day}; no backfilled fact.",
                        trade_date=day,
                        ts_code=code,
                        fact_kind=kind,
                        origins=parents,
                    )
                )
            else:
                first = entries[0][0]
                assert first.available_at is not None
                result.append(
                    HistoricalPreparedFact(
                        kind=kind,
                        ts_code=code,
                        trade_date=day,
                        available_at=first.available_at,
                        origins=parents,
                    )
                )
    return tuple(result)


def prepare_historical_minute_reconstruction(
    request: HistoricalReconstructionRequest,
) -> HistoricalReconstructionPreparation:
    """Prepare exact original-value research records without IO, approval, or source installation.

    JSON archives retain complete original bytes. An explicit columns_pointer projects positional
    cells using exact original header names; each value/header keeps its original JSON pointer.
    No column aliases or unit conversions are inferred. Object rows need no columns_pointer.
    Minute rows use ts_code/trade_time/freq/source,
    OHLC/vol/amount; screen rows use ts_code/trade_date/preset_name. Fact rows explicitly carry
    kind/ts_code/trade_date/status/value/available_at and prior-reference date where required.
    Fact presence is preparation evidence, not an assertion that a source owner accepts it.
    """
    request = HistoricalReconstructionRequest.model_validate(request.model_dump(mode="python"))
    origins = {item.object_key: item for item in request.origin_materials}
    gaps: list[HistoricalPreparationGap] = []
    previous = _previous_open_days(request, gaps)
    minutes = _prepare_minutes(request, origins, gaps)
    candidates = _prepare_candidates(request, origins, previous, gaps)
    calendar = {item.trade_date: item for item in request.calendar}
    minute_keys = {(item.trade_date, item.ts_code, item.bar_end) for item in minutes}
    for day, code in sorted({(item.replay_trade_date, item.ts_code) for item in candidates}):
        for expected in _minute_slots(calendar[day], ZoneInfo(request.calendar_timezone)):
            if (day, code, expected) not in minute_keys:
                gaps.append(
                    HistoricalPreparationGap(
                        code="minute_coverage_missing",
                        category="material",
                        detail=f"Missing minute {expected.isoformat()}; no fill or interpolation.",
                        trade_date=day,
                        ts_code=code,
                    )
                )
    facts = _prepare_facts(request, origins, candidates, previous, gaps)
    materials_ready = not any(item.blocking and item.category == "material" for item in gaps)
    facts_ready = not any(item.blocking and item.category == "fact" for item in gaps)
    transformation = canonical_sha256(
        {
            "algorithm": "historical-minute-preparation/v1",
            "policy": request.policy.fingerprint,
            "request": request.model_dump(mode="json"),
        }
    )
    derivations = tuple(
        MinuteDerivation(
            material_path=f"preparation/{name}.json",
            origin_object_keys=tuple(sorted({item.origin_object_key for item in archives})),
            method="research_derivative",
            transformation_fingerprint=transformation,
            time_basis="modeled",
        )
        for name, archives, values in (
            ("completed-minutes", request.minute_archives, minutes),
            ("previous-screen-candidates", request.candidate_archives, candidates),
        )
        if values
    )
    return HistoricalReconstructionPreparation(
        policy=request.policy,
        origin_materials=request.origin_materials,
        timestamp_bases=request.timestamp_bases,
        registration=request.registration,
        native_definition_visibility=tuple(
            HistoricalNativeDefinitionVisibility(
                replay_trade_date=item.trade_date,
                modeled_available_at=item.initialized_at,
            )
            for item in request.replay_days
        ),
        replay_days=request.replay_days,
        prepared_at=request.prepared_at,
        minutes=minutes,
        candidates=candidates,
        facts=facts,
        derivations=derivations,
        unavailable_reasons=tuple(gaps),
        materials_ready=materials_ready,
        facts_ready=facts_ready,
        preparation_status="complete" if materials_ready and facts_ready else "unavailable",
    )
