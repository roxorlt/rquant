"""Explicit Tushare captures and fail-closed historical security normalization.

Run ``python -m rquant.factor.security_collect --help`` for live capture,
offline probe import, replay, and the existing member archive publisher.
Importing this module does not load application settings or a network client.
"""

from __future__ import annotations

import argparse
import hashlib
import math
import os
import re
import secrets
import sys
import time
from collections.abc import Callable, Iterator, Sequence
from datetime import UTC, date, datetime, timedelta, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Literal, Protocol, TypeVar

import pandas as pd
from pydantic import BaseModel, ConfigDict, Field, model_validator

from rquant.factor.member_archive import (
    FactorMemberArchiveReference,
    FactorMemberArchiveRequest,
    FactorMemberDayInput,
    _read_file,
    publish_factor_member_archive,
)
from rquant.factor.result_artifact import (
    _cleanup_owned_temporary,
    _open_private_root,
    _require_same_root,
    _root_path,
    _write_all,
)
from rquant.factor.universe import (
    DailySecurityBatch,
    DailySecurityFact,
    FactorUniverseError,
    FactorUniverseRequest,
    ObservedTime,
    Sha256,
    UniverseSelection,
    select_factor_universe,
)
from rquant.private_fs import rename_noreplace_at
from rquant.runtime_contracts import canonical_sha256
from rquant.security_status import normalize_name
from rquant.source_quota_store import SourceQuotaAttempt, SourceQuotaExhaustedError
from rquant.source_quota_transport import SourceTransportCallReceipt, SourceTransportObserver
from rquant.strict_json import canonical_json_bytes, strict_canonical_json_loads, strict_json_loads

if TYPE_CHECKING:
    from rquant.factor.name_collect import CapturedNameResponse

STOCK_REFERENCE_FIELDS = (
    "ts_code",
    "name",
    "exchange",
    "curr_type",
    "market",
    "list_status",
    "list_date",
    "delist_date",
)
DAILY_SECURITY_FIELDS = ("trade_date", "ts_code", "name", "list_date")
LIST_STATUSES = ("L", "D", "P", "G", "UN")
EXCHANGES = ("SSE", "SZSE", "BSE")
MAX_CAPTURE_DAYS = 31
MAX_CAPTURE_CALLS = 64
MAX_CAPTURE_BYTES = 16 * 1024 * 1024
MAX_COLLECTION_BYTES = 256 * 1024
_EARLIEST = date(2016, 1, 1)
_CHINA = timezone(timedelta(hours=8))
_STOCK_CODE = re.compile(r"[0-9]{6}\.(?:SH|SZ|BJ)\Z")
_EXCHANGE_SUFFIX = {"SSE": "SH", "SZSE": "SZ", "BSE": "BJ"}
_MARKET_BOARD = {"主板": "main", "创业板": "gem", "科创板": "star", "北交所": "bse"}
_MODEL = ConfigDict(extra="forbid", frozen=True, strict=True, revalidate_instances="always")
RawCell = str | int | float | bool | None
_T = TypeVar("_T")


class SecuritySourceRequest(BaseModel):
    model_config = _MODEL

    api_name: Literal["stock_basic", "bak_basic"]
    fields: tuple[str, ...]
    list_status: Literal["L", "D", "P", "G", "UN"] | None = None
    exchange: Literal["", "SSE", "SZSE", "BSE"] | None = None
    trade_date: date | None = None

    @model_validator(mode="after")
    def _contract(self) -> SecuritySourceRequest:
        if self.api_name == "stock_basic":
            if (
                self.list_status is None
                or self.exchange is None
                or self.trade_date is not None
                or self.fields != STOCK_REFERENCE_FIELDS
            ):
                raise ValueError("stock_basic requires explicit fields, status and exchange")
        elif (
            self.trade_date is None
            or self.trade_date < _EARLIEST
            or self.exchange is not None
            or self.list_status is not None
            or self.fields != DAILY_SECURITY_FIELDS
        ):
            raise ValueError("bak_basic requires explicit supported date and fields")
        return self


class RawSecurityTable(BaseModel):
    model_config = _MODEL

    fields: tuple[str, ...]
    items: tuple[tuple[RawCell, ...], ...]

    @model_validator(mode="after")
    def _shape(self) -> RawSecurityTable:
        if any(len(row) != len(self.fields) for row in self.items):
            raise ValueError("raw response row width differs from fields")
        if any(
            isinstance(value, float) and not math.isfinite(value)
            for row in self.items
            for value in row
        ):
            raise ValueError("raw response contains a non-finite number")
        return self


class CapturedSecurityResponse(BaseModel):
    model_config = _MODEL

    schema_version: Literal[1] = 1
    request: SecuritySourceRequest
    requested_at: ObservedTime
    observed_at: ObservedTime
    response: RawSecurityTable
    response_sha256: Sha256

    @model_validator(mode="after")
    def _binding(self) -> CapturedSecurityResponse:
        if self.requested_at > self.observed_at:
            raise ValueError("response observation precedes request")
        if self.response_sha256 != hashlib.sha256(_bytes(self.response)).hexdigest():
            raise ValueError("raw response digest differs")
        return self


class SecuritySourceDiagnostic(BaseModel):
    model_config = _MODEL

    severity: Literal["error", "info"]
    reason: str
    detail: str
    stock_code: str | None = None


class SecurityDayResult(BaseModel):
    model_config = _MODEL

    trade_date: date
    batch: DailySecurityBatch | None
    diagnostics: tuple[SecuritySourceDiagnostic, ...]

    @model_validator(mode="after")
    def _refusal(self) -> SecurityDayResult:
        failed = any(issue.severity == "error" for issue in self.diagnostics)
        if failed != (self.batch is None):
            raise ValueError("normalization result and diagnostics differ")
        if self.batch is not None and self.batch.trade_date != self.trade_date:
            raise ValueError("normalization result date differs")
        return self

    @property
    def accepted(self) -> bool:
        return self.batch is not None

    @property
    def reason(self) -> str | None:
        if self.accepted:
            return None
        if any(issue.reason == "historical_security_missing" for issue in self.diagnostics):
            return "当日历史股票名单不完整"
        if any(issue.reason == "provider_limit" for issue in self.diagnostics):
            return "来源响应达到上限，无法确认完整名单"
        return "历史股票资料缺失或冲突，暂不能使用该日期"


class SecurityCaptureReference(BaseModel):
    model_config = _MODEL

    request: SecuritySourceRequest
    filename: str
    sha256: Sha256
    byte_count: int = Field(gt=0, le=MAX_CAPTURE_BYTES)
    row_count: int = Field(ge=0)
    observed_at: ObservedTime

    @model_validator(mode="after")
    def _filename(self) -> SecurityCaptureReference:
        if self.filename != _capture_filename(self.request):
            raise ValueError("source capture filename differs from request")
        return self


class SecurityCollectionManifest(BaseModel):
    model_config = _MODEL

    schema_version: Literal[1] = 1
    status: Literal["captured"] = "captured"
    trading_days: tuple[date, ...] = Field(min_length=1, max_length=MAX_CAPTURE_DAYS)
    responses: tuple[SecurityCaptureReference, ...] = Field(max_length=MAX_CAPTURE_CALLS)
    actual_call_count: int | None = Field(default=None, ge=0, le=MAX_CAPTURE_CALLS)

    @model_validator(mode="after")
    def _schedule(self) -> SecurityCollectionManifest:
        _validate_days(self.trading_days)
        requests = tuple(ref.request for ref in self.responses)
        if len(set(requests)) != len(requests):
            raise ValueError("duplicate source capture request")
        if (
            tuple(
                sorted(
                    request.trade_date for request in requests if request.api_name == "bak_basic"
                )
            )
            != self.trading_days
        ):
            raise ValueError("captured day schedule differs")
        return self


class SecurityCaptureRequest(BaseModel):
    model_config = _MODEL

    root: Path
    trading_days: tuple[date, ...] = Field(min_length=1, max_length=MAX_CAPTURE_DAYS)
    max_calls: int = Field(ge=1, le=MAX_CAPTURE_CALLS)
    exchanges: tuple[Literal["", "SSE", "SZSE", "BSE"], ...] = EXCHANGES

    @model_validator(mode="after")
    def _scope(self) -> SecurityCaptureRequest:
        _root_path(self.root)
        _validate_days(self.trading_days)
        if self.exchanges not in (("",), EXCHANGES):
            raise ValueError("reference exchanges must cover all exchanges without overlap")
        if self.max_calls < len(LIST_STATUSES) * len(self.exchanges) + len(self.trading_days):
            raise ValueError(
                "call budget cannot cover the requested dates and reference partitions"
            )
        return self


class SecurityCaptureAdapter(Protocol):
    def stock_basic_history_raw(self, *, list_status: str, exchange: str) -> pd.DataFrame: ...

    def bak_basic_raw(self, trade_date: date) -> pd.DataFrame: ...


class _CaptureBudget:
    def __init__(
        self,
        max_calls: int,
        *,
        api_names: frozenset[str] = frozenset({"stock_basic", "bak_basic"}),
    ) -> None:
        self.max_calls = max_calls
        self.api_names = api_names
        self.actual_call_count = 0

    def observe(self, api_name: str, call: Callable[[], _T]) -> _T:
        if api_name not in self.api_names:
            raise SourceQuotaExhaustedError("capture API is outside the explicit scope")
        if self.actual_call_count >= self.max_calls:
            raise SourceQuotaExhaustedError("explicit security capture call budget exhausted")
        self.actual_call_count += 1
        return call()

    def current_receipts(self) -> tuple[SourceTransportCallReceipt, ...]:
        return ()

    def request_attempts(self, logical_request_id: str) -> tuple[SourceQuotaAttempt, ...]:
        return ()

    def request_outcome(self, logical_request_id: str) -> None:
        return None


def _validate_days(days: tuple[date, ...]) -> None:
    if tuple(sorted(set(days))) != days or any(day < _EARLIEST for day in days):
        raise ValueError("security dates must be unique, ascending and no earlier than 2016")


def _reference_request(status: str, exchange: str) -> SecuritySourceRequest:
    return SecuritySourceRequest(
        api_name="stock_basic", fields=STOCK_REFERENCE_FIELDS, list_status=status, exchange=exchange
    )


def _daily_request(day: date) -> SecuritySourceRequest:
    return SecuritySourceRequest(api_name="bak_basic", fields=DAILY_SECURITY_FIELDS, trade_date=day)


def _capture_filename(request: SecuritySourceRequest) -> str:
    if request.api_name == "stock_basic":
        return f"stock-basic-{request.list_status}-{request.exchange or 'ALL'}.json"
    return f"bak-basic-{request.trade_date.strftime('%Y%m%d')}.json"


def _bytes(model: BaseModel) -> bytes:
    return canonical_json_bytes(model.model_dump(mode="json", round_trip=True))


def _raw_cell(value: object) -> RawCell:
    if value is None or pd.isna(value):
        return None
    if isinstance(value, (str, int, float, bool)):
        return value
    item = getattr(value, "item", None)
    if callable(item):
        return _raw_cell(item())
    raise ValueError("provider cell cannot be preserved as a JSON scalar")


def make_security_capture(
    request: SecuritySourceRequest,
    frame: pd.DataFrame,
    *,
    requested_at: datetime,
    observed_at: datetime,
) -> CapturedSecurityResponse:
    """Retain provider columns and scalar values, including zero listing placeholders."""
    if not isinstance(frame, pd.DataFrame):
        raise ValueError("provider response is not a table")
    table = RawSecurityTable(
        fields=tuple(frame.columns),
        items=tuple(
            tuple(_raw_cell(value) for value in row)
            for row in frame.itertuples(index=False, name=None)
        ),
    )
    return CapturedSecurityResponse(
        request=request,
        requested_at=requested_at,
        observed_at=observed_at,
        response=table,
        response_sha256=hashlib.sha256(_bytes(table)).hexdigest(),
    )


def _new_root(root: Path) -> int:
    root = _root_path(root)
    root.mkdir(mode=0o700)
    return _open_private_root(root)


def _write_new(root: Path, descriptor: int, filename: str, data: bytes) -> None:
    if len(data) > MAX_CAPTURE_BYTES:
        raise ValueError("capture file exceeds byte limit")
    _require_same_root(root, descriptor)
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC
    file_descriptor = os.open(filename, flags, 0o600, dir_fd=descriptor)
    try:
        _write_all(file_descriptor, data)
        os.fsync(file_descriptor)
    finally:
        os.close(file_descriptor)
    os.fsync(descriptor)


def _publish_collection(root: Path, descriptor: int, manifest: BaseModel) -> None:
    data = _bytes(manifest)
    if len(data) > MAX_COLLECTION_BYTES:
        raise ValueError("collection manifest exceeds byte limit")
    temporary = f".security-collection-{secrets.token_hex(16)}.tmp"
    identity: tuple[int, int] | None = None
    try:
        _require_same_root(root, descriptor)
        file_descriptor = os.open(
            temporary,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC,
            0o600,
            dir_fd=descriptor,
        )
        try:
            observed = os.fstat(file_descriptor)
            identity = observed.st_dev, observed.st_ino
            _write_all(file_descriptor, data)
            os.fsync(file_descriptor)
        finally:
            os.close(file_descriptor)
        _require_same_root(root, descriptor)
        rename_noreplace_at(descriptor, temporary, descriptor, "collection.json")
        os.fsync(descriptor)
    except BaseException:
        if identity is not None:
            _cleanup_owned_temporary(descriptor, "collection.json", identity)
        raise
    finally:
        if identity is not None:
            _cleanup_owned_temporary(descriptor, temporary, identity)


def _reject_interrupted_collection(descriptor: int) -> None:
    try:
        os.stat("interrupted.json", dir_fd=descriptor, follow_symlinks=False)
    except FileNotFoundError:
        return
    raise ValueError("来源目录记录了采集中断，不能作为完成资料")


def _save_capture(
    root: Path, descriptor: int, capture: CapturedSecurityResponse
) -> SecurityCaptureReference:
    data = _bytes(capture)
    reference = SecurityCaptureReference(
        request=capture.request,
        filename=_capture_filename(capture.request),
        sha256=hashlib.sha256(data).hexdigest(),
        byte_count=len(data),
        row_count=len(capture.response.items),
        observed_at=capture.observed_at,
    )
    _write_new(root, descriptor, reference.filename, data)
    return reference


def collect_security_sources(
    request: SecurityCaptureRequest,
    *,
    adapter_factory: Callable[[SourceTransportObserver], SecurityCaptureAdapter],
    clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    request_interval_seconds: float = 0.35,
    sleep: Callable[[float], None] = time.sleep,
) -> SecurityCollectionManifest:
    """Capture a bounded explicit date schedule, counting transport retries in its budget."""
    request = SecurityCaptureRequest.model_validate(request)
    if not math.isfinite(request_interval_seconds) or request_interval_seconds < 0:
        raise ValueError("request interval must be finite and nonnegative")
    budget = _CaptureBudget(request.max_calls)
    descriptor = _new_root(request.root)
    references: list[SecurityCaptureReference] = []
    try:
        adapter = adapter_factory(budget)
        requests = tuple(
            _reference_request(status, exchange)
            for status in LIST_STATUSES
            for exchange in request.exchanges
        )
        requests += tuple(_daily_request(day) for day in request.trading_days)
        for source_request in requests:
            requested_at = clock()
            frame = (
                adapter.stock_basic_history_raw(
                    list_status=source_request.list_status, exchange=source_request.exchange
                )
                if source_request.api_name == "stock_basic"
                else adapter.bak_basic_raw(source_request.trade_date)
            )
            observed_at = clock()
            capture = make_security_capture(
                source_request, frame, requested_at=requested_at, observed_at=observed_at
            )
            references.append(_save_capture(request.root, descriptor, capture))
            del frame, capture
            sleep(request_interval_seconds)
        manifest = SecurityCollectionManifest(
            trading_days=request.trading_days,
            responses=tuple(references),
            actual_call_count=budget.actual_call_count,
        )
        _publish_collection(request.root, descriptor, manifest)
        return manifest
    except BaseException as exc:
        _write_new(
            request.root,
            descriptor,
            "interrupted.json",
            canonical_json_bytes(
                {
                    "schema_version": 1,
                    "status": "interrupted",
                    "error_type": type(exc).__name__,
                    "completed_response_count": len(references),
                    "actual_call_count": budget.actual_call_count,
                }
            ),
        )
        raise
    finally:
        os.close(descriptor)


def load_security_capture_collection(root: Path) -> SecurityCollectionManifest:
    root = _root_path(root)
    descriptor = _open_private_root(root)
    try:
        _reject_interrupted_collection(descriptor)
        data, _ = _read_file(descriptor, "collection.json", MAX_COLLECTION_BYTES)
        strict_canonical_json_loads(data)
        manifest = SecurityCollectionManifest.model_validate_json(data)
        _reject_interrupted_collection(descriptor)
        _require_same_root(root, descriptor)
        return manifest
    finally:
        os.close(descriptor)


def _load_capture(descriptor: int, reference: SecurityCaptureReference) -> CapturedSecurityResponse:
    data, _ = _read_file(descriptor, reference.filename, MAX_CAPTURE_BYTES, reference.sha256)
    if len(data) != reference.byte_count:
        raise ValueError("capture byte count differs")
    strict_canonical_json_loads(data)
    capture = CapturedSecurityResponse.model_validate_json(data)
    if (
        capture.request != reference.request
        or capture.observed_at != reference.observed_at
        or len(capture.response.items) != reference.row_count
    ):
        raise ValueError("capture response differs from its collection reference")
    return capture


def _source_rows(
    capture: CapturedSecurityResponse, diagnostics: list[SecuritySourceDiagnostic]
) -> list[dict[str, RawCell]]:
    fields = capture.response.fields
    limit = 6000 if capture.request.api_name == "stock_basic" else 7000
    if len(capture.response.items) >= limit:
        diagnostics.append(_issue("provider_limit", f"{capture.request.api_name}: rows>={limit}"))
        return []
    if len(set(fields)) != len(fields) or not set(capture.request.fields) <= set(fields):
        diagnostics.append(
            _issue("missing_columns", f"{capture.request.api_name}: fields={fields}")
        )
        return []
    return [dict(zip(fields, row, strict=True)) for row in capture.response.items]


def _issue(
    reason: str, detail: str, code: str | None = None, *, info: bool = False
) -> SecuritySourceDiagnostic:
    return SecuritySourceDiagnostic(
        severity="info" if info else "error", reason=reason, detail=detail, stock_code=code
    )


def _date(value: RawCell, *, optional: bool = False) -> date | None:
    if optional and value in (None, "", "0", 0):
        return None
    if not isinstance(value, str) or not re.fullmatch(r"[0-9]{8}", value):
        raise ValueError("date is not a YYYYMMDD string")
    return datetime.strptime(value, "%Y%m%d").date()


def _reference_coverage(
    references: Sequence[CapturedSecurityResponse], diagnostics: list[SecuritySourceDiagnostic]
) -> None:
    scopes = tuple(
        (capture.request.list_status, capture.request.exchange) for capture in references
    )
    global_scope = {(status, "") for status in LIST_STATUSES}
    partitioned_scope = {(status, exchange) for status in LIST_STATUSES for exchange in EXCHANGES}
    if (
        any(capture.request.api_name != "stock_basic" for capture in references)
        or len(set(scopes)) != len(scopes)
        or set(scopes) not in (global_scope, partitioned_scope)
    ):
        diagnostics.append(_issue("reference_coverage_missing", f"reference partitions: {scopes}"))


def normalize_security_day(
    daily: CapturedSecurityResponse,
    references: Sequence[CapturedSecurityResponse],
    *,
    selection: UniverseSelection = "all",
    name_sources: Sequence[CapturedNameResponse] = (),
) -> SecurityDayResult:
    """Cross-check the listing intervals and historical names; never guess missing members."""
    daily = CapturedSecurityResponse.model_validate(daily)
    references = tuple(CapturedSecurityResponse.model_validate(item) for item in references)
    if daily.request.api_name != "bak_basic":
        raise ValueError("daily normalization requires a bak_basic request")
    day = daily.request.trade_date
    names: dict[str, CapturedNameResponse] = {}
    used_names: list[CapturedNameResponse] = []
    if name_sources:
        from rquant.factor.name_collect import (
            MAX_NAME_CODES,
            CapturedNameResponse,
            resolve_name_day,
        )

        if len(name_sources) > MAX_NAME_CODES:
            raise ValueError("name supplement exceeds explicit code budget")
        for source in name_sources:
            source = CapturedNameResponse.model_validate(source)
            if source.request.ts_code in names:
                raise ValueError("duplicate name supplement code")
            names[source.request.ts_code] = source
    diagnostics: list[SecuritySourceDiagnostic] = []
    _reference_coverage(references, diagnostics)
    if any(capture.observed_at.astimezone(_CHINA).date() < day for capture in (*references, daily)):
        diagnostics.append(
            _issue("source_before_trade_date", "response predates requested trade date")
        )
    expected: dict[str, tuple[dict[str, RawCell], date, date | None]] = {}
    known: dict[str, tuple[dict[str, RawCell], date | None, date | None]] = {}
    for capture in references:
        for row in _source_rows(capture, diagnostics):
            code = row["ts_code"]
            if not isinstance(code, str) or not code:
                diagnostics.append(_issue("invalid_reference_code", "reference code is missing"))
                continue
            if code in known:
                diagnostics.append(
                    _issue("duplicate_reference", "code appears in multiple rows", code)
                )
                continue
            if (
                row["list_status"] != capture.request.list_status
                or row["exchange"] not in _EXCHANGE_SUFFIX
                or (capture.request.exchange and row["exchange"] != capture.request.exchange)
            ):
                diagnostics.append(
                    _issue("reference_partition_conflict", "status or exchange differs", code)
                )
                continue
            status = row["list_status"]
            try:
                listed = _date(row["list_date"], optional=status in ("G", "UN"))
                delisted = _date(row["delist_date"], optional=status != "D")
                if listed is not None and delisted is not None and delisted <= listed:
                    raise ValueError("delisting does not follow listing")
                if status in ("G", "UN") and (
                    delisted is not None or (listed is not None and listed <= day)
                ):
                    raise ValueError("unlisted status conflicts with listing interval")
            except ValueError as exc:
                diagnostics.append(_issue("listing_interval_unknown", str(exc), code))
                continue
            known[code] = (row, listed, delisted)
            if delisted is not None and day >= delisted:
                diagnostics.append(
                    _issue("outside_listing_interval", f"delisted {delisted}", code, info=True)
                )
                continue
            if status in ("G", "UN") or day < listed:
                diagnostics.append(
                    _issue(
                        "not_yet_listed", "explicit unlisted status or later IPO", code, info=True
                    )
                )
                continue
            if row["curr_type"] != "CNY":
                if row["curr_type"] in ("USD", "HKD"):
                    diagnostics.append(
                        _issue("outside_a_share_scope", "non-CNY security", code, info=True)
                    )
                else:
                    diagnostics.append(
                        _issue("unknown_currency", "cannot establish A-share currency", code)
                    )
                continue
            if not _STOCK_CODE.fullmatch(code) or code[-2:] != _EXCHANGE_SUFFIX[row["exchange"]]:
                diagnostics.append(
                    _issue("invalid_reference_code", "unresolved active security identity", code)
                )
                continue
            expected[code] = (row, listed, delisted)
    history: dict[str, dict[str, RawCell]] = {}
    daily_rows = _source_rows(daily, diagnostics)
    if not daily_rows:
        diagnostics.append(_issue("empty_history", "no usable daily historical rows"))
    for row in daily_rows:
        code = row["ts_code"]
        if not isinstance(code, str) or not code:
            diagnostics.append(_issue("invalid_history_code", "historical code is missing"))
            continue
        if code in history:
            diagnostics.append(
                _issue("duplicate_history", "duplicate historical security row", code)
            )
            continue
        history[code] = row
        try:
            if _date(row["trade_date"]) != day:
                raise ValueError("historical row belongs to another trade date")
            historical_listing = _date(row["list_date"], optional=True)
        except ValueError as exc:
            diagnostics.append(_issue("historical_date_conflict", str(exc), code))
            continue
        reference = known.get(code)
        if reference is None:
            diagnostics.append(
                _issue(
                    "historical_reference_missing", "historical code lacks listing reference", code
                )
            )
            continue
        reference_row, listed, _ = reference
        if code not in expected:
            if (
                reference_row["list_status"] in ("G", "UN")
                and historical_listing is not None
                and historical_listing <= day
            ):
                diagnostics.append(
                    _issue(
                        "historical_listing_conflict",
                        "daily listing conflicts with unlisted status",
                        code,
                    )
                )
            elif (
                historical_listing is not None
                and listed is not None
                and historical_listing != listed
            ):
                diagnostics.append(
                    _issue(
                        "historical_listing_conflict", "daily and reference IPO dates differ", code
                    )
                )
        elif historical_listing != listed:
            diagnostics.append(
                _issue(
                    "historical_listing_conflict",
                    "active daily IPO date is missing or differs",
                    code,
                )
            )
    for code in sorted(set(expected) - set(history)):
        diagnostics.append(
            _issue(
                "historical_security_missing",
                "listed security absent from bak_basic",
                code,
                info=True,
            )
        )
    facts: list[DailySecurityFact] = []
    for code in sorted(expected):
        row = expected[code][0]
        board = _MARKET_BOARD.get(row["market"])
        name, is_st = normalize_name(history.get(code, {}).get("name"))
        if code in names:
            evidence = resolve_name_day(names[code], day)
            diagnostics.extend(evidence.diagnostics)
            if evidence.name is not None:
                if name is not None and name != evidence.name:
                    diagnostics.append(
                        _issue("name_daily_conflict", "daily and interval names differ", code)
                    )
                else:
                    name, is_st = evidence.name, evidence.is_st
                    used_names.append(names[code])
                    diagnostics.append(
                        _issue(
                            "name_interval_used",
                            "single effective namechange interval",
                            code,
                            info=True,
                        )
                    )
        if board is None:
            diagnostics.append(
                _issue("unknown_market", "provider market has no explicit board", code, info=True)
            )
        if name is None or is_st is None:
            diagnostics.append(
                _issue(
                    "historical_name_missing", "daily historical name is unknown", code, info=True
                )
            )
        try:
            facts.append(
                DailySecurityFact(
                    stock_code=code, exchange=code[-2:], board=board, is_listed=True, is_st=is_st
                )
            )
        except ValueError:
            diagnostics.append(
                _issue("exchange_board_conflict", "provider market and exchange conflict", code)
            )
    if not expected:
        diagnostics.append(_issue("empty_market", "reference does not establish a nonempty market"))
    batch = None
    if not any(issue.severity == "error" for issue in diagnostics):
        source_sha = canonical_sha256(
            (
                "tushare-historical-security-v1",
                tuple(
                    sorted(
                        references,
                        key=lambda capture: (capture.request.list_status, capture.request.exchange),
                    )
                ),
                daily,
            )
        )
        source_id = "tushare:bak_basic+stock_basic:v1"
        observations = [capture.observed_at for capture in (*references, daily)]
        if used_names:
            source_sha = canonical_sha256(
                ("tushare-historical-security-names-v1", source_sha, tuple(used_names))
            )
            source_id = "tushare:bak_basic+stock_basic+namechange:v1"
            observations.extend(capture.observed_at for capture in used_names)
        batch = DailySecurityBatch(
            trade_date=day,
            source_id=source_id,
            source_sha256=source_sha,
            source_mode="historical_retrospective",
            security_scope="china_a_share",
            observed_at=max(observations),
            complete_stock_codes=tuple(expected),
            facts=tuple(facts),
        )
        try:
            select_factor_universe(
                FactorUniverseRequest(
                    selection=selection, trade_date=day, as_of=batch.observed_at, securities=batch
                )
            )
        except FactorUniverseError as exc:
            diagnostics.append(_issue(exc.reason, "selected pool requires missing daily facts"))
            batch = None
    return SecurityDayResult(trade_date=day, batch=batch, diagnostics=tuple(diagnostics))


def iter_security_collection_days(
    root: Path, *, selection: UniverseSelection = "all", name_root: Path | None = None
) -> Iterator[SecurityDayResult]:
    """Keep one reusable reference and at most one daily response in memory."""
    manifest = load_security_capture_collection(root)
    name_sources = ()
    if name_root is not None:
        from rquant.factor.name_collect import iter_name_captures

        name_sources = tuple(iter_name_captures(name_root))
    descriptor = _open_private_root(root)
    try:
        references = tuple(
            _load_capture(descriptor, reference)
            for reference in manifest.responses
            if reference.request.api_name == "stock_basic"
        )
        days = {
            reference.request.trade_date: reference
            for reference in manifest.responses
            if reference.request.api_name == "bak_basic"
        }
        for day in manifest.trading_days:
            capture = _load_capture(descriptor, days[day])
            result = normalize_security_day(
                capture, references, selection=selection, name_sources=name_sources
            )
            del capture
            _require_same_root(root, descriptor)
            yield result
            del result
    finally:
        os.close(descriptor)


class _ProbeRecord(BaseModel):
    model_config = ConfigDict(extra="ignore")

    api_name: Literal["stock_basic", "bak_basic"]
    params: dict[str, str] = Field(default_factory=dict)
    fields: tuple[str, ...]
    requested_at: ObservedTime
    observed_at: ObservedTime
    status: Literal["response_archived"]
    filename: str | None = None
    trade_date: str | None = None
    payload_sha256: Sha256
    row_count: int = Field(ge=0)

    @model_validator(mode="after")
    def _request_scope(self) -> _ProbeRecord:
        if self.api_name == "stock_basic":
            if set(self.params) != {"exchange", "list_status"} or self.trade_date is not None:
                raise ValueError("probe stock_basic request scope is incomplete or filtered")
        elif set(self.params) not in (set(), {"trade_date"}):
            raise ValueError("probe bak_basic request scope is incomplete or filtered")
        elif not self.params and self.trade_date is None:
            raise ValueError("probe bak_basic request scope is missing its date")
        elif (
            self.trade_date is not None
            and self.params
            and self.trade_date != self.params["trade_date"]
        ):
            raise ValueError("probe bak_basic request scope has conflicting dates")
        return self


def import_security_probes(
    source_roots: Sequence[Path], *, root: Path
) -> SecurityCollectionManifest:
    """Import existing probe raw files using their actual receipt metadata, without retiming."""
    if not source_roots or len(source_roots) > 8:
        raise ValueError("probe import requires one to eight explicit source directories")
    descriptor = _new_root(root)
    references: list[SecurityCaptureReference] = []
    dates: set[date] = set()
    try:
        for source in source_roots:
            source = _root_path(source)
            metadata = strict_json_loads((source / "metadata.json").read_bytes())
            records = metadata.get("requests", [metadata])
            if len(records) + len(references) > MAX_CAPTURE_CALLS:
                raise ValueError("probe import exceeds capture budget")
            for raw_record in records:
                record = _ProbeRecord.model_validate(raw_record)
                if record.api_name == "stock_basic":
                    request = _reference_request(
                        record.params["list_status"], record.params["exchange"]
                    )
                else:
                    day = _date(record.params.get("trade_date", record.trade_date))
                    request = _daily_request(day)
                    dates.add(day)
                if record.fields != request.fields:
                    raise ValueError("probe request fields differ from supported source contract")
                filename = record.filename or "bak-basic-response.json"
                if Path(filename).name != filename or filename in (".", ".."):
                    raise ValueError("probe response filename is not a direct child")
                data = (source / filename).read_bytes()
                if (
                    len(data) > MAX_CAPTURE_BYTES
                    or hashlib.sha256(data).hexdigest() != record.payload_sha256
                ):
                    raise ValueError("probe raw response digest or byte count differs")
                strict_canonical_json_loads(data)
                table = RawSecurityTable.model_validate_json(data)
                if len(table.items) != record.row_count:
                    raise ValueError("probe response row count differs")
                capture = CapturedSecurityResponse(
                    request=request,
                    requested_at=record.requested_at,
                    observed_at=record.observed_at,
                    response=table,
                    response_sha256=record.payload_sha256,
                )
                references.append(_save_capture(root, descriptor, capture))
                del data, table, capture
        manifest = SecurityCollectionManifest(
            trading_days=tuple(sorted(dates)), responses=tuple(references)
        )
        _publish_collection(root, descriptor, manifest)
        return manifest
    except BaseException as exc:
        _write_new(
            root,
            descriptor,
            "interrupted.json",
            canonical_json_bytes(
                {
                    "schema_version": 1,
                    "status": "interrupted",
                    "error_type": type(exc).__name__,
                    "completed_response_count": len(references),
                    "actual_call_count": None,
                }
            ),
        )
        raise
    finally:
        os.close(descriptor)


def archive_security_collection(
    capture_root: Path,
    *,
    selection: UniverseSelection,
    as_of: datetime,
    input_root: Path,
    root: Path,
    trading_days: tuple[date, ...] | None = None,
    name_root: Path | None = None,
) -> FactorMemberArchiveReference:
    """Validate every requested day, then use the existing immutable archive publisher."""
    if selection in ("hs300", "zz1000"):
        raise ValueError("尚无完整逐日指数成分，无法归档该指数股票池")
    manifest = load_security_capture_collection(capture_root)
    days = manifest.trading_days if trading_days is None else trading_days
    _validate_days(days)
    if not days or not set(days) <= set(manifest.trading_days):
        raise ValueError("requested archive days are absent from source collection")
    codes: set[str] = set()
    for result in iter_security_collection_days(
        capture_root, selection=selection, name_root=name_root
    ):
        if result.trade_date not in days:
            continue
        if not result.accepted:
            raise ValueError(f"{result.trade_date}: {result.reason}")
        if result.batch.observed_at > as_of:
            raise ValueError("归档截止时刻早于真实来源接收时刻")
        codes.update(result.batch.complete_stock_codes)
    request = FactorMemberArchiveRequest(
        selection=selection,
        trading_days=days,
        as_of=as_of,
        computation_stock_codes=tuple(sorted(codes)),
    )
    descriptor = _new_root(input_root)
    try:
        names: list[str] = []
        for result in iter_security_collection_days(
            capture_root, selection=selection, name_root=name_root
        ):
            if result.trade_date not in days:
                continue
            if not result.accepted:
                raise ValueError(f"{result.trade_date}: {result.reason}")
            payload = FactorMemberDayInput(
                schema_version=1,
                trade_date=result.trade_date,
                securities=result.batch,
                membership=None,
            )
            name = f"security-{result.trade_date.strftime('%Y%m%d')}.json"
            _write_new(input_root, descriptor, name, _bytes(payload))
            names.append(name)
        archive_descriptor = _new_root(root)
        os.close(archive_descriptor)
        return publish_factor_member_archive(
            request, input_root=input_root, daily_filenames=names, root=root
        )
    finally:
        os.close(descriptor)


def _live_adapter(observer: SourceTransportObserver) -> SecurityCaptureAdapter:
    from rquant.adapter.tushare import TushareAdapter

    return TushareAdapter(backup_token="", transport_observer=observer)


def _cli_day(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("日期须为 YYYY-MM-DD") from exc


def _cli_time(value: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value)
        if parsed.tzinfo is None or parsed.utcoffset() is None:
            raise ValueError("timezone missing")
        return parsed
    except ValueError as exc:
        raise argparse.ArgumentTypeError("截止时刻须为含时区的 ISO 时间") from exc


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="历史股票名单采集、离线回放与成员归档")
    commands = parser.add_subparsers(dest="command", required=True)
    live = commands.add_parser("live", help="显式调用已有 Tushare 配置，只写新建私有目录")
    live.add_argument("--date", dest="days", action="append", type=_cli_day, required=True)
    live.add_argument("--root", type=Path, required=True)
    live.add_argument("--max-calls", type=int, required=True)
    live.add_argument(
        "--all-exchanges", action="store_true", help="五个全交易所状态分区（默认十五个交易所分区）"
    )
    probe = commands.add_parser("import-probes", help="导入实际已捕获原件与真实接收元数据")
    probe.add_argument("--source-root", dest="sources", action="append", type=Path, required=True)
    probe.add_argument("--root", type=Path, required=True)
    replay = commands.add_parser("replay", help="离线核对日期并输出拒绝原因与来源诊断")
    replay.add_argument("--capture-root", type=Path, required=True)
    replay.add_argument("--name-root", type=Path, help="可选的单代码完整历史名称来源")
    replay.add_argument("--selection", choices=("all", "gem", "hs300", "zz1000"), default="all")
    archive = commands.add_parser("archive", help="按现有成员归档合同发布完整日期")
    archive.add_argument("--capture-root", type=Path, required=True)
    archive.add_argument("--name-root", type=Path, help="可选的单代码完整历史名称来源")
    archive.add_argument("--selection", choices=("all", "gem", "hs300", "zz1000"), required=True)
    archive.add_argument("--as-of", type=_cli_time, required=True)
    archive.add_argument("--input-root", type=Path, required=True)
    archive.add_argument("--archive-root", type=Path, required=True)
    archive.add_argument("--date", dest="days", action="append", type=_cli_day)
    args = parser.parse_args(argv)
    try:
        if args.command == "live":
            output = collect_security_sources(
                SecurityCaptureRequest(
                    root=args.root,
                    trading_days=tuple(args.days),
                    max_calls=args.max_calls,
                    exchanges=("",) if args.all_exchanges else EXCHANGES,
                ),
                adapter_factory=_live_adapter,
            )
            print(_bytes(output).decode())
        elif args.command == "import-probes":
            print(_bytes(import_security_probes(args.sources, root=args.root)).decode())
        elif args.command == "replay":
            results = []
            failed = False
            for result in iter_security_collection_days(
                args.capture_root, selection=args.selection, name_root=args.name_root
            ):
                failed = failed or not result.accepted
                results.append(
                    {
                        "trade_date": result.trade_date.isoformat(),
                        "accepted": result.accepted,
                        "reason": result.reason,
                        "security_count": None if result.batch is None else len(result.batch.facts),
                        "observed_at": None
                        if result.batch is None
                        else result.batch.observed_at.isoformat(),
                        "source_sha256": None
                        if result.batch is None
                        else result.batch.source_sha256,
                        "diagnostics": [
                            issue.model_dump(mode="json") for issue in result.diagnostics
                        ],
                    }
                )
            print(canonical_json_bytes({"days": results}).decode())
            return 2 if failed else 0
        else:
            reference = archive_security_collection(
                args.capture_root,
                selection=args.selection,
                as_of=args.as_of,
                input_root=args.input_root,
                root=args.archive_root,
                trading_days=None if args.days is None else tuple(args.days),
                name_root=args.name_root,
            )
            print(
                canonical_json_bytes(
                    {
                        "archive_root": str(args.archive_root),
                        "reference": reference.model_dump(mode="json"),
                    }
                ).decode()
            )
        return 0
    except (ValueError, OSError, RuntimeError) as exc:
        reason = str(exc)
        if args.command == "live":
            if isinstance(exc, SourceQuotaExhaustedError):
                reason = "调用预算已用完，采集未完成"
            elif isinstance(exc, FileExistsError):
                reason = "目标目录已存在，请指定新建私有目录"
            else:
                reason = "历史股票资料采集未完成，请检查配置与来源状态"
        print(
            canonical_json_bytes(
                {"status": "refused", "reason": reason, "error_type": type(exc).__name__}
            ).decode(),
            file=sys.stderr,
        )
        return 2
    except KeyboardInterrupt:
        print('{"status":"interrupted","reason":"采集已中断，未生成完成回执"}', file=sys.stderr)
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
