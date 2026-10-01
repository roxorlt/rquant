"""Bounded official CSI daily sample capture, replay and existing member archive.

Only dates inside the actual XLS are admitted. Explicit ``live`` is the only
network entry; ordinary imports and receipt replay never initialize settings.
"""

from __future__ import annotations

import argparse
import hashlib
import io
import os
import re
import ssl
import struct
import sys
import urllib.request
from collections.abc import Callable, Iterator, Sequence
from contextlib import closing
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Literal

import xlrd
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator
from xlrd.compdoc import CompDocError

from rquant.factor.history_assemble import (
    MAX_CAPTURE_ROOTS,
    _check_collection,
    _CollectionBinding,
    _day_binding,
    _DayBinding,
    _snapshot_collection,
)
from rquant.factor.member_archive import (
    MAX_FACTOR_MEMBER_MANIFEST_BYTES,
    FactorMemberArchiveReference,
    FactorMemberArchiveRequest,
    FactorMemberDayInput,
    _check_identities,
    _read_file,
    publish_factor_member_archive,
)
from rquant.factor.name_collect import _record_interruption, load_name_capture_collection
from rquant.factor.result_artifact import (
    _cleanup_owned_temporary,
    _file_identity,
    _open_private_root,
    _require_same_root,
    _root_identity,
    _root_path,
)
from rquant.factor.security_collect import (
    _CHINA,
    _MODEL,
    MAX_COLLECTION_BYTES,
    _bytes,
    _cli_day,
    _cli_time,
    _new_root,
    _publish_collection,
    _reject_interrupted_collection,
    _validate_days,
    _write_new,
    iter_security_collection_days,
    load_security_capture_collection,
)
from rquant.factor.time_series import MAX_TRADE_DAYS
from rquant.factor.universe import (
    MAX_UNIVERSE_SECURITIES,
    DailyIndexConstituentBatch,
    FactorUniverseRequest,
    ObservedTime,
    Sha256,
    StockCode,
    select_factor_universe,
)
from rquant.strict_json import canonical_json_bytes, strict_canonical_json_loads, strict_json_loads

IndexSelection = Literal["hs300", "zz1000"]
MAX_INDEX_BYTES = 4 * 1024 * 1024
MAX_RECEIPT_BYTES = 64 * 1024
MAX_INDEX_CALLS = 4
_BASE = "https://oss-ch.csindex.com.cn/static/html/csindex/public/uploads/file/autofile/cons/"
_INDICES = {
    "hs300": ("000300", "沪深300", "CSI 300", 300),
    "zz1000": ("000852", "中证1000", "CSI 1000", 1000),
}
_URLS = {selection: _BASE + values[0] + "cons.xls" for selection, values in _INDICES.items()}
_HEADERS = (
    "日期Date",
    "指数代码 Index Code",
    "指数名称 Index Name",
    "指数英文名称Index Name(Eng)",
    "成份券代码Constituent Code",
    "成份券名称Constituent Name",
    "成份券英文名称Constituent Name(Eng)",
    "交易所Exchange",
    "交易所英文名称Exchange(Eng)",
)
_HTTP_HEADERS = ("Content-Type", "Content-Length", "Last-Modified", "ETag")
_EXCHANGES = {
    "上海证券交易所": ("Shanghai Stock Exchange", "SH", r"6[0-9]{5}"),
    "深圳证券交易所": ("Shenzhen Stock Exchange", "SZ", r"[03][0-9]{5}"),
}


def _headers(value: dict[str, str]) -> dict[str, str]:
    if any(
        key not in _HTTP_HEADERS or len(text) > 1024 or not text.isprintable()
        for key, text in value.items()
    ):
        raise ValueError("官方HTTP回执响应头不符合有界字段合同")
    return value


class IndexSourceRequest(BaseModel):
    model_config = _MODEL

    selection: IndexSelection
    url: str

    @model_validator(mode="after")
    def _scope(self) -> IndexSourceRequest:
        if self.url != _URLS[self.selection]:
            raise ValueError("指数请求必须使用固定官方样本URL")
        return self


class IndexHttpResponse(BaseModel):
    model_config = _MODEL

    data: bytes = Field(min_length=1, max_length=MAX_INDEX_BYTES)
    http_status: int
    final_url: str
    headers: dict[str, str] = Field(max_length=4)

    _bounded_headers = field_validator("headers")(_headers)


class IndexHttpReceipt(BaseModel):
    # Existing public receipts also retain diagnostic fields; their exact bytes are saved.
    model_config = ConfigDict(
        extra="ignore", strict=True, frozen=True, revalidate_instances="always"
    )

    url: str
    requested_at: ObservedTime
    received_at: ObservedTime
    status: Literal["received"] = "received"
    http_status: Literal[200]
    final_url: str | None = None
    headers: dict[str, str] = Field(max_length=4)
    bytes: int = Field(gt=0, le=MAX_INDEX_BYTES)
    sha256: Sha256
    raw_file: str = Field(min_length=1, max_length=4096)

    _bounded_headers = field_validator("headers")(_headers)

    @model_validator(mode="after")
    def _binding(self) -> IndexHttpReceipt:
        if self.url not in _URLS.values() or (
            self.final_url is not None and self.final_url != self.url
        ):
            raise ValueError("HTTP回执不是固定官方请求或最终URL不符")
        if self.requested_at > self.received_at:
            raise ValueError("HTTP实收时刻早于请求时刻")
        length = self.headers.get("Content-Length")
        if length is not None and (
            re.fullmatch(r"[0-9]+", length) is None or int(length) != self.bytes
        ):
            raise ValueError("HTTP Content-Length与实际原件字节数不符")
        return self


class IndexCaptureRequest(BaseModel):
    model_config = _MODEL

    root: Path
    selections: tuple[IndexSelection, ...] = Field(min_length=1, max_length=2)
    max_calls: int = Field(default=2, ge=1, le=MAX_INDEX_CALLS)
    timeout_seconds: float = Field(default=20.0, gt=0, le=60, allow_inf_nan=False)

    @model_validator(mode="after")
    def _scope(self) -> IndexCaptureRequest:
        _root_path(self.root)
        if len(set(self.selections)) != len(self.selections) or self.max_calls < len(
            self.selections
        ):
            raise ValueError("指数请求重复或调用预算不足")
        return self


class IndexCaptureReference(BaseModel):
    model_config = _MODEL

    selection: IndexSelection
    filename: str
    sha256: Sha256
    byte_count: int = Field(gt=0, le=MAX_RECEIPT_BYTES)
    raw_filename: str
    raw_sha256: Sha256
    raw_byte_count: int = Field(gt=0, le=MAX_INDEX_BYTES)
    trade_date: date
    observed_at: ObservedTime

    @model_validator(mode="after")
    def _names(self) -> IndexCaptureReference:
        if (
            self.filename != f"index-{self.selection}.receipt.json"
            or self.raw_filename != f"index-{self.selection}.xls"
        ):
            raise ValueError("指数原件与回执文件名不符合固定合同")
        _validate_days((self.trade_date,))
        return self


class IndexCollectionManifest(BaseModel):
    model_config = _MODEL

    schema_version: Literal[1] = 1
    status: Literal["captured"] = "captured"
    capture_mode: Literal["live", "imported_receipts"]
    selections: tuple[IndexSelection, ...] = Field(min_length=1, max_length=2)
    responses: tuple[IndexCaptureReference, ...] = Field(min_length=1, max_length=2)
    actual_call_count: int = Field(ge=0, le=MAX_INDEX_CALLS)

    @model_validator(mode="after")
    def _complete(self) -> IndexCollectionManifest:
        if (
            len(set(self.selections)) != len(self.selections)
            or tuple(ref.selection for ref in self.responses) != self.selections
        ):
            raise ValueError("指数完成回执缺少请求响应或重复")
        expected = len(self.responses) if self.capture_mode == "live" else 0
        if self.actual_call_count != expected:
            raise ValueError("指数完成回执实际dispatch计数不符")
        return self


def normalize_index_response(
    request: IndexSourceRequest,
    receipt: IndexHttpReceipt,
    data: bytes,
    *,
    trade_date: date | None = None,
    as_of: datetime | None = None,
) -> DailyIndexConstituentBatch:
    """Interpret one complete sample date, retaining actual bytes and receipt time."""
    request = IndexSourceRequest.model_validate(request)
    receipt = IndexHttpReceipt.model_validate(receipt)
    if (
        receipt.url != request.url
        or len(data) != receipt.bytes
        or hashlib.sha256(data).hexdigest() != receipt.sha256
    ):
        raise ValueError("官方原件摘要、字节数或请求绑定不符")
    if as_of is not None and (as_of.tzinfo is None or receipt.received_at > as_of):
        raise ValueError("指数实际实收时刻晚于截止时刻或截止时刻无时区")
    expected_code, expected_name, expected_english, expected_count = _INDICES[request.selection]
    book = None
    try:
        book = xlrd.open_workbook(file_contents=data, on_demand=True, logfile=io.StringIO())
        if book.nsheets != 1:
            raise ValueError("官方日样本必须只有一张样本表")
        sheet = book.sheet_by_index(0)
        if (
            sheet.ncols != 9
            or sheet.nrows != expected_count + 1
            or tuple(sheet.row_values(0)) != _HEADERS
        ):
            raise ValueError("官方样本表头或完整成员行数不符")
        codes: list[str] = []
        sample_date = None
        for row_number in range(1, sheet.nrows):
            values = sheet.row_values(row_number)
            if any(
                sheet.cell_type(row_number, column) != xlrd.XL_CELL_TEXT for column in range(9)
            ) or any(not value.strip() or not value.isprintable() for value in values):
                raise ValueError(f"官方样本第{row_number}行不是完整文本字段")
            if re.fullmatch(r"[0-9]{8}", values[0]) is None:
                raise ValueError(f"官方样本第{row_number}行日期格式不符")
            day = datetime.strptime(values[0], "%Y%m%d").date()
            if sample_date is None:
                sample_date = day
            if day != sample_date or tuple(values[1:4]) != (
                expected_code,
                expected_name,
                expected_english,
            ):
                raise ValueError(f"官方样本第{row_number}行日期或指数标识冲突")
            exchange = _EXCHANGES.get(values[7])
            if (
                exchange is None
                or values[8] != exchange[0]
                or re.fullmatch(exchange[2], values[4]) is None
            ):
                raise ValueError(f"官方样本第{row_number}行代码或交易所不符")
            codes.append(values[4] + "." + exchange[1])
        if len(set(codes)) != expected_count:
            raise ValueError("官方完整成员名单包含重复证券")
        _validate_days((sample_date,))
        if trade_date is not None and trade_date != sample_date:
            raise ValueError(f"官方样本仅覆盖{sample_date}，缺少请求日期{trade_date}")
        if sample_date > receipt.received_at.astimezone(_CHINA).date():
            raise ValueError("官方样本日期晚于实际接收日期")
        return DailyIndexConstituentBatch(
            selection=request.selection,
            trade_date=sample_date,
            source_id=f"csindex:official-cons:{expected_code}:xls:v1",
            source_sha256=receipt.sha256,
            source_mode="historical_retrospective",
            source_kind="daily_complete_membership",
            observed_at=receipt.received_at,
            stock_codes=tuple(codes),
        )
    except (xlrd.XLRDError, CompDocError, struct.error, IndexError, UnicodeError) as exc:
        raise ValueError("官方样本XLS原件不可解析") from exc
    finally:
        if book is not None:
            book.release_resources()


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(
        self,
        req: urllib.request.Request,
        fp: object,
        code: int,
        msg: str,
        headers: object,
        newurl: str,
    ) -> None:
        return None


def _https_get(
    request: IndexSourceRequest, timeout_seconds: float, max_bytes: int
) -> IndexHttpResponse:
    opener = urllib.request.build_opener(
        urllib.request.HTTPSHandler(context=ssl.create_default_context()), _NoRedirect()
    )
    http_request = urllib.request.Request(
        request.url, headers={"Accept": "application/vnd.ms-excel"}
    )
    with opener.open(http_request, timeout=timeout_seconds) as response:
        data = response.read(max_bytes + 1)
        return IndexHttpResponse(
            data=data,
            http_status=response.status,
            final_url=response.geturl(),
            headers={
                name: response.headers[name] for name in _HTTP_HEADERS if name in response.headers
            },
        )


def _save_index(
    root: Path,
    descriptor: int,
    selection: IndexSelection,
    data: bytes,
    receipt_data: bytes,
    receipt: IndexHttpReceipt,
) -> IndexCaptureReference:
    batch = normalize_index_response(
        IndexSourceRequest(selection=selection, url=_URLS[selection]), receipt, data
    )
    if len(receipt_data) > MAX_RECEIPT_BYTES:
        raise ValueError("指数HTTP回执超过本地字节上限")
    raw_filename = f"index-{selection}.xls"
    filename = f"index-{selection}.receipt.json"
    reference = IndexCaptureReference(
        selection=selection,
        filename=filename,
        sha256=hashlib.sha256(receipt_data).hexdigest(),
        byte_count=len(receipt_data),
        raw_filename=raw_filename,
        raw_sha256=receipt.sha256,
        raw_byte_count=len(data),
        trade_date=batch.trade_date,
        observed_at=batch.observed_at,
    )
    _write_new(root, descriptor, raw_filename, data)
    _write_new(root, descriptor, filename, receipt_data)
    return reference


def collect_index_sources(
    request: IndexCaptureRequest,
    *,
    transport: Callable[[IndexSourceRequest, float, int], IndexHttpResponse] = _https_get,
    clock: Callable[[], datetime] = lambda: datetime.now(UTC),
) -> IndexCollectionManifest:
    """One dispatch per selected official URL, no implicit retry or redirect."""
    request = IndexCaptureRequest.model_validate(request)
    descriptor = _new_root(request.root)
    references: list[IndexCaptureReference] = []
    calls = 0
    try:
        for selection in request.selections:
            if calls >= request.max_calls:
                raise ValueError("指数实际dispatch超过本次预算")
            source = IndexSourceRequest(selection=selection, url=_URLS[selection])
            requested_at = clock()
            calls += 1
            response = IndexHttpResponse.model_validate(
                transport(source, request.timeout_seconds, MAX_INDEX_BYTES)
            )
            receipt = IndexHttpReceipt(
                url=source.url,
                requested_at=requested_at,
                received_at=clock(),
                http_status=response.http_status,
                final_url=response.final_url,
                headers=response.headers,
                bytes=len(response.data),
                sha256=hashlib.sha256(response.data).hexdigest(),
                raw_file=f"index-{selection}.xls",
            )
            references.append(
                _save_index(
                    request.root, descriptor, selection, response.data, _bytes(receipt), receipt
                )
            )
            del response, receipt
        manifest = IndexCollectionManifest(
            capture_mode="live",
            selections=request.selections,
            responses=tuple(references),
            actual_call_count=calls,
        )
        _publish_collection(request.root, descriptor, manifest)
        return manifest
    except BaseException as exc:
        _record_interruption(request.root, descriptor, exc, len(references), calls)
        raise
    finally:
        os.close(descriptor)


def import_index_receipts(receipt_files: Sequence[Path], *, root: Path) -> IndexCollectionManifest:
    """Copy existing raw receipts unchanged; do not invent receive times or final URLs."""
    if not 1 <= len(receipt_files) <= 2 or len(set(receipt_files)) != len(receipt_files):
        raise ValueError("指数回执导入需要一至两个不同的显式原件回执")
    descriptor = _new_root(root)
    references: list[IndexCaptureReference] = []
    try:
        for path in receipt_files:
            _root_path(path)
            if not path.name.endswith(".receipt.json"):
                raise ValueError("指数导入回执必须明确对应同目录XLS原件")
            with path.open("rb") as incoming:
                receipt_data = incoming.read(MAX_RECEIPT_BYTES + 1)
            if len(receipt_data) > MAX_RECEIPT_BYTES:
                raise ValueError("指数导入回执超过本地字节上限")
            strict_json_loads(receipt_data)
            receipt = IndexHttpReceipt.model_validate_json(receipt_data)
            raw_path = path.with_name(path.name.removesuffix(".receipt.json"))
            if receipt.raw_file not in (str(raw_path), raw_path.name):
                raise ValueError("指数原始回执与显式原件路径绑定不符")
            selection = next(key for key, url in _URLS.items() if url == receipt.url)
            if any(ref.selection == selection for ref in references):
                raise ValueError("导入的官方指数回执重复")
            with raw_path.open("rb") as incoming:
                data = incoming.read(MAX_INDEX_BYTES + 1)
            references.append(_save_index(root, descriptor, selection, data, receipt_data, receipt))
            del data, receipt_data, receipt
        manifest = IndexCollectionManifest(
            capture_mode="imported_receipts",
            selections=tuple(ref.selection for ref in references),
            responses=tuple(references),
            actual_call_count=0,
        )
        _publish_collection(root, descriptor, manifest)
        return manifest
    except BaseException as exc:
        _record_interruption(root, descriptor, exc, len(references), 0)
        raise
    finally:
        os.close(descriptor)


def _read_index(
    descriptor: int, reference: IndexCaptureReference, mode: str, *, as_of: datetime | None = None
) -> DailyIndexConstituentBatch:
    receipt_data, _ = _read_file(
        descriptor, reference.filename, MAX_RECEIPT_BYTES, reference.sha256
    )
    strict_json_loads(receipt_data)
    receipt = IndexHttpReceipt.model_validate_json(receipt_data)
    if (
        len(receipt_data) != reference.byte_count
        or receipt.received_at != reference.observed_at
        or receipt.sha256 != reference.raw_sha256
        or receipt.bytes != reference.raw_byte_count
    ):
        raise ValueError("指数HTTP回执与完成绑定不符")
    if mode == "live" and receipt.final_url is None:
        raise ValueError("live指数回执缺少实际最终URL")
    data, _ = _read_file(descriptor, reference.raw_filename, MAX_INDEX_BYTES, reference.raw_sha256)
    return normalize_index_response(
        IndexSourceRequest(selection=reference.selection, url=_URLS[reference.selection]),
        receipt,
        data,
        trade_date=reference.trade_date,
        as_of=as_of,
    )


def load_index_capture_collection(root: Path) -> IndexCollectionManifest:
    descriptor = _open_private_root(_root_path(root))
    try:
        _reject_interrupted_collection(descriptor)
        data, _ = _read_file(descriptor, "collection.json", MAX_COLLECTION_BYTES)
        strict_canonical_json_loads(data)
        manifest = IndexCollectionManifest.model_validate_json(data)
        for reference in manifest.responses:
            batch = _read_index(descriptor, reference, manifest.capture_mode)
            del batch
        _require_same_root(root, descriptor)
        _reject_interrupted_collection(descriptor)
        return manifest
    finally:
        os.close(descriptor)


class _ReplayRequest(BaseModel):
    model_config = _MODEL

    root: Path
    selection: IndexSelection
    trade_date: date
    as_of: ObservedTime

    @model_validator(mode="after")
    def _scope(self) -> _ReplayRequest:
        _root_path(self.root)
        _validate_days((self.trade_date,))
        return self


def replay_index_day(
    root: Path, *, selection: IndexSelection, trade_date: date, as_of: datetime
) -> DailyIndexConstituentBatch:
    request = _ReplayRequest(root=root, selection=selection, trade_date=trade_date, as_of=as_of)
    manifest = load_index_capture_collection(root)
    reference = next((ref for ref in manifest.responses if ref.selection == selection), None)
    if reference is None or reference.trade_date != trade_date:
        raise ValueError(f"官方指数捕获缺少请求日期{trade_date}的完整{selection}名单")
    descriptor = _open_private_root(root)
    try:
        _reject_interrupted_collection(descriptor)
        result = _read_index(descriptor, reference, manifest.capture_mode, as_of=request.as_of)
        _require_same_root(root, descriptor)
        _reject_interrupted_collection(descriptor)
        return result
    finally:
        os.close(descriptor)


class IndexArchiveRequest(BaseModel):
    model_config = _MODEL

    index_roots: tuple[Path, ...] = Field(min_length=1, max_length=MAX_TRADE_DAYS)
    security_roots: tuple[Path, ...] = Field(min_length=1, max_length=MAX_CAPTURE_ROOTS)
    trading_days: tuple[date, ...] = Field(min_length=1, max_length=MAX_TRADE_DAYS)
    selection: IndexSelection
    as_of: ObservedTime
    input_root: Path
    root: Path
    name_root: Path | None = None

    @model_validator(mode="after")
    def _scope(self) -> IndexArchiveRequest:
        _validate_days(self.trading_days)
        paths = (*self.index_roots, *self.security_roots, self.input_root, self.root)
        if self.name_root is not None:
            paths += (self.name_root,)
        for path in paths:
            _root_path(path)
        if len(set(paths)) != len(paths):
            raise ValueError("指数、证券、名称、输入和归档目录必须不同且不重复")
        return self


class _IndexBinding(BaseModel):
    model_config = _MODEL

    source: _CollectionBinding
    reference: IndexCaptureReference
    capture_mode: Literal["live", "imported_receipts"]


class _ArchivePreview(BaseModel):
    model_config = _MODEL

    indices: tuple[_IndexBinding, ...] = Field(min_length=1, max_length=MAX_TRADE_DAYS)
    securities: tuple[_CollectionBinding, ...] = Field(min_length=1, max_length=MAX_CAPTURE_ROOTS)
    names: _CollectionBinding | None
    days: tuple[_DayBinding, ...] = Field(min_length=1, max_length=MAX_TRADE_DAYS)
    codes: tuple[StockCode, ...] = Field(min_length=1, max_length=MAX_UNIVERSE_SECURITIES)


def _snapshot_index(root: Path, selection: IndexSelection) -> _IndexBinding:
    manifest = load_index_capture_collection(root)
    reference = next((ref for ref in manifest.responses if ref.selection == selection), None)
    if reference is None:
        raise ValueError("指数捕获根缺少请求指数")
    descriptor = _open_private_root(root)
    try:
        data, identity = _read_file(descriptor, "collection.json", MAX_COLLECTION_BYTES)
        if IndexCollectionManifest.model_validate_json(data) != manifest:
            raise ValueError("指数完成回执在加载与绑定之间改变")
        files = [("collection.json", identity)]
        for ref in manifest.responses:
            for filename in (ref.filename, ref.raw_filename):
                files.append(
                    (
                        filename,
                        _file_identity(os.stat(filename, dir_fd=descriptor, follow_symlinks=False)),
                    )
                )
        binding = _CollectionBinding(
            root=root,
            directory_identity=_root_identity(os.fstat(descriptor)),
            manifest_sha256=hashlib.sha256(data).hexdigest(),
            files=tuple(files),
            trading_days=(reference.trade_date,),
        )
        _check_identities(root, descriptor, dict(binding.files))
        _reject_interrupted_collection(descriptor)
        return _IndexBinding(
            source=binding, reference=reference, capture_mode=manifest.capture_mode
        )
    finally:
        os.close(descriptor)


def _check_all(preview: _ArchivePreview) -> None:
    for binding in preview.indices:
        _check_collection(binding.source)
    for binding in preview.securities:
        _check_collection(binding)
    if preview.names is not None:
        _check_collection(preview.names)


def _iter_inputs(
    request: IndexArchiveRequest,
    indices: tuple[_IndexBinding, ...],
    securities: tuple[_CollectionBinding, ...],
    names: _CollectionBinding | None,
) -> Iterator[FactorMemberDayInput]:
    requested = set(request.trading_days)
    by_day = {binding.reference.trade_date: binding for binding in indices}
    for binding in securities:
        _check_collection(binding)
        if names is not None:
            _check_collection(names)
        # The existing source iterator conservatively admits an all-compatible full batch.
        with closing(
            iter_security_collection_days(
                binding.root, selection="all", name_root=request.name_root
            )
        ) as iterator:
            for result in iterator:
                if result.trade_date not in requested:
                    del result
                    continue
                if not result.accepted:
                    raise ValueError(f"{result.trade_date}: {result.reason}")
                index = by_day[result.trade_date]
                _check_collection(index.source)
                descriptor = _open_private_root(index.source.root)
                try:
                    membership = _read_index(
                        descriptor, index.reference, index.capture_mode, as_of=request.as_of
                    )
                finally:
                    os.close(descriptor)
                payload = FactorMemberDayInput(
                    schema_version=1,
                    trade_date=result.trade_date,
                    securities=result.batch,
                    membership=membership,
                )
                select_factor_universe(
                    FactorUniverseRequest(
                        selection=request.selection,
                        trade_date=payload.trade_date,
                        as_of=request.as_of,
                        securities=payload.securities,
                        membership=membership,
                    )
                )
                yield payload
                del result, membership, payload
        _check_collection(binding)
        if names is not None:
            _check_collection(names)


def _preflight(request: IndexArchiveRequest) -> _ArchivePreview:
    indices = tuple(_snapshot_index(root, request.selection) for root in request.index_roots)
    dates = [binding.reference.trade_date for binding in indices]
    if len(set(dates)) != len(dates):
        raise ValueError("官方指数捕获日期重复或冲突")
    if not set(request.trading_days) <= set(dates):
        raise ValueError("官方指数日样本缺少请求日期，不可填相邻日")
    securities: list[_CollectionBinding] = []
    covered: set[date] = set()
    for root in request.security_roots:
        manifest = load_security_capture_collection(root)
        if covered.intersection(manifest.trading_days):
            raise ValueError("证券来源日期重叠")
        covered.update(manifest.trading_days)
        securities.append(_snapshot_collection(root, manifest))
        del manifest
    if not set(request.trading_days) <= covered:
        raise ValueError("证券来源缺少请求日期")
    names = (
        None
        if request.name_root is None
        else _snapshot_collection(
            request.name_root, load_name_capture_collection(request.name_root)
        )
    )
    days: dict[date, _DayBinding] = {}
    codes: set[str] = set()
    with closing(_iter_inputs(request, indices, tuple(securities), names)) as iterator:
        for payload in iterator:
            if payload.trade_date in days:
                raise ValueError("指数归档预验日期重复")
            codes.update(payload.securities.complete_stock_codes)
            if len(codes) > MAX_UNIVERSE_SECURITIES:
                raise ValueError("证券代码并集超过7000上限")
            data = _bytes(payload)
            days[payload.trade_date] = _day_binding(payload, data)
            del payload, data
    if set(days) != set(request.trading_days):
        raise ValueError("指数归档没有完整消费请求日期")
    preview = _ArchivePreview(
        indices=indices,
        securities=tuple(securities),
        names=names,
        days=tuple(days[day] for day in request.trading_days),
        codes=tuple(sorted(codes)),
    )
    _check_all(preview)
    return preview


def _filenames(request: IndexArchiveRequest, preview: _ArchivePreview) -> Iterator[str]:
    for day in request.trading_days:
        yield f"index-security-{day:%Y%m%d}.json"
    _check_all(preview)


def archive_index_collections(request: IndexArchiveRequest) -> FactorMemberArchiveReference:
    """Preflight exact same-day sources, then publish the existing v1 member archive."""
    request = IndexArchiveRequest.model_validate(request)
    preview = _preflight(request)
    _check_all(preview)
    expected = {binding.trade_date: binding for binding in preview.days}
    written: set[date] = set()
    descriptor = _new_root(request.input_root)
    try:
        with closing(
            _iter_inputs(request, preview.indices, preview.securities, preview.names)
        ) as iterator:
            for payload in iterator:
                data = _bytes(payload)
                if payload.trade_date in written or _day_binding(payload, data) != expected.get(
                    payload.trade_date
                ):
                    raise ValueError("指数归档预验与写入日绑定不一致")
                _write_new(
                    request.input_root,
                    descriptor,
                    f"index-security-{payload.trade_date:%Y%m%d}.json",
                    data,
                )
                written.add(payload.trade_date)
                del payload, data
        if written != set(request.trading_days):
            raise ValueError("指数归档写入未完成完整日程")
        _check_all(preview)
        _require_same_root(request.input_root, descriptor)
        archive_descriptor = _new_root(request.root)
        try:
            reference = publish_factor_member_archive(
                FactorMemberArchiveRequest(
                    selection=request.selection,
                    trading_days=request.trading_days,
                    as_of=request.as_of,
                    computation_stock_codes=preview.codes,
                ),
                input_root=request.input_root,
                root=request.root,
                daily_filenames=_filenames(request, preview),
            )
            identity = _file_identity(
                os.stat(reference.filename, dir_fd=archive_descriptor, follow_symlinks=False)
            )
            try:
                data, _ = _read_file(
                    archive_descriptor,
                    reference.filename,
                    MAX_FACTOR_MEMBER_MANIFEST_BYTES,
                    reference.sha256,
                )
                del data
                _require_same_root(request.root, archive_descriptor)
                _check_all(preview)
            except BaseException:
                _cleanup_owned_temporary(archive_descriptor, reference.filename, identity[:2])
                os.fsync(archive_descriptor)
                raise
            return reference
        finally:
            os.close(archive_descriptor)
    finally:
        os.close(descriptor)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="官方CSI日样本显式采集、离线回放与成员归档")
    commands = parser.add_subparsers(dest="command", required=True)
    live = commands.add_parser("live", help="显式访问固定官方HTTPS样本URL")
    live.add_argument("--selection", action="append", choices=tuple(_INDICES), required=True)
    live.add_argument("--root", type=Path, required=True)
    live.add_argument("--max-calls", type=int, default=2)
    live.add_argument("--timeout-seconds", type=float, default=20.0)
    imported = commands.add_parser("import-receipts", help="保留既有HTTP回执和原件，不执行live")
    imported.add_argument("--receipt-file", action="append", type=Path, required=True)
    imported.add_argument("--root", type=Path, required=True)
    replay = commands.add_parser("replay", help="仅解释真实表内日期")
    replay.add_argument("--index-root", type=Path, required=True)
    replay.add_argument("--selection", choices=tuple(_INDICES), required=True)
    replay.add_argument("--date", type=_cli_day, required=True)
    replay.add_argument("--as-of", type=_cli_time, required=True)
    archive = commands.add_parser("archive", help="同日官方完整成员与已有证券源归档")
    archive.add_argument("--index-root", action="append", type=Path, required=True)
    archive.add_argument("--security-root", action="append", type=Path, required=True)
    archive.add_argument("--name-root", type=Path)
    archive.add_argument("--selection", choices=tuple(_INDICES), required=True)
    archive.add_argument("--date", action="append", type=_cli_day, required=True)
    archive.add_argument("--as-of", type=_cli_time, required=True)
    archive.add_argument("--input-root", type=Path, required=True)
    archive.add_argument("--archive-root", type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        if args.command == "live":
            result = collect_index_sources(
                IndexCaptureRequest(
                    root=args.root,
                    selections=tuple(args.selection),
                    max_calls=args.max_calls,
                    timeout_seconds=args.timeout_seconds,
                )
            )
        elif args.command == "import-receipts":
            result = import_index_receipts(tuple(args.receipt_file), root=args.root)
        elif args.command == "replay":
            result = replay_index_day(
                args.index_root, selection=args.selection, trade_date=args.date, as_of=args.as_of
            )
        else:
            reference = archive_index_collections(
                IndexArchiveRequest(
                    index_roots=tuple(args.index_root),
                    security_roots=tuple(args.security_root),
                    trading_days=tuple(args.date),
                    selection=args.selection,
                    as_of=args.as_of,
                    input_root=args.input_root,
                    root=args.archive_root,
                    name_root=args.name_root,
                )
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
        print(_bytes(result).decode())
        return 0
    except (ValueError, OSError, RuntimeError) as exc:
        print(
            canonical_json_bytes(
                {"status": "refused", "reason": str(exc), "error_type": type(exc).__name__}
            ).decode(),
            file=sys.stderr,
        )
        return 2
    except KeyboardInterrupt:
        print(
            '{"status":"interrupted","reason":"采集或归档中断，不能作为完成资料"}', file=sys.stderr
        )
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
