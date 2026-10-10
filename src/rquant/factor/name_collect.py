"""Bounded single-code name history capture and pure historical name interpretation.

Run ``python -m rquant.factor.name_collect --help`` for explicit live capture or
offline probe import. Ordinary imports do not create settings or provider clients.
"""

from __future__ import annotations

import argparse
import hashlib
import math
import os
import sys
import time
from collections.abc import Callable, Iterator, Sequence
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Literal, Protocol

import pandas as pd
from pydantic import BaseModel, ConfigDict, Field, model_validator

from rquant.factor.member_archive import _read_file
from rquant.factor.result_artifact import _open_private_root, _require_same_root, _root_path
from rquant.factor.security_collect import (
    _CHINA,
    _MODEL,
    _STOCK_CODE,
    MAX_CAPTURE_CALLS,
    MAX_COLLECTION_BYTES,
    RawSecurityTable,
    SecuritySourceDiagnostic,
    _bytes,
    _CaptureBudget,
    _issue,
    _new_root,
    _publish_collection,
    _raw_cell,
    _reject_interrupted_collection,
    _write_new,
)
from rquant.factor.universe import ObservedTime, Sha256
from rquant.security_status import normalize_name, normalize_namechange_history
from rquant.source_quota_store import SourceQuotaExhaustedError
from rquant.source_quota_transport import SourceTransportObserver
from rquant.strict_json import canonical_json_bytes, strict_canonical_json_loads, strict_json_loads

NAME_HISTORY_FIELDS = ("ts_code", "name", "start_date", "end_date", "ann_date", "change_reason")
MAX_NAME_CODES = 16
MAX_NAME_ROWS = 2000
MAX_NAME_BYTES = 1024 * 1024


def _validate_codes(codes: tuple[str, ...]) -> None:
    if tuple(sorted(set(codes))) != codes or any(not _STOCK_CODE.fullmatch(code) for code in codes):
        raise ValueError("name codes must be explicit, unique, ascending A-share codes")


class NameSourceRequest(BaseModel):
    model_config = _MODEL

    api_name: Literal["namechange"] = "namechange"
    ts_code: str
    fields: tuple[str, ...] = NAME_HISTORY_FIELDS

    @model_validator(mode="after")
    def _scope(self) -> NameSourceRequest:
        _validate_codes((self.ts_code,))
        if self.fields != NAME_HISTORY_FIELDS:
            raise ValueError("namechange requires the complete explicit name history fields")
        return self


class CapturedNameResponse(BaseModel):
    model_config = _MODEL

    schema_version: Literal[1] = 1
    request: NameSourceRequest
    requested_at: ObservedTime
    observed_at: ObservedTime
    response: RawSecurityTable
    response_sha256: Sha256

    @model_validator(mode="after")
    def _binding(self) -> CapturedNameResponse:
        if self.requested_at > self.observed_at:
            raise ValueError("name response observation precedes request")
        fields = self.response.fields
        if len(set(fields)) != len(fields) or not set(NAME_HISTORY_FIELDS) <= set(fields):
            raise ValueError("name response is missing required columns or has duplicate columns")
        if len(self.response.items) > MAX_NAME_ROWS:
            raise ValueError("name response exceeds local row resource budget")
        code_index = fields.index("ts_code")
        if any(row[code_index] != self.request.ts_code for row in self.response.items):
            raise ValueError("name response contains a code outside its explicit request")
        if self.response_sha256 != hashlib.sha256(_bytes(self.response)).hexdigest():
            raise ValueError("name raw response digest differs")
        if len(_bytes(self)) > MAX_NAME_BYTES:
            raise ValueError("name capture exceeds local byte resource budget")
        return self


class NameCaptureRequest(BaseModel):
    model_config = _MODEL

    root: Path
    stock_codes: tuple[str, ...] = Field(min_length=1, max_length=MAX_NAME_CODES)
    max_calls: int = Field(ge=1, le=MAX_CAPTURE_CALLS)

    @model_validator(mode="after")
    def _scope(self) -> NameCaptureRequest:
        _root_path(self.root)
        _validate_codes(self.stock_codes)
        if self.max_calls < len(self.stock_codes):
            raise ValueError("name call budget cannot cover the explicitly requested codes")
        return self


def _filename(request: NameSourceRequest) -> str:
    return f"namechange-{request.ts_code}.json"


class NameCaptureReference(BaseModel):
    model_config = _MODEL

    request: NameSourceRequest
    filename: str
    sha256: Sha256
    byte_count: int = Field(gt=0, le=MAX_NAME_BYTES)
    row_count: int = Field(ge=0, le=MAX_NAME_ROWS)
    observed_at: ObservedTime

    @model_validator(mode="after")
    def _filename(self) -> NameCaptureReference:
        if self.filename != _filename(self.request):
            raise ValueError("name capture filename differs from its request")
        return self


class NameCollectionManifest(BaseModel):
    model_config = _MODEL

    schema_version: Literal[1] = 1
    status: Literal["captured"] = "captured"
    stock_codes: tuple[str, ...] = Field(min_length=1, max_length=MAX_NAME_CODES)
    responses: tuple[NameCaptureReference, ...] = Field(min_length=1, max_length=MAX_NAME_CODES)
    actual_call_count: int | None = Field(default=None, ge=0, le=MAX_CAPTURE_CALLS)

    @model_validator(mode="after")
    def _schedule(self) -> NameCollectionManifest:
        _validate_codes(self.stock_codes)
        if tuple(ref.request.ts_code for ref in self.responses) != self.stock_codes:
            raise ValueError("name collection references differ from explicit code schedule")
        if self.actual_call_count is not None and self.actual_call_count < len(self.responses):
            raise ValueError("name collection dispatch count is less than response count")
        return self


class NameCaptureAdapter(Protocol):
    def namechange_history_raw(self, *, ts_code: str) -> pd.DataFrame: ...


def make_name_capture(
    request: NameSourceRequest,
    frame: pd.DataFrame,
    *,
    requested_at: datetime,
    observed_at: datetime,
) -> CapturedNameResponse:
    if not isinstance(frame, pd.DataFrame):
        raise ValueError("name provider response is not a table")
    if len(frame) > MAX_NAME_ROWS:
        raise ValueError("name response exceeds local row resource budget")
    table = RawSecurityTable(
        fields=tuple(frame.columns),
        items=tuple(
            tuple(_raw_cell(value) for value in row)
            for row in frame.itertuples(index=False, name=None)
        ),
    )
    return CapturedNameResponse(
        request=request,
        requested_at=requested_at,
        observed_at=observed_at,
        response=table,
        response_sha256=hashlib.sha256(_bytes(table)).hexdigest(),
    )


def _save_capture(
    root: Path, descriptor: int, capture: CapturedNameResponse
) -> NameCaptureReference:
    data = _bytes(capture)
    reference = NameCaptureReference(
        request=capture.request,
        filename=_filename(capture.request),
        sha256=hashlib.sha256(data).hexdigest(),
        byte_count=len(data),
        row_count=len(capture.response.items),
        observed_at=capture.observed_at,
    )
    _write_new(root, descriptor, reference.filename, data)
    return reference


def _record_interruption(
    root: Path, descriptor: int, exc: BaseException, completed: int, calls: int | None
) -> None:
    _write_new(
        root,
        descriptor,
        "interrupted.json",
        canonical_json_bytes(
            {
                "schema_version": 1,
                "status": "interrupted",
                "error_type": type(exc).__name__,
                "completed_response_count": completed,
                "actual_call_count": calls,
            }
        ),
    )


def collect_name_sources(
    request: NameCaptureRequest,
    *,
    adapter_factory: Callable[[SourceTransportObserver], NameCaptureAdapter],
    clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    request_interval_seconds: float = 0.35,
    sleep: Callable[[float], None] = time.sleep,
) -> NameCollectionManifest:
    """Call only explicit codes, counting retries, retaining one current response."""
    request = NameCaptureRequest.model_validate(request)
    if not math.isfinite(request_interval_seconds) or request_interval_seconds < 0:
        raise ValueError("name request interval must be finite and nonnegative")
    budget = _CaptureBudget(request.max_calls, api_names=frozenset({"namechange"}))
    descriptor = _new_root(request.root)
    references: list[NameCaptureReference] = []
    try:
        adapter = adapter_factory(budget)
        for code in request.stock_codes:
            requested_at = clock()
            frame = adapter.namechange_history_raw(ts_code=code)
            observed_at = clock()
            capture = make_name_capture(
                NameSourceRequest(ts_code=code),
                frame,
                requested_at=requested_at,
                observed_at=observed_at,
            )
            references.append(_save_capture(request.root, descriptor, capture))
            del frame, capture
            sleep(request_interval_seconds)
        manifest = NameCollectionManifest(
            stock_codes=request.stock_codes,
            responses=tuple(references),
            actual_call_count=budget.actual_call_count,
        )
        _publish_collection(request.root, descriptor, manifest)
        return manifest
    except BaseException as exc:
        _record_interruption(
            request.root, descriptor, exc, len(references), budget.actual_call_count
        )
        raise
    finally:
        os.close(descriptor)


def load_name_capture_collection(root: Path) -> NameCollectionManifest:
    root = _root_path(root)
    descriptor = _open_private_root(root)
    try:
        _reject_interrupted_collection(descriptor)
        data, _ = _read_file(descriptor, "collection.json", MAX_COLLECTION_BYTES)
        strict_canonical_json_loads(data)
        manifest = NameCollectionManifest.model_validate_json(data)
        _reject_interrupted_collection(descriptor)
        _require_same_root(root, descriptor)
        return manifest
    finally:
        os.close(descriptor)


def iter_name_captures(root: Path) -> Iterator[CapturedNameResponse]:
    manifest = load_name_capture_collection(root)
    descriptor = _open_private_root(root)
    try:
        for reference in manifest.responses:
            _reject_interrupted_collection(descriptor)
            data, _ = _read_file(descriptor, reference.filename, MAX_NAME_BYTES, reference.sha256)
            if len(data) != reference.byte_count:
                raise ValueError("name capture byte count differs")
            strict_canonical_json_loads(data)
            capture = CapturedNameResponse.model_validate_json(data)
            if (
                capture.request != reference.request
                or capture.observed_at != reference.observed_at
                or len(capture.response.items) != reference.row_count
            ):
                raise ValueError("name capture differs from its collection reference")
            _require_same_root(root, descriptor)
            _reject_interrupted_collection(descriptor)
            yield capture
            del data, capture
    finally:
        os.close(descriptor)


class _ProbeRecord(BaseModel):
    model_config = ConfigDict(extra="ignore")

    api_name: Literal["namechange"]
    params: dict[str, str]
    fields: tuple[str, ...]
    requested_at: ObservedTime
    observed_at: ObservedTime
    status: Literal["response_archived"]
    filename: str
    payload_sha256: Sha256
    row_count: int = Field(ge=0, le=MAX_NAME_ROWS)

    @model_validator(mode="after")
    def _scope(self) -> _ProbeRecord:
        if set(self.params) != {"ts_code"} or self.fields != NAME_HISTORY_FIELDS:
            raise ValueError("name probe is filtered or differs from full-history request contract")
        return self


def import_name_probes(source_roots: Sequence[Path], *, root: Path) -> NameCollectionManifest:
    """Import only full single-code namechange records, retaining original receipt times."""
    if not source_roots or len(source_roots) > MAX_NAME_CODES:
        raise ValueError("name probe import requires one to sixteen explicit source directories")
    descriptor = _new_root(root)
    references: list[NameCaptureReference] = []
    try:
        for source in source_roots:
            source = _root_path(source)
            with (source / "metadata.json").open("rb") as metadata_file:
                data = metadata_file.read(MAX_COLLECTION_BYTES + 1)
            if len(data) > MAX_COLLECTION_BYTES:
                raise ValueError("name probe metadata exceeds local byte resource budget")
            metadata = strict_json_loads(data)
            if not isinstance(metadata, dict):
                raise ValueError("name probe metadata is not an object")
            records = metadata.get("requests", [metadata])
            if not isinstance(records, list):
                raise ValueError("name probe requests are not a list")
            for raw_record in records:
                if not isinstance(raw_record, dict) or raw_record.get("api_name") != "namechange":
                    continue
                if len(references) >= MAX_NAME_CODES:
                    raise ValueError("name probe import exceeds explicit code budget")
                record = _ProbeRecord.model_validate(raw_record)
                request = NameSourceRequest(ts_code=record.params["ts_code"], fields=record.fields)
                if any(ref.request == request for ref in references):
                    raise ValueError("duplicate name probe code")
                if Path(record.filename).name != record.filename or record.filename in (".", ".."):
                    raise ValueError("name probe filename is not a direct child")
                with (source / record.filename).open("rb") as raw_file:
                    data = raw_file.read(MAX_NAME_BYTES + 1)
                if (
                    len(data) > MAX_NAME_BYTES
                    or hashlib.sha256(data).hexdigest() != record.payload_sha256
                ):
                    raise ValueError("name probe raw digest or byte count differs")
                strict_canonical_json_loads(data)
                table = RawSecurityTable.model_validate_json(data)
                if len(table.items) != record.row_count:
                    raise ValueError("name probe row count differs")
                capture = CapturedNameResponse(
                    request=request,
                    requested_at=record.requested_at,
                    observed_at=record.observed_at,
                    response=table,
                    response_sha256=record.payload_sha256,
                )
                references.append(_save_capture(root, descriptor, capture))
                del data, table, capture
        references.sort(key=lambda ref: ref.request.ts_code)
        manifest = NameCollectionManifest(
            stock_codes=tuple(ref.request.ts_code for ref in references),
            responses=tuple(references),
        )
        _publish_collection(root, descriptor, manifest)
        return manifest
    except BaseException as exc:
        _record_interruption(root, descriptor, exc, len(references), None)
        raise
    finally:
        os.close(descriptor)


class NameDayEvidence(BaseModel):
    model_config = _MODEL

    ts_code: str
    name: str | None
    is_st: bool | None
    diagnostics: tuple[SecuritySourceDiagnostic, ...] = ()


def resolve_name_day(capture: CapturedNameResponse, day: date) -> NameDayEvidence:
    """Interpret an effective interval only; production status boundary rules remain separate."""
    capture = CapturedNameResponse.model_validate(capture)
    code = capture.request.ts_code
    diagnostic = None
    history = normalize_namechange_history(
        pd.DataFrame(capture.response.items, columns=capture.response.fields)
    )
    if capture.observed_at.astimezone(_CHINA).date() < day:
        diagnostic = _issue(
            "name_source_before_trade_date", "name response predates trade date", code
        )
    elif history.issues:
        diagnostic = _issue(
            "invalid_name_history", "; ".join(issue.reason for issue in history.issues), code
        )
    else:
        covering = tuple(
            row
            for row in history.intervals
            if row.start_date <= day and (row.end_date is None or day <= row.end_date)
        )
        if len(covering) > 1:
            diagnostic = _issue("name_interval_overlap", "multiple effective name intervals", code)
        elif not covering:
            diagnostic = _issue(
                "name_interval_missing", "no effective name interval", code, info=True
            )
        else:
            name, is_st = normalize_name(covering[0].name)
            return NameDayEvidence(ts_code=code, name=name, is_st=is_st)
    return NameDayEvidence(ts_code=code, name=None, is_st=None, diagnostics=(diagnostic,))


def _live_adapter(observer: SourceTransportObserver) -> NameCaptureAdapter:
    from rquant.adapter.tushare import TushareAdapter

    return TushareAdapter(backup_token="", transport_observer=observer)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="单代码完整历史名称采集与离线导入")
    commands = parser.add_subparsers(dest="command", required=True)
    live = commands.add_parser("live", help="显式使用已有主token配置，只写新建私有目录")
    live.add_argument("--code", dest="codes", action="append", required=True)
    live.add_argument("--root", type=Path, required=True)
    live.add_argument("--max-calls", type=int, required=True)
    probe = commands.add_parser("import-probes", help="只导入完整单代码namechange实际来源")
    probe.add_argument("--source-root", dest="sources", action="append", type=Path, required=True)
    probe.add_argument("--root", type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        output = (
            collect_name_sources(
                NameCaptureRequest(
                    root=args.root, stock_codes=tuple(args.codes), max_calls=args.max_calls
                ),
                adapter_factory=_live_adapter,
            )
            if args.command == "live"
            else import_name_probes(args.sources, root=args.root)
        )
        print(_bytes(output).decode())
        return 0
    except (ValueError, OSError, RuntimeError) as exc:
        reason = str(exc)
        if args.command == "live":
            if isinstance(exc, SourceQuotaExhaustedError):
                reason = "调用预算已用完，名称采集未完成"
            elif isinstance(exc, FileExistsError):
                reason = "目标目录已存在，请指定新建私有目录"
            else:
                reason = "历史名称采集未完成，请检查配置与来源状态"
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
