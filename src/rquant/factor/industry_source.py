"""Bounded SW2021 captures and retrospective daily industry interval facts."""

from __future__ import annotations

import hashlib
import os
import re
import shutil
from collections import Counter
from collections.abc import Callable, Iterator
from contextlib import contextmanager, suppress
from datetime import date, datetime
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Literal

import duckdb
import pandas as pd
from pydantic import BaseModel, Field, ValidationInfo, field_validator, model_validator

from rquant.data_metadata import DatasetSnapshotArtifact, normalize_utc_datetime, utc_now
from rquant.factor.member_archive import _check_identities, _read_file
from rquant.factor.result_artifact import (
    _cleanup_owned_temporary,
    _open_private_root,
    _require_same_root,
    _root_path,
)
from rquant.factor.security_collect import (
    _MODEL,
    RawSecurityTable,
    _bytes,
    _new_root,
    _publish_collection,
    _raw_cell,
    _reject_interrupted_collection,
    _write_new,
)
from rquant.factor.source_prepare import FactorPreparedStreamSource
from rquant.factor.universe import ObservedTime, Sha256, StockCode
from rquant.research_lake import _quoted_literal
from rquant.research_snapshot import (
    FactorComputationScope,
    materialize_table_dependency,
    verify_materialized_table_artifact,
)
from rquant.runtime_contracts import canonical_sha256
from rquant.source_quota_store import SourceQuotaAttemptOutcome
from rquant.source_quota_transport import SourceTransportUsageReceipt
from rquant.strategy_dependencies import StrategyTableDependency
from rquant.strict_json import canonical_json_bytes, strict_canonical_json_loads

INDUSTRY_DIRECTORY_FIELDS = (
    "index_code",
    "industry_name",
    "parent_code",
    "level",
    "industry_code",
    "is_pub",
    "src",
)
INDUSTRY_MEMBER_FIELDS = (
    "l1_code",
    "l1_name",
    "l2_code",
    "l2_name",
    "l3_code",
    "l3_name",
    "ts_code",
    "name",
    "in_date",
    "out_date",
    "is_new",
)
MAX_INDUSTRY_CALLS = 64
MAX_INDUSTRIES = 31
MAX_INDUSTRY_BYTES = 2 * 1024 * 1024
_INDEX = re.compile(r"[0-9]{6}\.SI\Z")
# Provider delisting aliases remain raw symbols, never computation stock codes.
_PROVIDER_SYMBOL = re.compile(r"(?:[0-9]{6}|T[0-9]{5})\.(?:SH|SZ|BJ)\Z")
_DEPENDENCY = StrategyTableDependency(
    dataset_id="factor_industry",
    table_name="industry_interval",
    code_column="ts_code",
)
_PRIMARY_KEY = ("ts_code", "response_sha256", "source_row")
IndustryStatus = Literal["valid", "missing", "ambiguous", "boundary_unverified"]


class IndustrySourceRequest(BaseModel):
    model_config = _MODEL

    api_name: Literal["index_classify", "index_member_all"]
    level: Literal["L1"] | None = None
    src: Literal["SW2021"] | None = None
    l1_code: str | None = None
    is_new: Literal["Y", "N"] | None = None
    fields: tuple[str, ...]

    @model_validator(mode="before")
    @classmethod
    def _defaults(cls, value: object, info: ValidationInfo) -> object:
        if isinstance(value, dict):
            value = dict(value)
            directory = value.get("api_name") == "index_classify"
            value.setdefault(
                "fields", INDUSTRY_DIRECTORY_FIELDS if directory else INDUSTRY_MEMBER_FIELDS
            )
            if info.mode == "json" and isinstance(value["fields"], list):
                value["fields"] = tuple(value["fields"])
            if directory:
                value.setdefault("level", "L1")
                value.setdefault("src", "SW2021")
        return value

    @model_validator(mode="after")
    def _request(self) -> IndustrySourceRequest:
        if self.api_name == "index_classify":
            if (self.level, self.src, self.l1_code, self.is_new, self.fields) != (
                "L1",
                "SW2021",
                None,
                None,
                INDUSTRY_DIRECTORY_FIELDS,
            ):
                raise ValueError("industry directory requires exactly SW2021/L1 and fixed fields")
        elif (
            self.level is not None
            or self.src is not None
            or self.is_new is None
            or self.l1_code is None
            or not _INDEX.fullmatch(self.l1_code)
            or self.fields != INDUSTRY_MEMBER_FIELDS
        ):
            raise ValueError("industry members require explicit L1 code, Y/N and fixed fields")
        return self


class IndustryDirectoryEntry(BaseModel):
    model_config = _MODEL

    index_code: str = Field(pattern=r"^[0-9]{6}\.SI$")
    industry_name: str = Field(min_length=1, max_length=200)


def _rows(table: RawSecurityTable) -> Iterator[dict[str, object]]:
    for row in table.items:
        yield dict(zip(table.fields, row, strict=True))


def _date(value: object, *, optional: bool = False) -> date | None:
    if optional and value in (None, ""):
        return None
    if not isinstance(value, str) or not re.fullmatch(r"[0-9]{8}", value):
        raise ValueError("industry interval date must be a YYYYMMDD string")
    return datetime.strptime(value, "%Y%m%d").date()


def _directory(table: RawSecurityTable) -> tuple[IndustryDirectoryEntry, ...]:
    entries = []
    for row in _rows(table):
        if row["level"] != "L1" or row["src"] != "SW2021":
            raise ValueError("industry directory is outside SW2021/L1")
        entries.append(
            IndustryDirectoryEntry(
                index_code=row["index_code"],
                industry_name=row["industry_name"],
            )
        )
    entries.sort(key=lambda entry: entry.index_code)
    if not 1 <= len(entries) <= MAX_INDUSTRIES or len({e.index_code for e in entries}) != len(
        entries
    ):
        raise ValueError("industry directory is empty, duplicate or exceeds the 64 dispatch budget")
    return tuple(entries)


class CapturedIndustryResponse(BaseModel):
    model_config = _MODEL

    schema_version: Literal[1] = 1
    request: IndustrySourceRequest
    requested_at: ObservedTime
    observed_at: ObservedTime
    response: RawSecurityTable
    response_sha256: Sha256
    transport_receipt: SourceTransportUsageReceipt

    @model_validator(mode="after")
    def _binding(self) -> CapturedIndustryResponse:
        if self.requested_at > self.observed_at:
            raise ValueError("industry response precedes its request")
        fields = self.response.fields
        if len(set(fields)) != len(fields) or not set(self.request.fields) <= set(fields):
            raise ValueError("industry response has missing or duplicate required fields")
        if self.response_sha256 != hashlib.sha256(_bytes(self.response)).hexdigest():
            raise ValueError("industry raw response digest differs")
        receipt = self.transport_receipt
        if tuple(call.call_ordinal for call in receipt.call_receipts) != tuple(
            range(1, receipt.actual_call_count + 1)
        ):
            raise ValueError("industry transport ordinal sequence differs")
        if any(
            call.api_name != self.request.api_name
            or call.outcome is not SourceQuotaAttemptOutcome.SUCCESS
            or not self.requested_at <= call.dispatched_at <= call.committed_at <= self.observed_at
            for call in receipt.call_receipts
        ):
            raise ValueError(
                "industry transport receipts differ from request or actual capture time"
            )
        if self.request.api_name == "index_classify":
            _directory(self.response)
        else:
            if len(self.response.items) >= 2000:
                raise ValueError("industry response reached the unpaginated provider limit")
            for row in _rows(self.response):
                if (
                    row["l1_code"] != self.request.l1_code
                    or row["is_new"] != self.request.is_new
                    or not isinstance(row["ts_code"], str)
                    or not _PROVIDER_SYMBOL.fullmatch(row["ts_code"])
                    or not isinstance(row["l1_name"], str)
                    or not row["l1_name"].strip()
                ):
                    raise ValueError(
                        "industry member is outside requested L1/state or has invalid code/name"
                    )
                start = _date(row["in_date"])
                end = _date(row["out_date"], optional=True)
                if end is not None and start > end:
                    raise ValueError("industry interval dates are reversed")
        if len(_bytes(self)) > MAX_INDUSTRY_BYTES:
            raise ValueError("industry capture exceeds byte budget")
        return self


def make_industry_capture(
    request: IndustrySourceRequest,
    frame: pd.DataFrame,
    *,
    requested_at: datetime,
    observed_at: datetime,
    transport_receipt: SourceTransportUsageReceipt,
) -> CapturedIndustryResponse:
    if not isinstance(frame, pd.DataFrame) or len(frame) >= 2000:
        raise ValueError("industry provider response is not a table or reached its limit")
    response = RawSecurityTable(
        fields=tuple(frame.columns),
        items=tuple(
            tuple(_raw_cell(cell) for cell in row)
            for row in frame.itertuples(index=False, name=None)
        ),
    )
    return CapturedIndustryResponse(
        request=request,
        requested_at=requested_at,
        observed_at=observed_at,
        response=response,
        response_sha256=hashlib.sha256(_bytes(response)).hexdigest(),
        transport_receipt=transport_receipt,
    )


def _filename(request: IndustrySourceRequest) -> str:
    if request.api_name == "index_classify":
        return "index-classify-SW2021-L1.json"
    return f"index-member-all-{request.l1_code}-{request.is_new}.json"


class IndustryCaptureRequest(BaseModel):
    model_config = _MODEL

    root: Path
    import_paths: tuple[Path, ...] = Field(default=(), max_length=63)
    max_calls: int = Field(default=64, ge=1, le=64)

    @model_validator(mode="after")
    def _paths(self) -> IndustryCaptureRequest:
        _root_path(self.root)
        if len(set(self.import_paths)) != len(self.import_paths):
            raise ValueError("duplicate industry import path")
        for path in self.import_paths:
            _root_path(path.parent)
            if path.name in ("", ".", ".."):
                raise ValueError("invalid industry import filename")
        return self


class IndustryCaptureReference(BaseModel):
    model_config = _MODEL

    request: IndustrySourceRequest
    filename: str
    sha256: Sha256
    response_sha256: Sha256
    byte_count: int = Field(gt=0, le=MAX_INDUSTRY_BYTES)
    row_count: int = Field(ge=0, lt=2000)
    requested_at: ObservedTime
    observed_at: ObservedTime
    actual_call_count: int = Field(ge=1, le=64)
    origin: Literal["imported", "collected"]

    @model_validator(mode="after")
    def _name(self) -> IndustryCaptureReference:
        if self.filename != _filename(self.request) or self.requested_at > self.observed_at:
            raise ValueError("industry reference filename or clock differs")
        return self


def _schedule(directory: tuple[IndustryDirectoryEntry, ...]) -> tuple[IndustrySourceRequest, ...]:
    return (
        IndustrySourceRequest(api_name="index_classify"),
        *(
            IndustrySourceRequest(
                api_name="index_member_all", l1_code=entry.index_code, is_new=state
            )
            for entry in directory
            for state in ("Y", "N")
        ),
    )


class IndustryCollectionManifest(BaseModel):
    model_config = _MODEL

    schema_version: Literal[1] = 1
    status: Literal["captured"] = "captured"
    classification: Literal["SW2021"] = "SW2021"
    level: Literal["L1"] = "L1"
    directory: tuple[IndustryDirectoryEntry, ...] = Field(min_length=1, max_length=31)
    responses: tuple[IndustryCaptureReference, ...] = Field(min_length=3, max_length=63)
    imported_call_count: int = Field(ge=0, le=64)
    new_call_count: int = Field(ge=0, le=64)
    actual_call_count: int = Field(ge=1, le=64)
    sha256: Sha256

    @model_validator(mode="after")
    def _complete(self) -> IndustryCollectionManifest:
        if tuple(sorted({e.index_code for e in self.directory})) != tuple(
            e.index_code for e in self.directory
        ):
            raise ValueError("industry collection directory is unordered or duplicate")
        if tuple(ref.request for ref in self.responses) != _schedule(self.directory):
            raise ValueError("industry collection is not the complete directory plus Y/N schedule")
        imported = sum(ref.actual_call_count for ref in self.responses if ref.origin == "imported")
        new = sum(ref.actual_call_count for ref in self.responses if ref.origin == "collected")
        if (self.imported_call_count, self.new_call_count, self.actual_call_count) != (
            imported,
            new,
            imported + new,
        ):
            raise ValueError("industry collection transport counts differ")
        if self.sha256 != canonical_sha256(self.model_dump(exclude={"sha256"})):
            raise ValueError("industry collection digest differs")
        return self


def _reference(
    capture: CapturedIndustryResponse, data: bytes, origin: str
) -> IndustryCaptureReference:
    return IndustryCaptureReference(
        request=capture.request,
        filename=_filename(capture.request),
        sha256=hashlib.sha256(data).hexdigest(),
        response_sha256=capture.response_sha256,
        byte_count=len(data),
        row_count=len(capture.response.items),
        requested_at=capture.requested_at,
        observed_at=capture.observed_at,
        actual_call_count=capture.transport_receipt.actual_call_count,
        origin=origin,
    )


def _capture_bytes(data: bytes) -> CapturedIndustryResponse:
    strict_canonical_json_loads(data)
    return CapturedIndustryResponse.model_validate_json(data)


def _import(path: Path, expected_sha: str | None = None) -> tuple[bytes, CapturedIndustryResponse]:
    descriptor = _open_private_root(_root_path(path.parent))
    try:
        data, _ = _read_file(descriptor, path.name, MAX_INDUSTRY_BYTES, expected_sha)
        return data, _capture_bytes(data)
    finally:
        os.close(descriptor)


def _read_reference(
    descriptor: int, reference: IndustryCaptureReference
) -> tuple[CapturedIndustryResponse, tuple[int, ...]]:
    data, identity = _read_file(
        descriptor, reference.filename, MAX_INDUSTRY_BYTES, reference.sha256
    )
    capture = _capture_bytes(data)
    if _reference(capture, data, reference.origin) != reference:
        raise ValueError("industry capture differs from its manifest reference")
    return capture, identity


def _verify_collection(
    root: Path,
    descriptor: int,
    manifest: IndustryCollectionManifest,
) -> dict[str, tuple[int, ...]]:
    identities = {}
    attempts = set()
    for reference in manifest.responses:
        capture, identity = _read_reference(descriptor, reference)
        if (
            reference.request.api_name == "index_classify"
            and _directory(capture.response) != manifest.directory
        ):
            raise ValueError("industry directory differs from completed manifest")
        for call in capture.transport_receipt.call_receipts:
            if call.attempt_id in attempts:
                raise ValueError("industry collection repeats a transport dispatch receipt")
            attempts.add(call.attempt_id)
        identities[reference.filename] = identity
    _reject_interrupted_collection(descriptor)
    _check_identities(root, descriptor, identities)
    return identities


def _remove_completion(descriptor: int) -> None:
    # The root was created by this invocation; identity still protects named cleanup.
    try:
        fd = os.open(
            "collection.json", os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC, dir_fd=descriptor
        )
    except FileNotFoundError:
        return
    try:
        observed = os.fstat(fd)
        _cleanup_owned_temporary(descriptor, "collection.json", (observed.st_dev, observed.st_ino))
        os.fsync(descriptor)
    finally:
        os.close(fd)


def collect_industry_sources(
    request: IndustryCaptureRequest,
    *,
    fetch: Callable[[IndustrySourceRequest], CapturedIndustryResponse] | None,
) -> IndustryCollectionManifest:
    """Verify all imports first; invoke the caller only for the missing finite schedule."""
    request = IndustryCaptureRequest.model_validate(request)
    imported = {}
    directory = None
    attempts = set()
    for path in request.import_paths:
        data, capture = _import(path)
        name = _filename(capture.request)
        if name in imported:
            raise ValueError("duplicate imported industry request")
        for call in capture.transport_receipt.call_receipts:
            if call.attempt_id in attempts:
                raise ValueError("duplicate imported transport dispatch")
            attempts.add(call.attempt_id)
        imported[name] = (path, _reference(capture, data, "imported"))
        if capture.request.api_name == "index_classify":
            directory = _directory(capture.response)
    imported_calls = sum(ref.actual_call_count for _, ref in imported.values())
    if imported and directory is None:
        raise ValueError(
            "industry imports require their SW2021 directory for preflight verification"
        )
    if directory is not None:
        schedule = _schedule(directory)
        if not set(imported) <= {_filename(item) for item in schedule}:
            raise ValueError("imported member request is outside its directory")
        if imported_calls + len(schedule) - len(imported) > request.max_calls:
            raise ValueError(
                "industry imported dispatches plus missing schedule exceed call budget"
            )
    descriptor = _new_root(request.root)
    references = []
    actual_calls = imported_calls
    try:

        def save(item: IndustrySourceRequest) -> CapturedIndustryResponse:
            nonlocal actual_calls
            name = _filename(item)
            if name in imported:
                path, ref = imported[name]
                data, capture = _import(path, ref.sha256)
                origin = "imported"
            else:
                if fetch is None or actual_calls >= request.max_calls:
                    raise ValueError("industry source is missing a callback or dispatch budget")
                capture = CapturedIndustryResponse.model_validate(fetch(item))
                actual_calls += capture.transport_receipt.actual_call_count
                if actual_calls > request.max_calls:
                    raise ValueError("industry callback exceeded actual dispatch budget")
                data = _bytes(capture)
                origin = "collected"
            if capture.request != item:
                raise ValueError("industry callback response request differs")
            ref = _reference(capture, data, origin)
            _write_new(request.root, descriptor, name, data)
            references.append(ref)
            return capture

        directory_capture = save(IndustrySourceRequest(api_name="index_classify"))
        directory = _directory(directory_capture.response)
        del directory_capture
        schedule = _schedule(directory)
        if (
            actual_calls + sum(_filename(item) not in imported for item in schedule[1:])
            > request.max_calls
        ):
            raise ValueError("industry complete schedule exceeds dispatch budget")
        for item in schedule[1:]:
            save(item)
        payload = dict(
            directory=directory,
            responses=tuple(references),
            imported_call_count=imported_calls,
            new_call_count=actual_calls - imported_calls,
            actual_call_count=actual_calls,
            schema_version=1,
            status="captured",
            classification="SW2021",
            level="L1",
        )
        manifest = IndustryCollectionManifest(**payload, sha256=canonical_sha256(payload))
        _verify_collection(request.root, descriptor, manifest)
        _publish_collection(request.root, descriptor, manifest)
        data, _ = _read_file(
            descriptor,
            "collection.json",
            MAX_INDUSTRY_BYTES,
            hashlib.sha256(_bytes(manifest)).hexdigest(),
        )
        if IndustryCollectionManifest.model_validate_json(data) != manifest:
            raise ValueError("industry completion publication differs")
        _verify_collection(request.root, descriptor, manifest)
        return manifest
    except BaseException as exc:
        _remove_completion(descriptor)
        with suppress(OSError, ValueError):
            _write_new(
                request.root,
                descriptor,
                "interrupted.json",
                canonical_json_bytes(
                    {
                        "status": "interrupted",
                        "error_type": type(exc).__name__,
                        "completed_response_count": len(references),
                        "actual_call_count": actual_calls,
                    }
                ),
            )
        raise
    finally:
        os.close(descriptor)


def _load_collection(
    root: Path, descriptor: int
) -> tuple[IndustryCollectionManifest, str, dict[str, tuple[int, ...]]]:
    _reject_interrupted_collection(descriptor)
    data, identity = _read_file(descriptor, "collection.json", MAX_INDUSTRY_BYTES)
    strict_canonical_json_loads(data)
    manifest = IndustryCollectionManifest.model_validate_json(data)
    identities = _verify_collection(root, descriptor, manifest)
    identities["collection.json"] = identity
    _check_identities(root, descriptor, identities)
    return manifest, hashlib.sha256(data).hexdigest(), identities


def load_industry_collection(root: Path) -> IndustryCollectionManifest:
    root = _root_path(root)
    descriptor = _open_private_root(root)
    try:
        return _load_collection(root, descriptor)[0]
    finally:
        os.close(descriptor)


class FactorIndustryPrepareRequest(BaseModel):
    model_config = _MODEL

    prepared_source: FactorPreparedStreamSource
    collection_root: Path

    @field_validator("collection_root")
    @classmethod
    def _root(cls, value: Path) -> Path:
        return _root_path(value)


class FactorIndustrySource(BaseModel):
    model_config = _MODEL

    schema_version: Literal[1] = 1
    prepared_source_sha256: Sha256
    prepared_snapshot_id: str = Field(min_length=1)
    prepared_binding_hash: Sha256
    scope_content_hash: Sha256
    scope: FactorComputationScope
    code_commit: str = Field(pattern=r"^[0-9a-f]{40}$")
    classification: Literal["SW2021"] = "SW2021"
    level: Literal["L1"] = "L1"
    source_mode: Literal["historical_retrospective"] = "historical_retrospective"
    source_read_boundary: Literal["captured_api_responses"] = "captured_api_responses"
    collection: IndustryCollectionManifest
    collection_sha256: Sha256
    captured_at: ObservedTime
    prepared_at: ObservedTime
    artifact: DatasetSnapshotArtifact
    sha256: Sha256

    @model_validator(mode="after")
    def _binding(self) -> FactorIndustrySource:
        if (
            self.captured_at != max(ref.observed_at for ref in self.collection.responses)
            or self.captured_at > self.scope.as_of_time
            or self.prepared_at < self.captured_at
            or self.collection_sha256 != hashlib.sha256(_bytes(self.collection)).hexdigest()
        ):
            raise ValueError("industry source capture time or collection binding differs")
        artifact = self.artifact
        if (
            artifact.artifact_type != "materialized_table"
            or artifact.dataset_id != "factor_industry"
            or artifact.table_name != "industry_interval"
            or artifact.primary_key != _PRIMARY_KEY
            or artifact.event_column is not None
            or artifact.earliest_time is not None
            or artifact.latest_time is not None
            or artifact.file_size is None
            or artifact.row_count > sum(ref.row_count for ref in self.collection.responses[1:])
            or artifact.relative_path
            != f"tables/industry_interval/versions/{artifact.file_hash}.parquet"
        ):
            raise ValueError("industry source interval artifact binding differs")
        if self.sha256 != canonical_sha256(self.model_dump(exclude={"sha256"})):
            raise ValueError("industry source receipt digest differs")
        return self


def prepare_factor_industry_source(
    request: FactorIndustryPrepareRequest,
    *,
    lake_root: Path,
    now: Callable[[], datetime] = utc_now,
) -> FactorIndustrySource:
    request = FactorIndustryPrepareRequest.model_validate(request)
    prepared = request.prepared_source
    scope = prepared.receipt.request.scope
    source_fd = _open_private_root(request.collection_root)
    try:
        manifest, collection_sha, identities = _load_collection(request.collection_root, source_fd)
        captured_at = max(ref.observed_at for ref in manifest.responses)
        prepared_at = normalize_utc_datetime(now())
        if captured_at > scope.as_of_time or prepared_at < captured_at:
            raise ValueError("industry capture is later than scope asof or preparation clock")
        root = _root_path(lake_root)
        if root == request.collection_root or root.is_relative_to(request.collection_root):
            raise ValueError("industry lake must be separate from capture root")
        root.mkdir(mode=0o700, exist_ok=True)
        root_fd = _open_private_root(root)
        try:
            with (
                TemporaryDirectory(prefix=".industry-prepare-", dir=root) as scratch,
                duckdb.connect(":memory:") as connection,
            ):
                connection.execute("SET threads=1")
                connection.execute("SET temp_directory=?", [scratch])
                connection.execute(
                    "CREATE TABLE industry_interval(ts_code VARCHAR, l1_code VARCHAR, "
                    "l1_name VARCHAR, in_date DATE, out_date DATE, is_new VARCHAR, "
                    "response_sha256 VARCHAR, source_row BIGINT, "
                    "PRIMARY KEY(ts_code, response_sha256, source_row))"
                )
                codes = frozenset(scope.stock_codes)
                for reference in manifest.responses[1:]:
                    capture, identity = _read_reference(source_fd, reference)
                    if identity != identities[reference.filename]:
                        raise ValueError("industry capture changed after collection validation")
                    rows = [
                        (
                            row["ts_code"],
                            row["l1_code"],
                            row["l1_name"],
                            _date(row["in_date"]),
                            _date(row["out_date"], optional=True),
                            row["is_new"],
                            capture.response_sha256,
                            n,
                        )
                        for n, row in enumerate(_rows(capture.response))
                        if row["ts_code"] in codes
                    ]
                    if rows:
                        connection.executemany(
                            "INSERT INTO industry_interval VALUES (?, ?, ?, ?, ?, ?, ?, ?)", rows
                        )
                    del rows, capture
                artifact = materialize_table_dependency(
                    connection,
                    dependency=_DEPENDENCY,
                    artifact_root=root,
                    start_date=scope.start_date,
                    end_date=scope.end_date,
                    as_of_time=scope.as_of_time,
                    ts_codes=scope.stock_codes,
                )
                verify_materialized_table_artifact(
                    artifact, lake_root=root, as_of_time=scope.as_of_time
                )
                _check_identities(request.collection_root, source_fd, identities)
                _reject_interrupted_collection(source_fd)
                _require_same_root(root, root_fd)
                payload = dict(
                    schema_version=1,
                    prepared_source_sha256=prepared.sha256,
                    prepared_snapshot_id=prepared.snapshot.snapshot_id,
                    prepared_binding_hash=prepared.binding.binding_hash,
                    scope_content_hash=prepared.scope_content_hash,
                    scope=scope,
                    code_commit=prepared.snapshot.code_commit,
                    classification="SW2021",
                    level="L1",
                    source_mode="historical_retrospective",
                    source_read_boundary="captured_api_responses",
                    collection=manifest,
                    collection_sha256=collection_sha,
                    captured_at=captured_at,
                    prepared_at=prepared_at,
                    artifact=artifact,
                )
                source = FactorIndustrySource(**payload, sha256=canonical_sha256(payload))
                _check_identities(request.collection_root, source_fd, identities)
                return source
        finally:
            os.close(root_fd)
    finally:
        os.close(source_fd)


class FactorIndustryQuery(BaseModel):
    model_config = _MODEL

    source_sha256: Sha256
    trade_date: date
    stock_codes: tuple[StockCode, ...] = Field(min_length=1, max_length=500)

    @field_validator("stock_codes")
    @classmethod
    def _codes(cls, codes: tuple[str, ...]) -> tuple[str, ...]:
        if len(set(codes)) != len(codes):
            raise ValueError("duplicate industry query codes")
        return tuple(sorted(codes))


class FactorIndustryFact(BaseModel):
    model_config = _MODEL

    stock_code: StockCode
    trade_date: date
    status: IndustryStatus
    l1_code: str | None = None
    l1_name: str | None = None

    @model_validator(mode="after")
    def _value(self) -> FactorIndustryFact:
        if self.status == "valid":
            if self.l1_code is None or not _INDEX.fullmatch(self.l1_code) or not self.l1_name:
                raise ValueError("valid industry fact has no industry")
        elif self.l1_code is not None or self.l1_name is not None:
            raise ValueError("unverified industry fact has an industry value")
        return self


class FactorIndustryCounts(BaseModel):
    model_config = _MODEL

    valid: int = Field(ge=0)
    missing: int = Field(ge=0)
    ambiguous: int = Field(ge=0)
    boundary_unverified: int = Field(ge=0)


class FactorIndustryDayBatch(BaseModel):
    model_config = _MODEL

    source_sha256: Sha256
    prepared_source_sha256: Sha256
    classification: Literal["SW2021"] = "SW2021"
    source_mode: Literal["historical_retrospective"] = "historical_retrospective"
    query: FactorIndustryQuery
    trade_date: date
    facts: tuple[FactorIndustryFact, ...] = Field(min_length=1, max_length=500)
    counts: FactorIndustryCounts

    @model_validator(mode="after")
    def _complete(self) -> FactorIndustryDayBatch:
        counts = Counter(fact.status for fact in self.facts)
        if (
            self.source_sha256 != self.query.source_sha256
            or self.trade_date != self.query.trade_date
            or tuple(fact.stock_code for fact in self.facts) != self.query.stock_codes
            or any(fact.trade_date != self.trade_date for fact in self.facts)
            or any(
                getattr(self.counts, status) != counts[status]
                for status in FactorIndustryCounts.model_fields
            )
        ):
            raise ValueError("industry batch differs from its complete request")
        return self


class FactorIndustryReadLease:
    def __init__(
        self,
        source: FactorIndustrySource,
        connection: duckdb.DuckDBPyConnection,
        private_root: Path,
    ) -> None:
        self._source = source
        self._connection = connection
        self._private_root = private_root
        self._closed = False
        self._codes = frozenset(source.scope.stock_codes)
        self._directory = {
            entry.index_code: entry.industry_name for entry in source.collection.directory
        }

    @property
    def source(self) -> FactorIndustrySource:
        return self._source

    def query(self, query: FactorIndustryQuery) -> FactorIndustryDayBatch:
        if self._closed:
            raise RuntimeError("industry read lease is closed")
        query = FactorIndustryQuery.model_validate(query)
        if (
            query.source_sha256 != self.source.sha256
            or not set(query.stock_codes) <= self._codes
            or not self.source.scope.start_date <= query.trade_date <= self.source.scope.end_date
        ):
            raise ValueError("industry query differs from bound source or scope")
        day = query.trade_date
        rows = self._connection.execute(
            "SELECT ts_code, bool_or(in_date=? OR out_date=?), "
            "list_sort(list(DISTINCT l1_code) FILTER(WHERE in_date<? "
            "AND (out_date IS NULL OR out_date>?))) "
            "FROM industry_interval WHERE ts_code IN (SELECT unnest(?)) AND in_date<=? "
            "AND (out_date IS NULL OR out_date>=?) GROUP BY ts_code ORDER BY ts_code",
            [day, day, day, day, list(query.stock_codes), day, day],
        ).fetchmany(501)
        if len(rows) > 500:
            raise ValueError("industry daily query returned too many codes")
        projection = {row[0]: (bool(row[1]), row[2] or []) for row in rows}
        facts = []
        for code in query.stock_codes:
            boundary, candidates = projection.get(code, (False, []))
            status = (
                "boundary_unverified"
                if boundary
                else "missing"
                if not candidates
                else "valid"
                if len(candidates) == 1
                else "ambiguous"
            )
            industry = candidates[0] if status == "valid" else None
            facts.append(
                FactorIndustryFact(
                    stock_code=code,
                    trade_date=day,
                    status=status,
                    l1_code=industry,
                    l1_name=None if industry is None else self._directory[industry],
                )
            )
        counts = Counter(fact.status for fact in facts)
        return FactorIndustryDayBatch(
            source_sha256=self.source.sha256,
            prepared_source_sha256=self.source.prepared_source_sha256,
            query=query,
            trade_date=day,
            facts=tuple(facts),
            counts=FactorIndustryCounts(
                **{status: counts[status] for status in FactorIndustryCounts.model_fields}
            ),
        )

    def close(self) -> None:
        if not self._closed:
            self._closed = True
            self._connection.close()


@contextmanager
def open_factor_industry_source(
    source: FactorIndustrySource, *, lake_root: Path
) -> Iterator[FactorIndustryReadLease]:
    source = FactorIndustrySource.model_validate(source)
    root = _root_path(lake_root)
    descriptor = _open_private_root(root)
    try:
        with TemporaryDirectory(prefix=".industry-reader-", dir=root) as scratch:
            private_root = Path(scratch)
            original = verify_materialized_table_artifact(
                source.artifact, lake_root=root, as_of_time=source.scope.as_of_time
            )
            private_path = private_root / source.artifact.relative_path
            private_path.parent.mkdir(parents=True)
            shutil.copyfile(original, private_path)
            os.chmod(private_path, 0o600)
            verify_materialized_table_artifact(
                source.artifact, lake_root=private_root, as_of_time=source.scope.as_of_time
            )
            _require_same_root(root, descriptor)
            connection = duckdb.connect(":memory:")
            lease = None
            try:
                connection.execute("SET threads=1")
                connection.execute("SET temp_directory=?", [scratch])
                connection.execute(
                    "CREATE VIEW industry_interval AS SELECT * FROM read_parquet("
                    f"{_quoted_literal(str(private_path))}, hive_partitioning=false)"
                )
                allowed = [
                    (ref.response_sha256, ref.request.l1_code, ref.request.is_new, ref.row_count)
                    for ref in source.collection.responses[1:]
                ]
                connection.execute(
                    "CREATE TEMP TABLE allowed(response_sha256 VARCHAR, l1_code VARCHAR, "
                    "is_new VARCHAR, rows BIGINT)"
                )
                connection.executemany("INSERT INTO allowed VALUES (?, ?, ?, ?)", allowed)
                invalid = connection.execute(
                    "SELECT count(*) FROM industry_interval i LEFT JOIN allowed a "
                    "ON i.response_sha256=a.response_sha256 "
                    "AND i.l1_code=a.l1_code AND i.is_new=a.is_new "
                    "WHERE a.response_sha256 IS NULL OR i.ts_code NOT IN (SELECT unnest(?)) "
                    "OR i.in_date IS NULL OR (i.out_date IS NOT NULL AND i.in_date>i.out_date) "
                    "OR i.source_row<0 OR i.source_row>=a.rows OR i.l1_name IS NULL",
                    [list(source.scope.stock_codes)],
                ).fetchone()
                if int(invalid[0]):
                    raise ValueError(
                        "industry private intervals differ from bound scope or captures"
                    )
                lease = FactorIndustryReadLease(source, connection, private_root)
                yield lease
            finally:
                if lease is None:
                    connection.close()
                else:
                    lease.close()
    finally:
        os.close(descriptor)
