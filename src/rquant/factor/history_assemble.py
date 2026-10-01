"""Assemble explicit complete captures into the existing bounded member archive.

``python -m rquant.factor.history_assemble archive --help`` exposes the offline
entry point. This module never constructs application settings or a provider client.
"""

from __future__ import annotations

import argparse
import hashlib
import os
import sys
from collections.abc import Iterator, Sequence
from contextlib import closing
from datetime import date
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, Field, model_validator

from rquant.factor.member_archive import (
    MAX_FACTOR_MEMBER_MANIFEST_BYTES,
    FactorMemberArchiveReference,
    FactorMemberArchiveRequest,
    FactorMemberDayInput,
    _check_identities,
    _read_file,
    publish_factor_member_archive,
)
from rquant.factor.name_collect import NameCollectionManifest, load_name_capture_collection
from rquant.factor.result_artifact import (
    _cleanup_owned_temporary,
    _file_identity,
    _open_private_root,
    _require_same_root,
    _root_identity,
    _root_path,
)
from rquant.factor.security_collect import (
    _MODEL,
    MAX_CAPTURE_CALLS,
    MAX_CAPTURE_DAYS,
    MAX_COLLECTION_BYTES,
    SecurityCollectionManifest,
    SecurityDayResult,
    _bytes,
    _cli_day,
    _cli_time,
    _new_root,
    _reject_interrupted_collection,
    _validate_days,
    _write_new,
    iter_security_collection_days,
    load_security_capture_collection,
)
from rquant.factor.time_series import MAX_TRADE_DAYS
from rquant.factor.universe import MAX_UNIVERSE_SECURITIES, ObservedTime, Sha256, StockCode
from rquant.strict_json import canonical_json_bytes

MAX_CAPTURE_ROOTS = 34
_FileIdentity = tuple[int, int, int, int, int, int, int, int]


class HistoryAssemblyRequest(BaseModel):
    model_config = _MODEL

    capture_roots: tuple[Path, ...] = Field(min_length=1, max_length=MAX_CAPTURE_ROOTS)
    trading_days: tuple[date, ...] = Field(min_length=1, max_length=MAX_TRADE_DAYS)
    selection: Literal["all", "gem"]
    as_of: ObservedTime
    input_root: Path
    root: Path
    name_root: Path | None = None

    @model_validator(mode="after")
    def _scope(self) -> HistoryAssemblyRequest:
        _validate_days(self.trading_days)
        if len(set(self.capture_roots)) != len(self.capture_roots):
            raise ValueError("duplicate historical capture directory")
        paths = (*self.capture_roots, self.input_root, self.root)
        if self.name_root is not None:
            paths += (self.name_root,)
        for path in paths:
            _root_path(path)
        if len(set(paths)) != len(paths):
            raise ValueError("history input, source and archive directories must differ")
        return self


class _CollectionBinding(BaseModel):
    model_config = _MODEL

    root: Path
    directory_identity: tuple[int, int]
    manifest_sha256: Sha256
    files: tuple[tuple[str, _FileIdentity], ...] = Field(
        min_length=1, max_length=MAX_CAPTURE_CALLS + 1
    )
    trading_days: tuple[date, ...] = Field(max_length=MAX_CAPTURE_DAYS)


class _DayBinding(BaseModel):
    model_config = _MODEL

    trade_date: date
    payload_sha256: Sha256
    source_sha256: Sha256
    observed_at: ObservedTime


class _AssemblyPreview(BaseModel):
    model_config = _MODEL

    batches: tuple[_CollectionBinding, ...] = Field(min_length=1, max_length=MAX_CAPTURE_ROOTS)
    names: _CollectionBinding | None
    days: tuple[_DayBinding, ...] = Field(min_length=1, max_length=MAX_TRADE_DAYS)
    computation_stock_codes: tuple[StockCode, ...] = Field(
        min_length=1, max_length=MAX_UNIVERSE_SECURITIES
    )


def _snapshot_collection(
    root: Path, manifest: SecurityCollectionManifest | NameCollectionManifest
) -> _CollectionBinding:
    descriptor = _open_private_root(root)
    try:
        _reject_interrupted_collection(descriptor)
        data, identity = _read_file(descriptor, "collection.json", MAX_COLLECTION_BYTES)
        if type(manifest).model_validate_json(data) != manifest:
            raise ValueError("历史采集回执在加载与绑定之间改变")
        files = [("collection.json", identity)]
        for reference in manifest.responses:
            files.append(
                (
                    reference.filename,
                    _file_identity(
                        os.stat(reference.filename, dir_fd=descriptor, follow_symlinks=False)
                    ),
                )
            )
        binding = _CollectionBinding(
            root=root,
            directory_identity=_root_identity(os.fstat(descriptor)),
            manifest_sha256=hashlib.sha256(data).hexdigest(),
            files=tuple(files),
            trading_days=manifest.trading_days
            if isinstance(manifest, SecurityCollectionManifest)
            else (),
        )
        _check_identities(root, descriptor, dict(binding.files))
        return binding
    finally:
        os.close(descriptor)


def _check_collection(binding: _CollectionBinding) -> None:
    descriptor = _open_private_root(binding.root)
    try:
        if _root_identity(os.fstat(descriptor)) != binding.directory_identity:
            raise ValueError("历史采集目录在预验后改变")
        _reject_interrupted_collection(descriptor)
        data, _ = _read_file(
            descriptor, "collection.json", MAX_COLLECTION_BYTES, binding.manifest_sha256
        )
        del data
        _check_identities(binding.root, descriptor, dict(binding.files))
        _reject_interrupted_collection(descriptor)
    finally:
        os.close(descriptor)


def _iter_requested_days(
    request: HistoryAssemblyRequest,
    batches: tuple[_CollectionBinding, ...],
    names: _CollectionBinding | None,
) -> Iterator[SecurityDayResult]:
    requested = set(request.trading_days)
    for binding in batches:
        _check_collection(binding)
        if names is not None:
            _check_collection(names)
        with closing(
            iter_security_collection_days(
                binding.root, selection=request.selection, name_root=request.name_root
            )
        ) as iterator:
            for result in iterator:
                if result.trade_date not in requested:
                    del result
                    continue
                if result.trade_date not in binding.trading_days:
                    raise ValueError("历史日期与采集批次日程不一致")
                if not result.accepted:
                    raise ValueError(f"{result.trade_date}: {result.reason}")
                if result.batch.observed_at > request.as_of:
                    raise ValueError("归档截止时刻早于真实来源接收时刻")
                yield result
                del result
        _check_collection(binding)
        if names is not None:
            _check_collection(names)


def _day_payload(result: SecurityDayResult) -> FactorMemberDayInput:
    return FactorMemberDayInput(
        schema_version=1, trade_date=result.trade_date, securities=result.batch, membership=None
    )


def _day_binding(payload: FactorMemberDayInput, data: bytes) -> _DayBinding:
    return _DayBinding(
        trade_date=payload.trade_date,
        payload_sha256=hashlib.sha256(data).hexdigest(),
        source_sha256=payload.securities.source_sha256,
        observed_at=payload.securities.observed_at,
    )


def _check_sources(preview: _AssemblyPreview) -> None:
    for binding in preview.batches:
        _check_collection(binding)
    if preview.names is not None:
        _check_collection(preview.names)


def _checked_filenames(request: HistoryAssemblyRequest, preview: _AssemblyPreview) -> Iterator[str]:
    for day in request.trading_days:
        yield f"security-{day:%Y%m%d}.json"
    # The existing publisher checks the natural tail before publishing its manifest.
    _check_sources(preview)


def _preflight(request: HistoryAssemblyRequest) -> _AssemblyPreview:
    batches: list[_CollectionBinding] = []
    covered: set[date] = set()
    for root in request.capture_roots:
        manifest = load_security_capture_collection(root)
        if covered.intersection(manifest.trading_days):
            raise ValueError("历史采集批次日期重叠")
        covered.update(manifest.trading_days)
        batches.append(_snapshot_collection(root, manifest))
        del manifest
    if not set(request.trading_days) <= covered:
        raise ValueError("历史采集批次缺少请求日期")
    names = None
    if request.name_root is not None:
        names = _snapshot_collection(
            request.name_root, load_name_capture_collection(request.name_root)
        )
    codes: set[str] = set()
    days: dict[date, _DayBinding] = {}
    with closing(_iter_requested_days(request, tuple(batches), names)) as iterator:
        for result in iterator:
            if result.trade_date in days:
                raise ValueError("历史装配出现重复日期")
            codes.update(result.batch.complete_stock_codes)
            if len(codes) > MAX_UNIVERSE_SECURITIES:
                raise ValueError("跨批股票代码并集超过7000上限")
            payload = _day_payload(result)
            data = _bytes(payload)
            days[result.trade_date] = _day_binding(payload, data)
            del result, payload, data
    if set(days) != set(request.trading_days):
        raise ValueError("历史装配预验未完成全部日期")
    preview = _AssemblyPreview(
        batches=tuple(batches),
        names=names,
        days=tuple(days[day] for day in request.trading_days),
        computation_stock_codes=tuple(sorted(codes)),
    )
    _check_sources(preview)
    return preview


def assemble_history_archive(request: HistoryAssemblyRequest) -> FactorMemberArchiveReference:
    """Preflight all days, recheck each written binding, then use the original publisher."""
    request = HistoryAssemblyRequest.model_validate(request)
    preview = _preflight(request)
    _check_sources(preview)
    archive_request = FactorMemberArchiveRequest(
        selection=request.selection,
        trading_days=request.trading_days,
        as_of=request.as_of,
        computation_stock_codes=preview.computation_stock_codes,
    )
    expected = {binding.trade_date: binding for binding in preview.days}
    written: set[date] = set()
    descriptor = _new_root(request.input_root)
    try:
        with closing(_iter_requested_days(request, preview.batches, preview.names)) as iterator:
            for result in iterator:
                payload = _day_payload(result)
                data = _bytes(payload)
                if result.trade_date in written or _day_binding(payload, data) != expected.get(
                    result.trade_date
                ):
                    raise ValueError("预验与写入日绑定不一致")
                _write_new(
                    request.input_root,
                    descriptor,
                    f"security-{result.trade_date:%Y%m%d}.json",
                    data,
                )
                written.add(result.trade_date)
                del result, payload, data
        if written != set(request.trading_days):
            raise ValueError("历史装配写入未完成全部日期")
        _check_sources(preview)
        _require_same_root(request.input_root, descriptor)
        archive_descriptor = _new_root(request.root)
        try:
            reference = publish_factor_member_archive(
                archive_request,
                input_root=request.input_root,
                root=request.root,
                daily_filenames=_checked_filenames(request, preview),
            )
            # A verification read may fail after the completion file already exists.
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
                _check_sources(preview)
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
    parser = argparse.ArgumentParser(description="完成采集批次的显式历史成员装配")
    commands = parser.add_subparsers(dest="command", required=True)
    archive = commands.add_parser("archive", help="预验完整日程并发布已有成员归档")
    archive.add_argument("--capture-root", dest="roots", action="append", type=Path, required=True)
    archive.add_argument("--date", dest="days", action="append", type=_cli_day, required=True)
    archive.add_argument("--selection", choices=("all", "gem"), required=True)
    archive.add_argument("--as-of", type=_cli_time, required=True)
    archive.add_argument("--name-root", type=Path)
    archive.add_argument("--input-root", type=Path, required=True)
    archive.add_argument("--archive-root", type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        reference = assemble_history_archive(
            HistoryAssemblyRequest(
                capture_roots=tuple(args.roots),
                trading_days=tuple(args.days),
                selection=args.selection,
                as_of=args.as_of,
                name_root=args.name_root,
                input_root=args.input_root,
                root=args.archive_root,
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
    except (ValueError, OSError, RuntimeError) as exc:
        print(
            canonical_json_bytes(
                {"status": "refused", "reason": str(exc), "error_type": type(exc).__name__}
            ).decode(),
            file=sys.stderr,
        )
        return 2
    except KeyboardInterrupt:
        print('{"status":"interrupted","reason":"装配已中断，未生成完成回执"}', file=sys.stderr)
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
