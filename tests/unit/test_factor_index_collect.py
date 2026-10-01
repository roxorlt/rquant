"""Official daily samples must remain exact dated inputs to the existing archive."""

from __future__ import annotations

import hashlib
import importlib
import importlib.util
import json
import os
import struct
import subprocess
import sys
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pytest

from rquant.factor import security_collect as security
from rquant.factor.member_stream import open_factor_member_stream
from rquant.strict_json import canonical_json_bytes

_DAY = date(2026, 9, 29)
_NEXT = date(2026, 9, 30)
_OBSERVED = datetime(2026, 10, 1, 10, tzinfo=UTC)
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
_INDICES = {
    "hs300": ("000300", "沪深300", "CSI 300", 300),
    "zz1000": ("000852", "中证1000", "CSI 1000", 1000),
}
_BASE = "https://oss-ch.csindex.com.cn/static/html/csindex/public/uploads/file/autofile/cons/"


def _module() -> object:
    name = "rquant.factor.index_collect"
    assert importlib.util.find_spec(name) is not None, "official daily index source is missing"
    return importlib.import_module(name)


def _rows(selection: str, day: date = _DAY) -> list[list[object]]:
    code, name, english, count = _INDICES[selection]
    rows = [
        [
            day.strftime("%Y%m%d"),
            code,
            name,
            english,
            f"{i:06d}",
            "样本公司",
            "Sample Company",
            "深圳证券交易所",
            "Shenzhen Stock Exchange",
        ]
        for i in range(1, count + 1)
    ]
    rows[-1][4:] = [
        "600999",
        "上海样本",
        "Shanghai Sample",
        "上海证券交易所",
        "Shanghai Stock Exchange",
    ]
    return rows


def _xls(
    rows: list[list[object]], *, headers: tuple[str, ...] = _HEADERS, sheets: int = 1
) -> bytes:
    """Minimal synthetic BIFF8 XLS, decoded by real xlrd without another writer library."""

    def record(opcode: int, data: bytes) -> bytes:
        return struct.pack("<HH", opcode, len(data)) + data

    def bof(kind: int) -> bytes:
        return record(0x0809, struct.pack("<HHHHII", 0x0600, kind, 0x0DBB, 0x07CC, 0, 0x0600))

    eof = record(0x000A, b"")
    table = bytearray(bof(0x0010))
    for row, values in enumerate([list(headers), *rows]):
        for column, value in enumerate(values):
            prefix = struct.pack("<HHH", row, column, 0)
            if isinstance(value, str):
                cell = prefix + struct.pack("<HB", len(value), 1) + value.encode("utf-16le")
                table.extend(record(0x0204, cell))
            else:
                table.extend(record(0x0203, prefix + struct.pack("<d", value)))
    table.extend(eof)
    globals_ = bof(0x0005) + record(0x0042, struct.pack("<H", 1200)) + record(0x00E0, bytes(20))
    names = [f"sample-{index}".encode() for index in range(sheets)]
    offset = len(globals_) + sum(12 + len(name) for name in names) + len(eof)
    bounds = bytearray()
    for name in names:
        bounds.extend(record(0x0085, struct.pack("<IBBBB", offset, 0, 0, len(name), 0) + name))
        offset += len(table)
    return globals_ + bounds + eof + bytes(table) * sheets


def _receipt(m: object, selection: str, data: bytes) -> object:
    return m.IndexHttpReceipt(
        url=_BASE + _INDICES[selection][0] + "cons.xls",
        final_url=_BASE + _INDICES[selection][0] + "cons.xls",
        requested_at=_OBSERVED - timedelta(seconds=1),
        received_at=_OBSERVED,
        status="received",
        http_status=200,
        headers={"Content-Type": "application/vnd.ms-excel", "Content-Length": str(len(data))},
        bytes=len(data),
        sha256=hashlib.sha256(data).hexdigest(),
        raw_file="source.xls",
    )


def _collect(root: Path, selection: str, day: date = _DAY) -> Path:
    m = _module()
    data = _xls(_rows(selection, day))

    def get(request: object, timeout_seconds: float, max_bytes: int) -> object:
        return m.IndexHttpResponse(
            data=data,
            http_status=200,
            final_url=request.url,
            headers={"Content-Type": "application/vnd.ms-excel"},
        )

    m.collect_index_sources(
        m.IndexCaptureRequest(root=root, selections=(selection,), max_calls=1),
        transport=get,
        clock=lambda: _OBSERVED,
    )
    return root


def _securities(root: Path, days: tuple[date, ...], codes: tuple[str, ...]) -> Path:
    from tests.unit.test_factor_security_collect import _daily, _reference, _references

    refs = _references(*(_reference(code, "主板") for code in codes))
    descriptor = security._new_root(root)
    try:
        captures = [
            *refs,
            *(
                _daily(
                    *(
                        (code, "*ST样本" if code == "000001.SZ" else "历史名称", "20200102")
                        for code in codes
                    ),
                    day=day,
                )
                for day in days
            ),
        ]
        manifest = security.SecurityCollectionManifest(
            trading_days=days,
            responses=tuple(security._save_capture(root, descriptor, item) for item in captures),
        )
        security._publish_collection(root, descriptor, manifest)
    finally:
        os.close(descriptor)
    return root


def _archive_request(
    tmp_path: Path,
    selection: str,
    *,
    days: tuple[date, ...] = (_DAY, _NEXT),
    missing_fact: bool = False,
) -> object:
    m = _module()
    index_roots = tuple(
        _collect(tmp_path / f"index-{i}", selection, day) for i, day in enumerate(days)
    )
    codes = tuple(
        sorted(
            str(row[4]) + (".SH" if row[7] == "上海证券交易所" else ".SZ")
            for row in _rows(selection)
        )
    )
    securities = _securities(tmp_path / "securities", days, codes[:-1] if missing_fact else codes)
    return m.IndexArchiveRequest(
        index_roots=index_roots,
        security_roots=(securities,),
        trading_days=days,
        selection=selection,
        as_of=_OBSERVED,
        input_root=tmp_path / "inputs",
        root=tmp_path / "archive",
    )


def _no_manifest(root: Path) -> None:
    assert not list(root.glob("factor-member-archive-v1-*.json"))


@pytest.mark.parametrize("selection", ["hs300", "zz1000"])
def test_exact_daily_xls_preserves_text_codes_fields_and_real_source_binding(
    selection: str,
) -> None:
    m = _module()
    data = _xls(_rows(selection))
    receipt = _receipt(m, selection, data)
    result = m.normalize_index_response(
        m.IndexSourceRequest(selection=selection, url=receipt.url),
        receipt,
        data,
        trade_date=_DAY,
        as_of=_OBSERVED,
    )
    expected = tuple(
        sorted(
            str(row[4]) + (".SH" if row[7] == "上海证券交易所" else ".SZ")
            for row in _rows(selection)
        )
    )
    assert result.stock_codes == expected
    assert result.selection == selection and result.trade_date == _DAY
    assert result.source_sha256 == hashlib.sha256(data).hexdigest()
    assert result.observed_at == _OBSERVED
    assert result.source_mode == "historical_retrospective"
    assert result.source_kind == "daily_complete_membership"
    assert "000001.SZ" in result.stock_codes


@pytest.mark.parametrize(
    "fault",
    [
        "duplicate",
        "missing",
        "extra",
        "mixed_date",
        "other_index",
        "exchange",
        "exchange_english",
        "header",
        "non_text_code",
        "bad_file",
        "multiple_sheets",
    ],
)
def test_invalid_or_incomplete_sample_is_refused(fault: str) -> None:
    m = _module()
    rows = _rows("hs300")
    headers = _HEADERS
    if fault == "duplicate":
        rows[1][4] = rows[0][4]
    elif fault == "missing":
        rows.pop()
    elif fault == "extra":
        rows.append(rows[-1].copy())
    elif fault == "mixed_date":
        rows[1][0] = "20260930"
    elif fault == "other_index":
        rows[1][1] = "000852"
    elif fault == "exchange":
        rows[1][7] = "未知交易所"
    elif fault == "exchange_english":
        rows[1][8] = "Shanghai Stock Exchange"
    elif fault == "header":
        headers = ("错误日期", *_HEADERS[1:])
    elif fault == "non_text_code":
        rows[1][4] = 2.0
    data = (
        b"<html>not a workbook</html>"
        if fault == "bad_file"
        else _xls(rows, headers=headers, sheets=2 if fault == "multiple_sheets" else 1)
    )
    receipt = _receipt(m, "hs300", data)
    with pytest.raises(ValueError):
        m.normalize_index_response(
            m.IndexSourceRequest(selection="hs300", url=receipt.url),
            receipt,
            data,
            trade_date=_DAY,
            as_of=_OBSERVED,
        )


@pytest.mark.parametrize("fault", ["raw_digest", "as_of", "requested_date"])
def test_response_digest_visibility_and_exact_requested_day_are_required(fault: str) -> None:
    m = _module()
    data = _xls(_rows("hs300"))
    receipt = _receipt(m, "hs300", data)
    with pytest.raises(ValueError):
        m.normalize_index_response(
            m.IndexSourceRequest(selection="hs300", url=receipt.url),
            receipt,
            data + b"x" if fault == "raw_digest" else data,
            trade_date=_NEXT if fault == "requested_date" else _DAY,
            as_of=_OBSERVED - timedelta(seconds=1) if fault == "as_of" else _OBSERVED,
        )


def test_http_seam_captures_actual_bytes_receipts_and_dispatch_count(tmp_path: Path) -> None:
    m = _module()
    calls = []
    times = iter(_OBSERVED + timedelta(seconds=i) for i in range(4))

    def get(request: object, timeout_seconds: float, max_bytes: int) -> object:
        calls.append((request, timeout_seconds, max_bytes))
        data = _xls(_rows(request.selection))
        return m.IndexHttpResponse(
            data=data,
            http_status=200,
            final_url=request.url,
            headers={"Content-Type": "application/vnd.ms-excel"},
        )

    request = m.IndexCaptureRequest(
        root=tmp_path / "capture", selections=("hs300", "zz1000"), max_calls=2, timeout_seconds=12.0
    )
    manifest = m.collect_index_sources(request, transport=get, clock=lambda: next(times))
    assert manifest.actual_call_count == 2 and len(calls) == 2
    assert all(timeout == 12.0 and limit == 4 * 1024 * 1024 for _, timeout, limit in calls)
    assert m.load_index_capture_collection(request.root) == manifest
    for i, reference in enumerate(manifest.responses):
        assert (request.root / reference.raw_filename).read_bytes() == _xls(
            _rows(reference.selection)
        )
        receipt = json.loads((request.root / reference.filename).read_bytes())
        assert receipt["final_url"] == calls[i][0].url and receipt["http_status"] == 200
        assert receipt["requested_at"] != receipt["received_at"]


@pytest.mark.parametrize("fault", ["network", "interrupt", "too_large", "http_error", "final_url"])
def test_failed_dispatch_or_interrupt_never_loads_as_complete(tmp_path: Path, fault: str) -> None:
    m = _module()
    calls = 0

    def get(request: object, timeout_seconds: float, max_bytes: int) -> object:
        nonlocal calls
        calls += 1
        if fault == "network":
            raise OSError("offline transport failed")
        if fault == "interrupt":
            raise KeyboardInterrupt()
        return m.IndexHttpResponse(
            data=b"x" * (max_bytes + 1) if fault == "too_large" else _xls(_rows("hs300")),
            http_status=404 if fault == "http_error" else 200,
            final_url="https://example.invalid/source" if fault == "final_url" else request.url,
            headers={},
        )

    request = m.IndexCaptureRequest(root=tmp_path / "capture", selections=("hs300",), max_calls=1)
    with pytest.raises((ValueError, OSError, KeyboardInterrupt)):
        m.collect_index_sources(request, transport=get, clock=lambda: _OBSERVED)
    assert calls == 1
    with pytest.raises((ValueError, OSError)):
        m.load_index_capture_collection(request.root)
    assert json.loads((request.root / "interrupted.json").read_bytes())["actual_call_count"] == 1


def test_only_explicit_indices_fixed_urls_and_finite_local_budgets_are_allowed(
    tmp_path: Path,
) -> None:
    m = _module()
    for selections, budget in [
        (("hs300", "zz1000"), 1),
        (("hs300",), 5),
        (("hs300", "hs300"), 2),
        (("all",), 1),
    ]:
        with pytest.raises(ValueError):
            m.IndexCaptureRequest(
                root=tmp_path / "capture", selections=selections, max_calls=budget
            )
    with pytest.raises(ValueError):
        m.IndexSourceRequest(selection="hs300", url="https://example.invalid/custom.xls")
    with pytest.raises(ValueError):
        m.IndexCaptureRequest(
            root=tmp_path / "capture",
            selections=("hs300",),
            max_calls=1,
            timeout_seconds=float("inf"),
        )


@pytest.mark.parametrize("component", ["raw", "receipt"])
def test_captured_bytes_or_receipt_digest_change_is_refused(tmp_path: Path, component: str) -> None:
    m = _module()
    root = _collect(tmp_path / "capture", "hs300")
    ref = m.load_index_capture_collection(root).responses[0]
    (root / (ref.raw_filename if component == "raw" else ref.filename)).write_bytes(b"{}")
    with pytest.raises(ValueError):
        m.replay_index_day(root, selection="hs300", trade_date=_DAY, as_of=_OBSERVED)


@pytest.mark.parametrize("selection", ["hs300", "zz1000"])
def test_two_exact_days_archive_through_original_reader_and_pool_contract(
    tmp_path: Path, selection: str
) -> None:
    m = _module()
    request = _archive_request(tmp_path, selection)
    reference = m.archive_index_collections(request)
    expected = tuple(
        sorted(
            str(row[4]) + (".SH" if row[7] == "上海证券交易所" else ".SZ")
            for row in _rows(selection)
        )
    )
    with open_factor_member_stream(request.root, reference) as stream:
        for day, universe in zip(request.trading_days, stream, strict=True):
            assert universe.trade_date == day and universe.membership.stock_codes == expected
            assert universe.membership.observed_at == _OBSERVED
            assert len(universe.securities.facts) == len(expected)
            # CSI keeps the historical ST member; the reused all pass keeps full facts.
            assert "000001.SZ" in universe.membership.stock_codes
        stream.require_completion()


@pytest.mark.parametrize("fault", ["missing_day", "duplicate_day", "missing_fact", "as_of"])
def test_missing_duplicate_or_inadmissible_archive_input_has_no_success_manifest(
    tmp_path: Path, fault: str
) -> None:
    m = _module()
    request = _archive_request(tmp_path, "hs300", missing_fact=fault == "missing_fact")
    if fault == "missing_day":
        request = request.model_copy(update={"index_roots": request.index_roots[:1]})
    elif fault == "duplicate_day":
        other = _collect(tmp_path / "duplicate", "hs300", _DAY)
        request = request.model_copy(update={"index_roots": (*request.index_roots, other)})
    elif fault == "as_of":
        request = request.model_copy(update={"as_of": _OBSERVED - timedelta(seconds=1)})
    with pytest.raises(ValueError):
        m.archive_index_collections(request)
    _no_manifest(request.root)


@pytest.mark.parametrize("read_error", [False, True])
def test_source_change_during_final_publication_cannot_leave_consumable_completion(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, read_error: bool
) -> None:
    m = _module()
    import rquant.factor.member_archive as archive

    request = _archive_request(tmp_path, "hs300", days=(_DAY,))
    ref = m.load_index_capture_collection(request.index_roots[0]).responses[0]
    raw = request.index_roots[0] / ref.raw_filename
    original_publish = archive._publish_bytes
    original_read = m._read_file

    def publish(root: Path, descriptor: int, name: str, data: bytes, limit: int) -> tuple[int, ...]:
        if name.startswith("factor-member-archive-v1-"):
            raw.write_bytes(b"{}")
        return original_publish(root, descriptor, name, data, limit)

    def read(descriptor: int, name: str, limit: int, expected_sha: str | None = None) -> object:
        if read_error and name.startswith("factor-member-archive-v1-"):
            raise OSError("manifest read failed")
        return original_read(descriptor, name, limit, expected_sha)

    monkeypatch.setattr(archive, "_publish_bytes", publish)
    monkeypatch.setattr(m, "_read_file", read)
    with pytest.raises((ValueError, OSError)):
        m.archive_index_collections(request)
    _no_manifest(request.root)


def test_imported_public_receipt_cli_preserves_observation_and_replays_only_its_day(
    tmp_path: Path,
) -> None:
    m = _module()
    data = _xls(_rows("hs300"))
    raw = tmp_path / "source.xls"
    raw.write_bytes(data)
    receipt = _receipt(m, "hs300", data).model_dump(mode="json")
    receipt.pop("final_url")
    receipt["raw_file"] = str(raw)
    incoming = tmp_path / "source.xls.receipt.json"
    incoming.write_bytes(canonical_json_bytes(receipt))
    root = tmp_path / "imported"
    command = [
        sys.executable,
        "-m",
        "rquant.factor.index_collect",
        "import-receipts",
        "--receipt-file",
        str(incoming),
        "--root",
        str(root),
    ]
    completed = subprocess.run(command, capture_output=True, text=True, check=False)
    assert completed.returncode == 0, completed.stderr
    manifest = m.load_index_capture_collection(root)
    assert manifest.capture_mode == "imported_receipts" and manifest.actual_call_count == 0
    assert (root / manifest.responses[0].filename).read_bytes() == incoming.read_bytes()
    replay = [
        sys.executable,
        "-m",
        "rquant.factor.index_collect",
        "replay",
        "--index-root",
        str(root),
        "--selection",
        "hs300",
        "--date",
        _DAY.isoformat(),
        "--as-of",
        _OBSERVED.isoformat(),
    ]
    result = subprocess.run(replay, capture_output=True, text=True, check=False)
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout)["observed_at"] == "2026-10-01T10:00:00Z"
    replay[replay.index(_DAY.isoformat())] = _NEXT.isoformat()
    missing = subprocess.run(replay, capture_output=True, text=True, check=False)
    assert missing.returncode == 2 and json.loads(missing.stderr)["status"] == "refused"


def test_plain_module_import_has_no_settings_or_network_initialization() -> None:
    _module()
    code = """
import sys
import rquant.factor.index_collect
assert 'rquant.config' not in sys.modules
assert 'rquant.adapter.tushare' not in sys.modules
"""
    result = subprocess.run(
        [sys.executable, "-c", code],
        capture_output=True,
        text=True,
        check=False,
        env={
            "PATH": os.environ["PATH"],
            "PYTHONPATH": os.environ["PYTHONPATH"],
            "RQUANT_DISABLE_DOTENV": "1",
        },
    )
    assert result.returncode == 0, result.stderr
