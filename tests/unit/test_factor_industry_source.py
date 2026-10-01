"""Industry responses remain auditable and interval endpoints stay explicit."""

from __future__ import annotations

import hashlib
import importlib
import importlib.util
import socket
from datetime import timedelta
from pathlib import Path

import duckdb
import pandas as pd
import pytest

from rquant.factor.security_collect import _bytes
from rquant.source_quota_store import SourceQuotaAttemptOutcome
from rquant.source_quota_transport import (
    SourceTransportCallReceipt,
    SourceTransportUsageReceipt,
)
from tests.unit.test_factor_market_cap_source import _prepared
from tests.unit.test_factor_source_prepare import _AS_OF, _FIRST

_CAPTURED = _AS_OF - timedelta(hours=1)
_CODES = tuple(f"{n:06d}.SZ" for n in range(1, 9))


def _module() -> object:
    name = "rquant.factor.industry_source"
    assert importlib.util.find_spec(name) is not None, "industry source is missing"
    return importlib.import_module(name)


def _receipt(request: object, *, count: int = 1) -> SourceTransportUsageReceipt:
    identifier = f"{request.api_name}:{request.l1_code}:{request.is_new}"
    return SourceTransportUsageReceipt(
        source="synthetic-tushare",
        logical_request_id=identifier,
        actual_call_count=count,
        call_receipts=tuple(
            SourceTransportCallReceipt(
                source="synthetic-tushare",
                logical_request_id=identifier,
                api_name=request.api_name,
                call_ordinal=n,
                attempt_id=hashlib.sha256(f"{identifier}:{n}".encode()).hexdigest(),
                outcome=SourceQuotaAttemptOutcome.SUCCESS,
                dispatched_at=_CAPTURED,
                committed_at=_CAPTURED,
            )
            for n in range(1, count + 1)
        ),
    )


def _member(code: int, start: str, end: str | None = None, *, name: str = "旧行业名") -> dict:
    return {
        "l1_name": name,
        "l2_code": "801011.SI",
        "l2_name": "二级",
        "l3_code": "850111.SI",
        "l3_name": "三级",
        "ts_code": f"{code:06d}.SZ",
        "name": "证券原名",
        "in_date": start,
        "out_date": end,
    }


def _capture(m: object, request: object, *, industries: int = 2, fault: str = "") -> object:
    if request.api_name == "index_classify":
        rows = [
            {
                "index_code": f"{801010 + n:06d}.SI",
                "industry_name": f"行业{n}",
                "parent_code": "0",
                "level": "L1",
                "industry_code": str(110000 + n),
                "is_pub": "1",
                "src": "SW2021",
            }
            for n in range(industries)
        ]
    else:
        data = {
            ("801010.SI", "Y"): [
                _member(3, "20100101"),
                _member(4, "20110101"),
                _member(4, "20200101"),
                _member(5, "20200101"),
                _member(7, "20260701"),
            ],
            ("801010.SI", "N"): [
                _member(1, "20100101", "20260702"),
                _member(2, "20100101", "20260701"),
                _member(8, "20100101", "20260702"),
            ],
            ("801011.SI", "Y"): [
                _member(1, "20260702"),
                _member(2, "20260701"),
                _member(5, "20200101"),
            ],
        }
        rows = [
            dict(row, l1_code=request.l1_code, is_new=request.is_new)
            for row in data.get((request.l1_code, request.is_new), [])
        ]
        if fault == "truncated":
            rows = [
                dict(_member(1, "20100101"), l1_code=request.l1_code, is_new=request.is_new)
            ] * 2000
        if fault and rows:
            if fault == "wrong_industry":
                rows[0]["l1_code"] = "899999.SI"
            if fault == "wrong_state":
                rows[0]["is_new"] = "N" if request.is_new == "Y" else "Y"
            if fault == "illegal_date":
                rows[0]["in_date"] = "20260230"
            if fault == "reversed_interval":
                rows[0].update(in_date="20260801", out_date="20260701")
    frame = pd.DataFrame(rows, columns=request.fields)
    if fault == "missing_field":
        frame = frame.drop(columns=request.fields[0])
    return m.make_industry_capture(
        request,
        frame,
        requested_at=_CAPTURED,
        observed_at=_CAPTURED,
        transport_receipt=_receipt(request),
    )


def _collect(tmp_path: Path, *, industries: int = 2) -> tuple[object, Path]:
    m = _module()
    root = tmp_path / "capture"
    result = m.collect_industry_sources(
        m.IndustryCaptureRequest(root=root),
        fetch=lambda request: _capture(m, request, industries=industries),
    )
    return result, root


def _source(tmp_path: Path) -> tuple[object, object, Path, Path]:
    m = _module()
    _, root = _collect(tmp_path)
    prepared = _prepared(tmp_path)
    lake = tmp_path / "industry-lake"
    source = m.prepare_factor_industry_source(
        m.FactorIndustryPrepareRequest(prepared_source=prepared, collection_root=root),
        lake_root=lake,
        now=lambda: _AS_OF,
    )
    return source, prepared, lake, root


def _query(m: object, source: object, *, offset: int = 0, codes: tuple = _CODES) -> object:
    return m.FactorIndustryQuery(
        source_sha256=source.sha256,
        trade_date=_FIRST + timedelta(days=offset),
        stock_codes=codes,
    )


def test_complete_31_industries_reuses_three_imports_and_dispatches_only_60(tmp_path: Path) -> None:
    m = _module()
    imports = tmp_path / "imports"
    imports.mkdir(mode=0o700)
    requests = (
        m.IndustrySourceRequest(api_name="index_classify"),
        m.IndustrySourceRequest(api_name="index_member_all", l1_code="801010.SI", is_new="Y"),
        m.IndustrySourceRequest(api_name="index_member_all", l1_code="801010.SI", is_new="N"),
    )
    paths = tuple(imports / f"{n}.json" for n in range(3))
    originals = []
    for path, request in zip(paths, requests, strict=True):
        data = _bytes(_capture(m, request, industries=31))
        path.write_bytes(data)
        path.chmod(0o600)
        originals.append(data)
    calls = []

    def fetch(request: object) -> object:
        calls.append(request)
        return _capture(m, request, industries=31)

    root = tmp_path / "complete"
    manifest = m.collect_industry_sources(
        m.IndustryCaptureRequest(root=root, import_paths=paths),
        fetch=fetch,
    )
    assert len(calls) == 60 and len(manifest.responses) == 63
    assert manifest.actual_call_count == 63
    assert manifest.imported_call_count == 3 and manifest.new_call_count == 60
    assert m.load_industry_collection(root) == manifest
    imported = [ref for ref in manifest.responses if ref.origin == "imported"]
    assert len(imported) == 3
    assert sorted((root / ref.filename).read_bytes() for ref in imported) == sorted(originals)
    assert all(ref.observed_at == _CAPTURED for ref in imported)


def test_complete_import_only_does_not_create_http_or_retake_sources(tmp_path: Path) -> None:
    m = _module()
    original, old_root = _collect(tmp_path)
    paths = tuple(old_root / ref.filename for ref in original.responses)
    root = tmp_path / "import-only"
    result = m.collect_industry_sources(
        m.IndustryCaptureRequest(root=root, import_paths=paths),
        fetch=None,
    )
    assert result.new_call_count == 0 and result.imported_call_count == 5
    with pytest.raises(FileExistsError):
        m.collect_industry_sources(m.IndustryCaptureRequest(root=root), fetch=None)


@pytest.mark.parametrize("fault", ["duplicate", "wrong_directory"])
def test_all_imports_are_checked_before_any_new_dispatch(tmp_path: Path, fault: str) -> None:
    m = _module()
    imports = tmp_path / "imports"
    imports.mkdir(mode=0o700)
    directory = m.IndustrySourceRequest(api_name="index_classify")
    paths = [imports / "directory.json", imports / "second.json"]
    paths[0].write_bytes(_bytes(_capture(m, directory)))
    paths[0].chmod(0o600)
    request = (
        directory
        if fault == "duplicate"
        else m.IndustrySourceRequest(
            api_name="index_member_all",
            l1_code="899999.SI",
            is_new="Y",
        )
    )
    paths[1].write_bytes(_bytes(_capture(m, request)))
    paths[1].chmod(0o600)
    calls = []
    with pytest.raises(ValueError):
        m.collect_industry_sources(
            m.IndustryCaptureRequest(root=tmp_path / "bad", import_paths=tuple(paths)),
            fetch=lambda request: calls.append(request),
        )
    assert not calls and not (tmp_path / "bad" / "collection.json").exists()


@pytest.mark.parametrize(
    "fault",
    [
        "missing_field",
        "wrong_industry",
        "wrong_state",
        "illegal_date",
        "reversed_interval",
        "truncated",
    ],
)
def test_bad_provider_schema_intervals_or_limit_cannot_complete(tmp_path: Path, fault: str) -> None:
    m = _module()
    root = tmp_path / "bad-provider"
    with pytest.raises(ValueError):
        m.collect_industry_sources(
            m.IndustryCaptureRequest(root=root),
            fetch=lambda request: _capture(
                m,
                request,
                fault=fault if request.api_name == "index_member_all" else "",
            ),
        )
    assert not (root / "collection.json").exists()


def test_imported_actual_dispatches_count_toward_the_64_call_limit(tmp_path: Path) -> None:
    m = _module()
    imports = tmp_path / "imports"
    imports.mkdir(mode=0o700)
    request = m.IndustrySourceRequest(api_name="index_classify")
    capture = _capture(m, request, industries=31)
    capture = capture.model_copy(update={"transport_receipt": _receipt(request, count=3)})
    path = imports / "directory.json"
    path.write_bytes(_bytes(capture))
    path.chmod(0o600)
    calls = []
    with pytest.raises(ValueError, match="budget|64|dispatch"):
        m.collect_industry_sources(
            m.IndustryCaptureRequest(root=tmp_path / "over-budget", import_paths=(path,)),
            fetch=lambda request: calls.append(request),
        )
    assert not calls


@pytest.mark.parametrize("cancel", [False, True])
def test_failure_after_completion_publication_removes_owned_manifest(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    cancel: bool,
) -> None:
    m = _module()
    publish = m._publish_collection

    def fail(root: Path, descriptor: int, manifest: object) -> None:
        publish(root, descriptor, manifest)
        raise KeyboardInterrupt() if cancel else OSError("after completion publication")

    monkeypatch.setattr(m, "_publish_collection", fail)
    root = tmp_path / "interrupted"
    with pytest.raises(KeyboardInterrupt if cancel else OSError):
        m.collect_industry_sources(
            m.IndustryCaptureRequest(root=root),
            fetch=lambda request: _capture(m, request),
        )
    assert not (root / "collection.json").exists()
    with pytest.raises((ValueError, FileNotFoundError)):
        m.load_industry_collection(root)
    assert not tuple(root.glob("*.tmp*"))


def test_raw_changed_during_completion_publication_is_refused_and_cleaned(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    m = _module()
    publish = m._publish_collection

    def change(root: Path, descriptor: int, manifest: object) -> None:
        publish(root, descriptor, manifest)
        path = root / manifest.responses[-1].filename
        path.write_bytes(path.read_bytes() + b"\n")

    monkeypatch.setattr(m, "_publish_collection", change)
    root = tmp_path / "changed"
    with pytest.raises(ValueError):
        m.collect_industry_sources(
            m.IndustryCaptureRequest(root=root),
            fetch=lambda request: _capture(m, request),
        )
    assert not (root / "collection.json").exists()


def test_two_days_preserve_old_intervals_changes_overlap_missing_and_unverified_endpoints(
    tmp_path: Path,
) -> None:
    m = _module()
    source, prepared, lake, _ = _source(tmp_path)
    assert source.prepared_source_sha256 == prepared.sha256
    assert source.prepared_snapshot_id == prepared.snapshot.snapshot_id
    assert source.prepared_binding_hash == prepared.binding.binding_hash
    assert source.scope == prepared.receipt.request.scope
    assert source.scope_content_hash == prepared.scope_content_hash
    assert source.code_commit == prepared.snapshot.code_commit
    assert source.source_read_boundary == "captured_api_responses"
    assert source.source_mode == "historical_retrospective" and source.classification == "SW2021"
    assert source.captured_at == _CAPTURED and source.prepared_at == _AS_OF
    assert source.artifact.event_column is None and source.artifact.row_count == 11
    assert m.FactorIndustrySource.model_validate_json(source.model_dump_json()) == source
    with duckdb.connect() as raw:
        rows = raw.execute(
            "SELECT * FROM read_parquet(?)", [str(lake / source.artifact.relative_path)]
        ).fetchall()
        assert len(rows) == 11 and any("旧行业名" in row for row in rows)
    with m.open_factor_industry_source(source, lake_root=lake) as lease:
        first = lease.query(_query(m, source))
        second = lease.query(_query(m, source, offset=1))
        assert [fact.status for fact in first.facts] == [
            "valid",
            "boundary_unverified",
            "valid",
            "valid",
            "ambiguous",
            "missing",
            "boundary_unverified",
            "valid",
        ]
        assert [fact.status for fact in second.facts] == [
            "boundary_unverified",
            "valid",
            "valid",
            "valid",
            "ambiguous",
            "missing",
            "valid",
            "boundary_unverified",
        ]
        assert first.facts[0].l1_code == "801010.SI"
        assert second.facts[1].l1_code == "801011.SI"
        assert first.facts[2].l1_name == "行业0"
        assert first.counts.model_dump() == dict(
            valid=4, missing=1, ambiguous=1, boundary_unverified=2
        )
        assert second.counts == first.counts
        assert all(
            fact.l1_code is None and fact.l1_name is None
            for fact in first.facts
            if fact.status != "valid"
        )


@pytest.mark.parametrize("fault", ["late_capture", "paired_digest", "raw_changed", "incomplete"])
def test_prepare_rejects_asof_pairing_or_incomplete_changed_collection(
    tmp_path: Path, fault: str
) -> None:
    m = _module()
    manifest, root = _collect(tmp_path)
    prepared = _prepared(tmp_path)
    if fault == "late_capture":
        m.collect_industry_sources(
            m.IndustryCaptureRequest(root=tmp_path / "future"),
            fetch=lambda request: _capture(m, request).model_copy(
                update={
                    "requested_at": _AS_OF + timedelta(days=1),
                    "observed_at": _AS_OF + timedelta(days=1),
                    "transport_receipt": _receipt(request).model_copy(
                        update={
                            "call_receipts": tuple(
                                call.model_copy(
                                    update={
                                        "dispatched_at": _AS_OF + timedelta(days=1),
                                        "committed_at": _AS_OF + timedelta(days=1),
                                    }
                                )
                                for call in _receipt(request).call_receipts
                            ),
                        }
                    ),
                }
            ),
        )
        root = tmp_path / "future"
    if fault == "paired_digest":
        prepared = prepared.model_copy(update={"sha256": "0" * 64})
    if fault == "raw_changed":
        path = root / manifest.responses[1].filename
        path.write_bytes(path.read_bytes() + b"\n")
    if fault == "incomplete":
        (root / manifest.responses[-1].filename).unlink()
    with pytest.raises((ValueError, FileNotFoundError)):
        m.prepare_factor_industry_source(
            m.FactorIndustryPrepareRequest(prepared_source=prepared, collection_root=root),
            lake_root=tmp_path / "refused-lake",
            now=lambda: _AS_OF,
        )


@pytest.mark.parametrize("fault", ["another_source", "outside_code", "outside_date", "too_many"])
def test_one_day_query_requires_the_bound_scope_and_at_most_500_codes(
    tmp_path: Path, fault: str
) -> None:
    m = _module()
    source, _, lake, _ = _source(tmp_path)
    query = _query(m, source)
    updates = {
        "another_source": {"source_sha256": "0" * 64},
        "outside_code": {"stock_codes": ("999999.SZ",)},
        "outside_date": {"trade_date": _FIRST - timedelta(days=1)},
        "too_many": {"stock_codes": tuple(f"{n:06d}.SZ" for n in range(1, 502))},
    }
    with m.open_factor_industry_source(source, lake_root=lake) as lease, pytest.raises(ValueError):
        lease.query(query.model_copy(update=updates[fault]))


@pytest.mark.parametrize("cancel", [False, True])
def test_private_reader_survives_original_change_and_closes_after_normal_or_exception_exit(
    tmp_path: Path,
    cancel: bool,
) -> None:
    m = _module()
    source, prepared, lake, root = _source(tmp_path)
    lease = None
    try:
        with m.open_factor_industry_source(source, lake_root=lake) as lease:
            private_root = lease._private_root
            connection = lease._connection
            expected = lease.query(_query(m, source))
            prepared.receipt.request.replica_path.unlink()
            (root / "collection.json").unlink()
            (lake / source.artifact.relative_path).write_bytes(b"changed original")
            assert lease.query(_query(m, source)) == expected
            if cancel:
                raise RuntimeError("consumer failed")
    except RuntimeError as exc:
        assert cancel and str(exc) == "consumer failed"
    assert lease is not None and not private_root.exists()
    with pytest.raises(RuntimeError, match="closed"):
        lease.query(_query(m, source))
    with pytest.raises(duckdb.ConnectionException):
        connection.execute("SELECT 1")
    assert not tuple(lake.glob(".industry-*"))


def test_corrupted_parquet_is_refused_before_reader_yields(tmp_path: Path) -> None:
    m = _module()
    source, _, lake, _ = _source(tmp_path)
    (lake / source.artifact.relative_path).write_bytes(b"bad parquet")
    with pytest.raises(ValueError), m.open_factor_industry_source(source, lake_root=lake):
        pytest.fail("corrupt source yielded")
    assert not tuple(lake.glob(".industry-reader-*"))


def test_ordinary_module_import_does_not_initialize_settings_or_network(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from rquant.config import Settings

    def forbidden(*args: object, **kwargs: object) -> None:
        pytest.fail("ordinary industry import initialized settings or network")

    monkeypatch.setattr(Settings, "__init__", forbidden)
    monkeypatch.setattr(socket, "socket", forbidden)
    importlib.reload(_module())
