"""Read-only provider evidence must establish a complete historical market."""

from __future__ import annotations

import importlib
import importlib.util
import json
import os
import subprocess
import sys
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest

from rquant.factor.universe import select_factor_universe
from rquant.strict_json import canonical_json_bytes

_DAY = date(2026, 9, 29)
_OBSERVED = datetime(2026, 10, 1, 7, tzinfo=UTC)
_REFERENCE_FIELDS = (
    "ts_code",
    "name",
    "exchange",
    "curr_type",
    "market",
    "list_status",
    "list_date",
    "delist_date",
)
_DAILY_FIELDS = ("trade_date", "ts_code", "name", "list_date")


def _module() -> object:
    name = "rquant.factor.security_collect"
    assert importlib.util.find_spec(name) is not None, "historical security collection is missing"
    return importlib.import_module(name)


def _reference(
    code: str,
    market: str,
    *,
    status: str = "L",
    listed: str | None = "20200102",
    delisted: str | None = None,
) -> tuple[object, ...]:
    exchange = {"SZ": "SZSE", "SH": "SSE", "BJ": "BSE"}[code[-2:]]
    return (code, "当前名称", exchange, "CNY", market, status, listed, delisted)


def _capture(
    api: str,
    rows: tuple[tuple[object, ...], ...],
    *,
    status: str | None = None,
    day: date = _DAY,
    fields: tuple[str, ...] | None = None,
    observed: datetime = _OBSERVED,
) -> object:
    m = _module()
    requested_fields = _REFERENCE_FIELDS if api == "stock_basic" else _DAILY_FIELDS
    request = m.SecuritySourceRequest(
        api_name=api,
        fields=requested_fields,
        list_status=status,
        exchange="" if api == "stock_basic" else None,
        trade_date=day if api == "bak_basic" else None,
    )
    return m.make_security_capture(
        request,
        pd.DataFrame(rows, columns=fields or requested_fields),
        requested_at=observed - timedelta(seconds=1),
        observed_at=observed,
    )


def _references(*rows: tuple[object, ...]) -> tuple[object, ...]:
    return tuple(
        _capture("stock_basic", tuple(row for row in rows if row[5] == status), status=status)
        for status in ("L", "D", "P", "G", "UN")
    )


def _daily(*rows: tuple[object, ...], day: date = _DAY) -> object:
    return _capture("bak_basic", tuple((day.strftime("%Y%m%d"), *row) for row in rows), day=day)


def test_historical_names_boards_and_listing_boundaries_drive_existing_pools() -> None:
    m = _module()
    references = _references(
        _reference("000001.SZ", "主板"),
        _reference("300001.SZ", "创业板", listed="20260929"),
        _reference("688001.SH", "科创板"),
        _reference("920001.BJ", "北交所"),
        _reference("000002.SZ", "主板", listed="20260930"),
        _reference("000003.SZ", "主板", status="D", delisted="20260929"),
        _reference("301569.SZ", "创业板", status="UN", listed=None),
        _reference("T600018.SH", None, status="D", listed="20000719", delisted="20061020"),
    )
    daily = _daily(
        ("000001.SZ", " ＳＴ 旧名称 ", "20200102"),
        ("300001.SZ", "上市首日", "20260929"),
        ("688001.SH", "历史非ST", "20200102"),
        ("920001.BJ", "北交所", "20200102"),
        ("000002.SZ", "未来上市", "0"),
        ("000003.SZ", "退市", "20200102"),
        ("301569.SZ", "未上市", "0"),
    )
    result = m.normalize_security_day(daily, references)
    assert result.accepted, result.diagnostics
    assert result.batch.complete_stock_codes == (
        "000001.SZ",
        "300001.SZ",
        "688001.SH",
        "920001.BJ",
    )
    assert result.batch.observed_at == _OBSERVED
    assert result.batch.source_mode == "historical_retrospective"
    from rquant.factor.universe import FactorUniverseRequest

    all_result = select_factor_universe(
        FactorUniverseRequest(
            selection="all",
            trade_date=_DAY,
            as_of=_OBSERVED,
            securities=result.batch,
        )
    )
    gem_result = select_factor_universe(
        FactorUniverseRequest(
            selection="gem",
            trade_date=_DAY,
            as_of=_OBSERVED,
            securities=result.batch,
        )
    )
    assert all_result.stock_codes == gem_result.stock_codes == ("300001.SZ", "688001.SH")
    assert all_result.excluded.st == 1 and all_result.excluded.beijing == 1
    assert any(
        issue.stock_code == "T600018.SH" and issue.severity == "info"
        for issue in result.diagnostics
    )


@pytest.mark.parametrize(
    "fault",
    [
        "missing_history",
        "missing_name",
        "unknown_listing",
        "conflicting_date",
        "wrong_trade_date",
        "duplicate_history",
        "duplicate_reference",
        "missing_partition",
        "missing_columns",
        "empty_history",
        "unlisted_contradiction",
        "wrong_status",
        "missing_delisting",
        "future_observation",
    ],
)
def test_incomplete_or_conflicting_sources_refuse_the_whole_date(fault: str) -> None:
    m = _module()
    ref = _reference("300001.SZ", "创业板")
    rows = [("300001.SZ", "历史名称", "20200102")]
    if fault == "missing_history":
        rows = [("300002.SZ", "未知代码", "20200102")]
    elif fault == "missing_name":
        rows[0] = ("300001.SZ", None, "20200102")
    elif fault == "unknown_listing":
        ref = _reference("300001.SZ", "创业板", listed=None)
    elif fault == "conflicting_date":
        rows[0] = ("300001.SZ", "历史名称", "20200103")
    elif fault == "duplicate_history":
        rows *= 2
    elif fault == "empty_history":
        rows = []
    elif fault == "unlisted_contradiction":
        ref = _reference("300001.SZ", "创业板", status="UN", listed=None)
    elif fault == "missing_delisting":
        ref = _reference("300001.SZ", "创业板", status="D")
    refs = _references(ref)
    if fault == "duplicate_reference":
        refs += (_capture("stock_basic", (ref,), status="L"),)
    elif fault == "missing_partition":
        refs = refs[:-1]
    elif fault == "wrong_status":
        refs = (
            _capture("stock_basic", (_reference("300001.SZ", "创业板", status="P"),), status="L"),
            *refs[1:],
        )
    elif fault == "future_observation":
        refs = tuple(
            _capture(
                "stock_basic",
                (ref,) if s == "L" else (),
                status=s,
                observed=datetime(2026, 9, 28, 7, tzinfo=UTC),
            )
            for s in ("L", "D", "P", "G", "UN")
        )
    daily = _daily(*rows)
    if fault == "wrong_trade_date":
        daily = _capture("bak_basic", (("20260928", *rows[0]),))
    elif fault == "missing_columns":
        daily = _capture(
            "bak_basic", (("20260929", "300001.SZ", "历史名称"),), fields=_DAILY_FIELDS[:-1]
        )
    result = m.normalize_security_day(daily, refs)
    assert not result.accepted and result.batch is None
    assert any(issue.severity == "error" for issue in result.diagnostics)
    assert result.reason and len(result.reason) < 100


def test_missing_historical_name_preserves_unknown_and_depends_on_selected_pool() -> None:
    m = _module()
    refs = _references(_reference("301139.SZ", "创业板"), _reference("688001.SH", "科创板"))
    daily = _daily(("688001.SH", "历史名称", "20200102"))
    rejected = m.normalize_security_day(daily, refs, selection="all")
    accepted = m.normalize_security_day(daily, refs, selection="gem")
    assert not rejected.accepted and rejected.reason == "当日历史股票名单不完整"
    assert accepted.accepted
    facts = {fact.stock_code: fact for fact in accepted.batch.facts}
    assert facts["301139.SZ"].is_st is None
    assert accepted.batch.complete_stock_codes == ("301139.SZ", "688001.SH")


def test_unknown_board_is_retained_and_refused_only_when_pool_needs_it() -> None:
    m = _module()
    refs = _references(_reference("300001.SZ", "未知"))
    daily = _daily(("300001.SZ", "历史名称", "20200102"))
    accepted = m.normalize_security_day(daily, refs, selection="all")
    rejected = m.normalize_security_day(daily, refs, selection="gem")
    assert accepted.accepted and accepted.batch.facts[0].board is None
    assert not rejected.accepted


@pytest.mark.parametrize("api,limit", [("stock_basic", 6000), ("bak_basic", 7000)])
def test_provider_limit_is_a_refusal_not_a_completeness_claim(api: str, limit: int) -> None:
    m = _module()
    ref = _reference("300001.SZ", "创业板")
    refs = _references(ref)
    daily = _daily(("300001.SZ", "历史名称", "20200102"))
    if api == "stock_basic":
        refs = (_capture(api, (ref,) * limit, status="L"), *refs[1:])
    else:
        daily = _daily(*(("300001.SZ", "历史名称", "20200102"),) * limit)
    result = m.normalize_security_day(daily, refs)
    assert not result.accepted
    assert any(issue.reason == "provider_limit" for issue in result.diagnostics)


def test_actual_capture_values_hashes_and_maximum_observed_time_are_bound() -> None:
    m = _module()
    refs = _references(
        _reference("300001.SZ", "创业板"),
        _reference("301569.SZ", "创业板", status="UN", listed=None),
    )
    daily = _daily(("300001.SZ", "历史名称", "20200102"), ("301569.SZ", "未上市", "0"))
    assert daily.response.items[-1][-1] == "0"
    result = m.normalize_security_day(daily, refs)
    later = daily.model_copy(update={"observed_at": _OBSERVED + timedelta(seconds=1)})
    changed = m.normalize_security_day(later, refs)
    assert result.batch.source_sha256 != changed.batch.source_sha256
    assert changed.batch.observed_at == _OBSERVED + timedelta(seconds=1)
    with pytest.raises(ValueError, match="digest"):
        type(daily).model_validate(daily.model_copy(update={"response_sha256": "0" * 64}))


def _adapter(observer: object, *, fault: str | None = None) -> object:
    from rquant.adapter.tushare import TushareAdapter

    def stock_basic(**params: str) -> pd.DataFrame:
        rows = ()
        if params["list_status"] == "L" and params["exchange"] in ("", "SZSE"):
            rows = (_reference("300001.SZ", "创业板"),)
        return pd.DataFrame(rows, columns=_REFERENCE_FIELDS)

    def bak_basic(**params: str) -> pd.DataFrame:
        if fault == "cancel":
            raise KeyboardInterrupt("offline cancellation")
        if fault == "retry":
            raise RuntimeError("offline transport failure")
        return pd.DataFrame(
            [(params["trade_date"], "300001.SZ", "历史名称", "20200102")], columns=_DAILY_FIELDS
        )

    adapter = TushareAdapter.__new__(TushareAdapter)
    adapter._pro = SimpleNamespace(stock_basic=stock_basic, bak_basic=bak_basic)
    adapter._transport_observer = observer
    adapter._primary_token = "offline"
    adapter._backup_token = ""
    adapter._using_backup = False
    return adapter


def test_raw_adapter_methods_keep_exact_request_and_transport_contract() -> None:
    from rquant.adapter.tushare import TushareAdapter

    calls = []

    def raw(**params: str) -> pd.DataFrame:
        calls.append(params)
        return pd.DataFrame({"raw_only": ["0"]})

    adapter = TushareAdapter.__new__(TushareAdapter)
    adapter._pro = SimpleNamespace(stock_basic=raw, bak_basic=raw)
    assert hasattr(adapter, "stock_basic_history_raw"), "raw reference endpoint missing"
    assert hasattr(adapter, "bak_basic_raw"), "raw daily endpoint missing"
    frame = adapter.stock_basic_history_raw(list_status="UN", exchange="BSE")
    assert frame.columns.tolist() == ["raw_only"]
    assert adapter.bak_basic_raw(_DAY).iloc[0, 0] == "0"
    assert calls == [
        {"exchange": "BSE", "list_status": "UN", "fields": ",".join(_REFERENCE_FIELDS)},
        {"trade_date": "20260929", "fields": ",".join(_DAILY_FIELDS)},
    ]


def test_capture_archive_reader_and_cli_replay_use_existing_contract(tmp_path: Path) -> None:
    m = _module()
    capture_root = tmp_path / "capture"
    request = m.SecurityCaptureRequest(root=capture_root, trading_days=(_DAY,), max_calls=16)
    manifest = m.collect_security_sources(
        request, adapter_factory=_adapter, request_interval_seconds=0
    )
    assert len(manifest.responses) == 16 and manifest.actual_call_count == 16
    assert m.load_security_capture_collection(capture_root) == manifest
    assert (capture_root.stat().st_mode & 0o777) == 0o700
    assert all((path.stat().st_mode & 0o777) == 0o600 for path in capture_root.iterdir())
    with pytest.raises(FileExistsError):
        m.collect_security_sources(request, adapter_factory=_adapter, request_interval_seconds=0)
    results = tuple(m.iter_security_collection_days(capture_root))
    assert len(results) == 1 and results[0].accepted
    from rquant.factor.member_archive import load_factor_member_archive
    from rquant.factor.member_stream import open_factor_member_stream

    for selection in ("all", "gem"):
        root = tmp_path / f"archive-{selection}"
        reference = m.archive_security_collection(
            capture_root,
            selection=selection,
            as_of=datetime.now(UTC),
            input_root=tmp_path / f"inputs-{selection}",
            root=root,
        )
        archive = load_factor_member_archive(root, reference)
        assert archive.days[0].security_payload_sha256
        with open_factor_member_stream(root, reference) as stream:
            rows = [select_factor_universe(row).stock_codes for row in stream]
            assert rows == [("300001.SZ",)]
            stream.require_completion()
    completed = subprocess.run(
        [
            sys.executable,
            "-m",
            "rquant.factor.security_collect",
            "replay",
            "--capture-root",
            str(capture_root),
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    assert completed.returncode == 0, completed.stderr
    receipt = json.loads(completed.stdout)
    assert receipt["days"][0]["accepted"] is True


@pytest.mark.parametrize("fault", ["cancel", "retry"])
def test_interrupted_capture_has_no_completion_and_retries_spend_budget(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    fault: str,
) -> None:
    m = _module()
    import rquant.adapter.tushare as adapter_module

    monkeypatch.setattr(adapter_module.time, "sleep", lambda _: None)
    root = tmp_path / "interrupted"
    request = m.SecurityCaptureRequest(root=root, trading_days=(_DAY,), max_calls=16)
    expected = KeyboardInterrupt if fault == "cancel" else RuntimeError
    with pytest.raises(expected):
        m.collect_security_sources(
            request,
            adapter_factory=lambda observer: _adapter(observer, fault=fault),
            request_interval_seconds=0,
        )
    assert not (root / "collection.json").exists()
    receipt = json.loads((root / "interrupted.json").read_bytes())
    assert receipt["status"] == "interrupted" and receipt["actual_call_count"] == 16
    with pytest.raises(FileNotFoundError):
        m.load_security_capture_collection(root)


def test_explicit_budget_and_date_boundaries_are_checked_before_client_creation(
    tmp_path: Path,
) -> None:
    m = _module()
    for fields in (
        {"trading_days": (_DAY,), "max_calls": 15},
        {"trading_days": (date(2015, 12, 31),), "max_calls": 16},
        {"trading_days": (_DAY, _DAY), "max_calls": 17},
    ):
        with pytest.raises(ValueError):
            m.SecurityCaptureRequest(root=tmp_path / "invalid", **fields)
    assert not (tmp_path / "invalid").exists()


def test_import_unconfigured_module_does_not_load_settings_or_create_client(tmp_path: Path) -> None:
    _module()
    child = """
import importlib, sys
import rquant.config as config
def forbidden():
    raise AssertionError('settings initialized by import')
config.get_settings = forbidden
import rquant.factor.security_collect
assert 'rquant.adapter.tushare' not in sys.modules
"""
    env = {
        "PATH": os.environ["PATH"],
        "PYTHONPATH": str(Path(__file__).parents[2] / "src"),
        "RQUANT_DISABLE_DOTENV": "1",
    }
    completed = subprocess.run(
        [sys.executable, "-c", child], env=env, check=False, capture_output=True, text=True
    )
    assert completed.returncode == 0, completed.stderr
    env["TUSHARE_TOKEN_MAIN"] = "never-display-test-value"
    completed = subprocess.run(
        [
            sys.executable,
            "-m",
            "rquant.factor.security_collect",
            "live",
            "--date",
            "2026-09-29",
            "--root",
            str(tmp_path / "invalid-config"),
            "--max-calls",
            "16",
        ],
        env=env,
        check=False,
        capture_output=True,
        text=True,
    )
    assert completed.returncode == 2
    assert "never-display-test-value" not in completed.stdout + completed.stderr


def test_probe_import_checks_actual_metadata_and_never_uses_diagnostic_listing_facts(
    tmp_path: Path,
) -> None:
    m = _module()
    source = tmp_path / "probe"
    source.mkdir(mode=0o700)
    refs = _references(_reference("300001.SZ", "创业板"))
    daily = _daily(("300001.SZ", "历史名称", "20200102"))
    records = []
    for i, capture in enumerate((*refs, daily)):
        filename = f"{i}.json"
        raw = canonical_json_bytes(capture.response.model_dump(mode="json"))
        (source / filename).write_bytes(raw)
        records.append(
            {
                "api_name": capture.request.api_name,
                "params": {"exchange": "", "list_status": capture.request.list_status}
                if capture.request.api_name == "stock_basic"
                else {"trade_date": "20260929"},
                "fields": list(capture.request.fields),
                "requested_at": capture.requested_at.isoformat(),
                "observed_at": capture.observed_at.isoformat(),
                "status": "response_archived",
                "filename": filename,
                "payload_sha256": capture.response_sha256,
                "row_count": len(capture.response.items),
                "listing_facts": [{"list_date": float("nan")}],
            }
        )
    (source / "metadata.json").write_text(json.dumps({"requests": records}))
    root = tmp_path / "imported"
    m.import_security_probes((source,), root=root)
    assert next(m.iter_security_collection_days(root)).accepted
    records[0]["params"]["ts_code"] = "300001.SZ"
    (source / "metadata.json").write_text(json.dumps({"requests": records}))
    with pytest.raises(ValueError, match="scope"):
        m.import_security_probes((source,), root=tmp_path / "filtered-probe")
    del records[0]["params"]["ts_code"]
    records[0]["payload_sha256"] = "0" * 64
    (source / "metadata.json").write_text(json.dumps({"requests": records}))
    with pytest.raises(ValueError, match="digest"):
        m.import_security_probes((source,), root=tmp_path / "tampered")
    assert not (tmp_path / "tampered" / "collection.json").exists()


def test_rejected_date_and_index_selection_never_publish_a_member_manifest(tmp_path: Path) -> None:
    m = _module()
    root = tmp_path / "capture"
    m.collect_security_sources(
        m.SecurityCaptureRequest(root=root, trading_days=(_DAY,), max_calls=16),
        adapter_factory=_adapter,
        request_interval_seconds=0,
    )
    for selection in ("hs300", "zz1000"):
        with pytest.raises(ValueError, match="逐日指数成分"):
            m.archive_security_collection(
                root,
                selection=selection,
                as_of=datetime.now(UTC),
                input_root=tmp_path / f"input-{selection}",
                root=tmp_path / f"archive-{selection}",
            )
        assert not (tmp_path / f"archive-{selection}").exists()
    manifest = m.load_security_capture_collection(root)
    ref = next(ref for ref in manifest.responses if ref.request.api_name == "bak_basic")
    (root / ref.filename).write_bytes(b"{}")
    with pytest.raises(ValueError, match="digest"):
        m.archive_security_collection(
            root,
            selection="all",
            as_of=datetime.now(UTC),
            input_root=tmp_path / "bad-input",
            root=tmp_path / "bad-archive",
        )
    assert not list(tmp_path.glob("bad-archive/factor-member-archive-*.json"))
