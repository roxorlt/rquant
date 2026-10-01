"""Explicit full-history names supplement only already established listed securities."""

from __future__ import annotations

import hashlib
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

from rquant.factor import security_collect as security
from rquant.factor.universe import FactorUniverseRequest, select_factor_universe
from rquant.strict_json import canonical_json_bytes

_CODE = "301139.SZ"
_DAY = date(2026, 9, 30)
_OLD_OBSERVED = datetime(2026, 10, 1, 7, tzinfo=UTC)
_OBSERVED = datetime(2026, 10, 1, 7, 15, 54, 828784, tzinfo=UTC)
_FIELDS = ("ts_code", "name", "start_date", "end_date", "ann_date", "change_reason")
_ROWS = (
    (_CODE, "元道退", "20260930", None, "20260921", "退市整理期"),
    (_CODE, "*ST元道", "20260512", "20260929", "20260509", "*ST"),
    (_CODE, "元道通信", "20220708", "20260511", "20220708", "其他"),
)


def _module() -> object:
    name = "rquant.factor.name_collect"
    assert importlib.util.find_spec(name) is not None, "explicit historical name supplement missing"
    return importlib.import_module(name)


def _name_capture(
    rows: tuple[tuple[object, ...], ...] = _ROWS,
    *,
    code: str = _CODE,
    fields: tuple[str, ...] = _FIELDS,
    observed: datetime = _OBSERVED,
) -> object:
    m = _module()
    return m.make_name_capture(
        m.NameSourceRequest(ts_code=code),
        pd.DataFrame(rows, columns=fields),
        requested_at=observed - timedelta(seconds=1),
        observed_at=observed,
    )


def _securities(
    day: date = _DAY, *, historical_name: str | None = None
) -> tuple[object, tuple[object, ...]]:
    refs = []
    for status in security.LIST_STATUSES:
        rows = ()
        if status == "L":
            rows = (
                (_CODE, "当前名不可用于历史", "SZSE", "CNY", "创业板", "L", "20220708", None),
                ("300001.SZ", "当前名", "SZSE", "CNY", "创业板", "L", "20200102", None),
            )
        refs.append(
            security.make_security_capture(
                security.SecuritySourceRequest(
                    api_name="stock_basic",
                    fields=security.STOCK_REFERENCE_FIELDS,
                    list_status=status,
                    exchange="",
                ),
                pd.DataFrame(rows, columns=security.STOCK_REFERENCE_FIELDS),
                requested_at=_OLD_OBSERVED - timedelta(seconds=1),
                observed_at=_OLD_OBSERVED,
            )
        )
    rows = [(day.strftime("%Y%m%d"), "300001.SZ", "普通名称", "20200102")]
    if historical_name is not None:
        rows.append((day.strftime("%Y%m%d"), _CODE, historical_name, "20220708"))
    daily = security.make_security_capture(
        security.SecuritySourceRequest(
            api_name="bak_basic", fields=security.DAILY_SECURITY_FIELDS, trade_date=day
        ),
        pd.DataFrame(rows, columns=security.DAILY_SECURITY_FIELDS),
        requested_at=_OLD_OBSERVED - timedelta(seconds=1),
        observed_at=_OLD_OBSERVED,
    )
    return daily, tuple(refs)


def _codes(batch: object, selection: str) -> tuple[str, ...]:
    return select_factor_universe(
        FactorUniverseRequest(
            selection=selection,
            trade_date=batch.trade_date,
            as_of=batch.observed_at,
            securities=batch,
        )
    ).stock_codes


def _adapter(observer: object, *, fail: bool = False) -> object:
    from rquant.adapter.tushare import TushareAdapter

    def namechange(**params: str) -> pd.DataFrame:
        assert params == {"ts_code": _CODE, "fields": ",".join(_FIELDS)}
        if fail:
            raise RuntimeError("offline transport failed")
        return pd.DataFrame(_ROWS, columns=_FIELDS)

    adapter = TushareAdapter.__new__(TushareAdapter)
    adapter._pro = SimpleNamespace(namechange=namechange)
    adapter._transport_observer = observer
    adapter._primary_token = "offline"
    adapter._backup_token = ""
    adapter._using_backup = False
    return adapter


def _probe(path: Path, *, fault: str | None = None) -> Path:
    path.mkdir(mode=0o700)
    payload = canonical_json_bytes({"fields": _FIELDS, "items": _ROWS})
    (path / "03-namechange-response.json").write_bytes(payload)
    record = {
        "api_name": "namechange",
        "params": {"ts_code": _CODE},
        "fields": _FIELDS,
        "requested_at": (_OBSERVED - timedelta(seconds=1)).isoformat(),
        "observed_at": _OBSERVED.isoformat(),
        "status": "response_archived",
        "filename": "03-namechange-response.json",
        "payload_sha256": hashlib.sha256(payload).hexdigest(),
        "row_count": 3,
    }
    if fault == "extra_filter":
        record["params"]["start_date"] = "20260901"
    elif fault == "digest":
        record["payload_sha256"] = "0" * 64
    elif fault == "time":
        record["observed_at"] = (_OBSERVED - timedelta(seconds=2)).isoformat()
    (path / "metadata.json").write_bytes(
        canonical_json_bytes(
            {"requests": [{"api_name": "stock_st", "ignored": "not a negative ST fact"}, record]}
        )
    )
    return path


def test_raw_name_adapter_has_no_announcement_filter_and_keeps_actual_columns() -> None:
    from rquant.adapter.tushare import TushareAdapter

    seen = []
    frame = pd.DataFrame({"raw_only": ["0"]})

    def raw(**params: str) -> pd.DataFrame:
        seen.append(params)
        return frame

    adapter = TushareAdapter.__new__(TushareAdapter)
    adapter._pro = SimpleNamespace(namechange=raw)
    assert hasattr(adapter, "namechange_history_raw"), "unfiltered raw history endpoint missing"
    assert adapter.namechange_history_raw(ts_code=_CODE) is frame
    assert seen == [{"ts_code": _CODE, "fields": ",".join(_FIELDS)}]


@pytest.mark.parametrize(
    ("day", "expected_st"),
    [
        (date(2026, 5, 11), False),
        (date(2026, 5, 12), True),
        (date(2026, 9, 29), True),
        (_DAY, False),
        (date(2026, 10, 1), False),
    ],
)
def test_name_intervals_use_inclusive_effective_dates_not_announcement_dates(
    day: date, expected_st: bool
) -> None:
    daily, refs = _securities(day)
    result = security.normalize_security_day(daily, refs, name_sources=(_name_capture(),))
    assert result.accepted, result.diagnostics
    fact = next(fact for fact in result.batch.facts if fact.stock_code == _CODE)
    assert fact.is_st is expected_st
    assert (_CODE in _codes(result.batch, "all")) is not expected_st


def test_supplement_precedes_selection_and_preserves_established_members_and_old_digest() -> None:
    daily, refs = _securities()
    assert not security.normalize_security_day(daily, refs).accepted
    old = security.normalize_security_day(daily, refs, selection="gem").batch
    new = security.normalize_security_day(daily, refs, name_sources=(_name_capture(),)).batch
    assert new is not None
    assert new.complete_stock_codes == old.complete_stock_codes
    assert _codes(new, "gem") == _codes(old, "gem")
    assert new.source_sha256 != old.source_sha256
    assert new.observed_at == _OBSERVED and new.source_mode == "historical_retrospective"
    for before, after in zip(old.facts, new.facts, strict=True):
        assert before.model_dump(exclude={"is_st"}) == after.model_dump(exclude={"is_st"})
    assert security.normalize_security_day(daily, refs, selection="gem").batch == old

    # This baseline was computed before introducing the optional supplement.
    from tests.unit.test_factor_security_collect import _daily, _reference, _references

    baseline = security.normalize_security_day(
        _daily(("300001.SZ", "历史名称", "20200102")),
        _references(_reference("300001.SZ", "创业板")),
        name_sources=(),
    )
    assert baseline.batch.source_sha256 == (
        "289e611e9740e19891dec373aa204642e9e3ff1ca959d6664bf492746df1e660"
    )


def test_only_used_listed_name_evidence_changes_digest_or_observation() -> None:
    daily, refs = _securities(historical_name="元道退")
    old = security.normalize_security_day(daily, refs).batch
    unused = _name_capture((("001001.SZ", "无关", "20200102", None, None, None),), code="001001.SZ")
    result = security.normalize_security_day(daily, refs, name_sources=(unused,))
    assert result.batch == old
    same = security.normalize_security_day(daily, refs, name_sources=(_name_capture(),))
    assert same.batch.facts == old.facts
    assert same.batch.source_sha256 != old.source_sha256
    assert same.batch.observed_at == _OBSERVED


def test_nonempty_daily_historical_name_conflict_refuses_entire_day() -> None:
    daily, refs = _securities(historical_name="*ST元道")
    result = security.normalize_security_day(
        daily, refs, selection="gem", name_sources=(_name_capture(),)
    )
    assert not result.accepted
    assert any(issue.reason == "name_daily_conflict" for issue in result.diagnostics)


@pytest.mark.parametrize("fault", ["no_cover", "overlap", "bad_row"])
def test_uninterpretable_name_intervals_never_infer_non_st(fault: str) -> None:
    rows = {
        "no_cover": ((_CODE, "未来名称", "20261002", None, "20260901", None),),
        "overlap": (*_ROWS, (_CODE, "另一个名称", "20260930", None, None, None)),
        "bad_row": (*_ROWS, (_CODE, "坏日期", "0", None, None, None)),
    }[fault]
    daily, refs = _securities()
    capture = _name_capture(rows)
    result = security.normalize_security_day(daily, refs, name_sources=(capture,))
    assert not result.accepted
    gem = security.normalize_security_day(daily, refs, selection="gem", name_sources=(capture,))
    if fault == "no_cover":
        assert gem.accepted
        assert next(f for f in gem.batch.facts if f.stock_code == _CODE).is_st is None
    else:
        assert not gem.accepted


@pytest.mark.parametrize("fault", ["wrong_code", "missing_field", "bad_sha"])
def test_name_raw_envelope_rejects_wrong_code_columns_and_digest(fault: str) -> None:
    m = _module()
    if fault == "wrong_code":
        with pytest.raises(ValueError):
            _name_capture((("300001.SZ", *_ROWS[0][1:]),))
    elif fault == "missing_field":
        with pytest.raises(ValueError):
            _name_capture(tuple(row[:-1] for row in _ROWS), fields=_FIELDS[:-1])
    else:
        payload = _name_capture().model_dump()
        payload["response_sha256"] = "0" * 64
        with pytest.raises(ValueError):
            m.CapturedNameResponse.model_validate(payload)


def test_live_name_capture_binds_raw_values_times_dispatch_and_private_completion(
    tmp_path: Path,
) -> None:
    m = _module()
    root = tmp_path / "names"
    times = iter((_OBSERVED - timedelta(seconds=1), _OBSERVED))
    request = m.NameCaptureRequest(root=root, stock_codes=(_CODE,), max_calls=1)
    manifest = m.collect_name_sources(
        request, adapter_factory=_adapter, clock=lambda: next(times), request_interval_seconds=0
    )
    assert manifest.actual_call_count == 1 and manifest.stock_codes == (_CODE,)
    assert m.load_name_capture_collection(root) == manifest
    capture = next(m.iter_name_captures(root))
    assert capture.response.items == _ROWS and capture.response.fields == _FIELDS
    assert capture.observed_at == _OBSERVED
    assert (
        capture.response_sha256
        == "5fd55bd90c5a4bc6be9b6b9ba7f32d9cb236b096e8c3f81c09a9baa4e85a1326"
    )
    assert (root.stat().st_mode & 0o777) == 0o700
    assert all((path.stat().st_mode & 0o777) == 0o600 for path in root.iterdir())
    with pytest.raises(FileExistsError):
        m.collect_name_sources(request, adapter_factory=_adapter, request_interval_seconds=0)


def test_name_limits_reject_before_client_and_retry_dispatch_spends_budget(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    m = _module()
    for codes, budget in [
        ((), 1),
        ((_CODE, _CODE), 2),
        (tuple(f"{i:06d}.SZ" for i in range(17)), 17),
        ((_CODE,), 65),
        ((_CODE, "688001.SH"), 1),
    ]:
        with pytest.raises(ValueError):
            m.NameCaptureRequest(root=tmp_path / "invalid", stock_codes=codes, max_calls=budget)
    import rquant.adapter.tushare as adapter_module
    from rquant.source_quota_store import SourceQuotaExhaustedError

    monkeypatch.setattr(adapter_module.time, "sleep", lambda _: None)
    root = tmp_path / "retry"
    with pytest.raises(SourceQuotaExhaustedError):
        m.collect_name_sources(
            m.NameCaptureRequest(root=root, stock_codes=(_CODE,), max_calls=2),
            adapter_factory=lambda observer: _adapter(observer, fail=True),
            request_interval_seconds=0,
        )
    assert json.loads((root / "interrupted.json").read_bytes())["actual_call_count"] == 2
    assert not (root / "collection.json").exists()
    with pytest.raises(ValueError, match="采集中断"):
        m.load_name_capture_collection(root)


def test_name_row_and_byte_limits_are_resource_refusals() -> None:
    m = _module()
    with pytest.raises(ValueError):
        _name_capture((_ROWS[0],) * (m.MAX_NAME_ROWS + 1))
    with pytest.raises(ValueError):
        _name_capture(((_CODE, "名" * m.MAX_NAME_BYTES, "20200102", None, None, None),))


def test_live_name_cli_uses_explicit_factory_and_reports_completion(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    m = _module()
    monkeypatch.setattr(m, "_live_adapter", _adapter)
    assert (
        m.main(["live", "--code", _CODE, "--root", str(tmp_path / "names"), "--max-calls", "1"])
        == 0
    )
    assert json.loads(capsys.readouterr().out)["actual_call_count"] == 1


def test_live_name_cli_failure_never_prints_configuration_values(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    m = _module()

    def fail(observer: object) -> object:
        raise RuntimeError("offline-secret-config-value")

    monkeypatch.setattr(m, "_live_adapter", fail)
    assert (
        m.main(["live", "--code", _CODE, "--root", str(tmp_path / "names"), "--max-calls", "1"])
        == 2
    )
    receipt = capsys.readouterr().err
    assert "offline-secret-config-value" not in receipt
    assert json.loads(receipt)["status"] == "refused"


def test_probe_name_import_retains_actual_evidence_and_ignores_stock_st(tmp_path: Path) -> None:
    m = _module()
    root = tmp_path / "import"
    source = _probe(tmp_path / "probe")
    completed = subprocess.run(
        [
            sys.executable,
            "-m",
            "rquant.factor.name_collect",
            "import-probes",
            "--source-root",
            str(source),
            "--root",
            str(root),
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    assert completed.returncode == 0, completed.stderr
    manifest = m.load_name_capture_collection(root)
    assert manifest.stock_codes == (_CODE,) and len(manifest.responses) == 1
    capture = next(m.iter_name_captures(root))
    assert capture.response.items == _ROWS and capture.observed_at == _OBSERVED
    assert capture.request.ts_code == _CODE


@pytest.mark.parametrize("fault", ["extra_filter", "digest", "time"])
def test_probe_name_import_rejects_filtered_or_misbound_evidence(
    tmp_path: Path, fault: str
) -> None:
    m = _module()
    root = tmp_path / "import"
    with pytest.raises(ValueError):
        m.import_name_probes((_probe(tmp_path / "probe", fault=fault),), root=root)
    assert not (root / "collection.json").exists()
    with pytest.raises(ValueError, match="采集中断"):
        m.load_name_capture_collection(root)


@pytest.mark.parametrize("entry", ["live", "probe"])
def test_name_completion_and_loader_reuse_interruption_refusal(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, entry: str
) -> None:
    m = _module()
    root = tmp_path / "interrupted"
    write_all = security._write_all

    def interrupt_completion(descriptor: int, data: bytes) -> None:
        write_all(descriptor, data)
        if b'"status":"captured"' in data:
            raise KeyboardInterrupt("after completion bytes, before fsync")

    monkeypatch.setattr(security, "_write_all", interrupt_completion)
    with pytest.raises(KeyboardInterrupt):
        if entry == "live":
            m.collect_name_sources(
                m.NameCaptureRequest(root=root, stock_codes=(_CODE,), max_calls=1),
                adapter_factory=_adapter,
                request_interval_seconds=0,
            )
        else:
            m.import_name_probes((_probe(tmp_path / "probe"),), root=root)
    assert not (root / "collection.json").exists()
    assert not list(root.glob("*.tmp"))
    with pytest.raises(ValueError, match="采集中断"):
        m.load_name_capture_collection(root)

    # Even intact completion bytes cannot override a recorded interruption.
    monkeypatch.setattr(security, "_write_all", write_all)
    valid = tmp_path / "valid"
    m.import_name_probes((_probe(tmp_path / "valid-probe"),), root=valid)
    (valid / "interrupted.json").write_bytes(canonical_json_bytes({"status": "interrupted"}))
    with pytest.raises(ValueError, match="采集中断"):
        tuple(m.iter_name_captures(valid))


def test_optional_name_replay_and_archive_are_consumable_by_existing_reader(tmp_path: Path) -> None:
    m = _module()
    names = tmp_path / "names"
    m.import_name_probes((_probe(tmp_path / "probe"),), root=names)

    def adapter_factory(observer: object) -> object:
        from rquant.adapter.tushare import TushareAdapter

        daily, refs = _securities()
        by_status = {ref.request.list_status: ref.response for ref in refs}

        def stock_basic(**params: str) -> pd.DataFrame:
            table = by_status[params["list_status"]]
            rows = [row for row in table.items if row[2] == params["exchange"]]
            return pd.DataFrame(rows, columns=table.fields)

        adapter = TushareAdapter.__new__(TushareAdapter)
        adapter._pro = SimpleNamespace(
            stock_basic=stock_basic,
            bak_basic=lambda **_: pd.DataFrame(daily.response.items, columns=daily.response.fields),
        )
        adapter._transport_observer = observer
        return adapter

    sources = tmp_path / "sources"
    security.collect_security_sources(
        security.SecurityCaptureRequest(root=sources, trading_days=(_DAY,), max_calls=16),
        adapter_factory=adapter_factory,
        clock=lambda: _OLD_OBSERVED,
        request_interval_seconds=0,
    )
    assert not next(security.iter_security_collection_days(sources)).accepted
    batch = next(security.iter_security_collection_days(sources, name_root=names)).batch
    assert batch is not None and _CODE in _codes(batch, "all")
    completed = subprocess.run(
        [
            sys.executable,
            "-m",
            "rquant.factor.security_collect",
            "replay",
            "--capture-root",
            str(sources),
            "--name-root",
            str(names),
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    assert completed.returncode == 0, completed.stderr
    assert json.loads(completed.stdout)["days"][0]["observed_at"] == _OBSERVED.isoformat()
    from rquant.factor.member_stream import open_factor_member_stream

    archive = tmp_path / "archive"
    ref = security.archive_security_collection(
        sources,
        selection="all",
        as_of=_OBSERVED,
        input_root=tmp_path / "inputs",
        root=archive,
        name_root=names,
    )
    with open_factor_member_stream(archive, ref) as stream:
        rows = list(stream)
        stream.require_completion()
    assert rows[0].securities.model_dump(exclude={"source_id", "source_sha256"}) == (
        batch.model_dump(exclude={"source_id", "source_sha256"})
    )
    assert _codes(rows[0].securities, "all") == ("300001.SZ", _CODE)
    payload = json.loads((tmp_path / "inputs" / "security-20260930.json").read_bytes())
    assert payload["securities"]["source_sha256"] == batch.source_sha256


def test_plain_name_module_import_is_unconfigured_and_does_not_initialize_settings() -> None:
    _module()
    code = """
import sys
import rquant.factor.name_collect
assert 'rquant.config' not in sys.modules
assert 'rquant.adapter.tushare' not in sys.modules
"""
    completed = subprocess.run(
        [sys.executable, "-c", code],
        check=False,
        capture_output=True,
        text=True,
        env={
            "PATH": os.environ["PATH"],
            "PYTHONPATH": os.environ["PYTHONPATH"],
            "RQUANT_DISABLE_DOTENV": "1",
        },
    )
    assert completed.returncode == 0, completed.stderr
