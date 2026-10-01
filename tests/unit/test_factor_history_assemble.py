"""Multiple completed captures must bind every explicit day before archive publication."""

from __future__ import annotations

import gc
import importlib
import importlib.util
import json
import os
import subprocess
import sys
import weakref
from collections.abc import Iterator
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest

from rquant.factor import security_collect as security
from rquant.factor.member_archive import FactorMemberDayInput, load_factor_member_archive
from rquant.factor.member_stream import open_factor_member_stream
from rquant.factor.universe import FactorUniverseRequest, select_factor_universe
from rquant.strict_json import canonical_json_bytes

_OBSERVED = datetime(2026, 10, 1, 18, tzinfo=UTC)
_FIRST = date(2026, 8, 17)
_DAYS = tuple(
    _FIRST + timedelta(days=i) for i in range(46) if (_FIRST + timedelta(days=i)).weekday() < 5
)[:32]
_LISTED = _DAYS[-1].strftime("%Y%m%d")


def _module() -> object:
    name = "rquant.factor.history_assemble"
    assert importlib.util.find_spec(name) is not None, "bounded multi-capture assembly missing"
    return importlib.import_module(name)


def _collection(
    root: Path,
    days: tuple[date, ...],
    *,
    missing_name: bool = False,
    codes: tuple[str, ...] | None = None,
) -> Path:
    from rquant.adapter.tushare import TushareAdapter

    def adapter_factory(observer: object) -> object:
        references = (
            (
                ("300001.SZ", "创业板", "20200102"),
                ("688001.SH", "科创板", "20200102"),
                ("000001.SZ", "主板", "20200102"),
                ("920001.BJ", "北交所", "20200102"),
                ("301001.SZ", "创业板", _LISTED),
            )
            if codes is None
            else tuple((code, "主板", "20200102") for code in codes)
        )

        def stock_basic(**params: str) -> pd.DataFrame:
            rows = []
            if params["list_status"] == "L":
                for code, board, listed in references:
                    rows.append(
                        (
                            code,
                            "当前名",
                            {"SZ": "SZSE", "SH": "SSE", "BJ": "BSE"}[code[-2:]],
                            "CNY",
                            board,
                            "L",
                            listed,
                            None,
                        )
                    )
            return pd.DataFrame(rows, columns=security.STOCK_REFERENCE_FIELDS)

        def bak_basic(**params: str) -> pd.DataFrame:
            day = date.fromisoformat(
                datetime.strptime(params["trade_date"], "%Y%m%d").date().isoformat()
            )
            rows = []
            for code, _, listed in references:
                if listed > params["trade_date"]:
                    continue
                name = "*ST历史名称" if code == "688001.SH" and day.day % 2 else "历史名称"
                if missing_name and code == "300001.SZ":
                    name = None
                rows.append((params["trade_date"], code, name, listed))
            return pd.DataFrame(rows, columns=security.DAILY_SECURITY_FIELDS)

        adapter = TushareAdapter.__new__(TushareAdapter)
        adapter._pro = SimpleNamespace(stock_basic=stock_basic, bak_basic=bak_basic)
        adapter._transport_observer = observer
        return adapter

    security.collect_security_sources(
        security.SecurityCaptureRequest(
            root=root, trading_days=days, max_calls=5 + len(days), exchanges=("",)
        ),
        adapter_factory=adapter_factory,
        clock=lambda: _OBSERVED,
        request_interval_seconds=0,
    )
    return root


def _request(
    path: Path, roots: tuple[Path, ...], days: tuple[date, ...] = _DAYS, **changes: object
) -> object:
    values = {
        "capture_roots": roots,
        "trading_days": days,
        "selection": "all",
        "as_of": _OBSERVED,
        "input_root": path / "inputs",
        "root": path / "archive",
    }
    values.update(changes)
    return _module().HistoryAssemblyRequest(**values)


def _members(batch: object, selection: str) -> tuple[str, ...]:
    return select_factor_universe(
        FactorUniverseRequest(
            selection=selection,
            trade_date=batch.trade_date,
            as_of=batch.observed_at,
            securities=batch,
        )
    ).stock_codes


def _no_manifest(path: Path) -> None:
    assert not list(path.glob("factor-member-archive-v1-*.json"))


@pytest.mark.parametrize("selection", ["all", "gem"])
def test_32_days_across_two_batches_match_original_daily_facts_sources_and_members(
    tmp_path: Path, selection: str
) -> None:
    m = _module()
    roots = (
        _collection(tmp_path / "first", _DAYS[:31]),
        _collection(tmp_path / "last", _DAYS[31:]),
    )
    request = _request(tmp_path, roots[::-1], selection=selection)
    reference = m.assemble_history_archive(request)
    manifest = load_factor_member_archive(request.root, reference)
    assert manifest.request.trading_days == _DAYS
    assert manifest.request.computation_stock_codes == (
        "000001.SZ",
        "300001.SZ",
        "301001.SZ",
        "688001.SH",
        "920001.BJ",
    )
    assert security.MAX_CAPTURE_DAYS == 31 and security.MAX_CAPTURE_CALLS == 64
    golden = [
        result.batch
        for root in roots
        for result in security.iter_security_collection_days(root, selection=selection)
    ]
    with open_factor_member_stream(request.root, reference) as stream:
        for expected, universe in zip(golden, stream, strict=True):
            payload = FactorMemberDayInput.model_validate_json(
                (request.input_root / f"security-{expected.trade_date:%Y%m%d}.json").read_bytes()
            )
            assert payload.securities == expected
            assert universe.securities.facts == expected.facts
            assert universe.securities.observed_at == expected.observed_at
            assert _members(universe.securities, selection) == _members(expected, selection)
            assert universe.securities.source_mode == "historical_retrospective"
        stream.require_completion()
        assert stream.completion.processed_days == 32


def test_explicit_request_enforces_existing_date_code_and_batch_bounds(tmp_path: Path) -> None:
    m = _module()
    roots = (tmp_path / "source",)
    for changes in (
        {"capture_roots": roots * 2},
        {"capture_roots": tuple(tmp_path / f"source-{i}" for i in range(35))},
        {"trading_days": ()},
        {"trading_days": _DAYS[::-1]},
        {"trading_days": (_DAYS[0], _DAYS[0])},
        {"trading_days": tuple(_FIRST + timedelta(days=i) for i in range(1025))},
        {"selection": "hs300"},
        {"as_of": _OBSERVED.replace(tzinfo=None)},
    ):
        with pytest.raises(ValueError):
            _request(tmp_path, roots, **changes)
    assert m.MAX_CAPTURE_ROOTS == 34


@pytest.mark.parametrize("fault", ["missing_day", "overlapping_day"])
def test_source_schedule_gaps_and_overlaps_refuse_before_outputs(
    tmp_path: Path, fault: str
) -> None:
    m = _module()
    first = _collection(tmp_path / "first", _DAYS[:2])
    second = _collection(
        tmp_path / "second", _DAYS[1:3] if fault == "overlapping_day" else _DAYS[3:4]
    )
    request = _request(tmp_path, (first, second), _DAYS[:4])
    with pytest.raises(ValueError):
        m.assemble_history_archive(request)
    assert not request.input_root.exists() and not request.root.exists()


@pytest.mark.parametrize("fault", ["bad_raw", "interrupted", "missing_fact", "as_of"])
def test_bad_or_invisible_final_batch_never_creates_outputs(tmp_path: Path, fault: str) -> None:
    m = _module()
    first = _collection(tmp_path / "first", _DAYS[:1])
    second = _collection(tmp_path / "second", _DAYS[1:2], missing_name=fault == "missing_fact")
    if fault == "bad_raw":
        (second / f"bak-basic-{_DAYS[1]:%Y%m%d}.json").write_bytes(b"{}")
    elif fault == "interrupted":
        (second / "interrupted.json").write_bytes(canonical_json_bytes({"status": "interrupted"}))
    request = _request(
        tmp_path,
        (first, second),
        _DAYS[:2],
        as_of=_OBSERVED - timedelta(seconds=1) if fault == "as_of" else _OBSERVED,
    )
    with pytest.raises(ValueError):
        m.assemble_history_archive(request)
    assert not request.input_root.exists() and not request.root.exists()


def test_cross_batch_code_union_cannot_exceed_existing_7000_code_limit(tmp_path: Path) -> None:
    m = _module()
    first = _collection(
        tmp_path / "first", _DAYS[:1], codes=tuple(f"{i:06d}.SZ" for i in range(3501))
    )
    second = _collection(
        tmp_path / "last", _DAYS[1:2], codes=tuple(f"{i:06d}.SZ" for i in range(3501, 7002))
    )
    request = _request(tmp_path, (first, second), _DAYS[:2])
    with pytest.raises(ValueError, match="7000"):
        m.assemble_history_archive(request)
    assert not request.input_root.exists() and not request.root.exists()


@pytest.mark.parametrize("fault", ["daily", "reference", "same_content_replacement"])
def test_changed_sources_after_preflight_are_refused_before_publication(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, fault: str
) -> None:
    m = _module()
    roots = (
        _collection(tmp_path / "first", _DAYS[:1]),
        _collection(tmp_path / "second", _DAYS[1:2]),
    )
    preflight = m._preflight

    def change_source(request: object) -> object:
        preview = preflight(request)
        manifest = security.load_security_capture_collection(roots[0])
        ref = next(
            ref
            for ref in manifest.responses
            if ref.request.api_name == ("stock_basic" if fault == "reference" else "bak_basic")
        )
        path = roots[0] / ref.filename
        if fault == "same_content_replacement":
            data = path.read_bytes()
            path.unlink()
            path.write_bytes(data)
            path.chmod(0o600)
        else:
            path.write_bytes(b"{}")
        return preview

    monkeypatch.setattr(m, "_preflight", change_source)
    request = _request(tmp_path, roots, _DAYS[:2])
    with pytest.raises(ValueError):
        m.assemble_history_archive(request)
    _no_manifest(request.root)


def test_day_binding_is_rechecked_even_if_a_normalizer_returns_another_valid_batch(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    m = _module()
    source = _collection(tmp_path / "first", _DAYS[:2])
    original = m.iter_security_collection_days
    calls = 0

    def change_binding(*args: object, **kwargs: object) -> Iterator[object]:
        nonlocal calls
        calls += 1
        for result in original(*args, **kwargs):
            if calls > 1 and result.trade_date == _DAYS[1]:
                batch = result.batch.model_copy(update={"source_sha256": "f" * 64})
                result = result.model_copy(update={"batch": batch})
            yield result
            del result

    monkeypatch.setattr(m, "iter_security_collection_days", change_binding)
    request = _request(tmp_path, (source,), _DAYS[:2])
    with pytest.raises(ValueError, match="绑定"):
        m.assemble_history_archive(request)
    _no_manifest(request.root)


def test_write_phase_tail_failure_closes_iterator_and_has_no_success_manifest(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    m = _module()
    roots = (
        _collection(tmp_path / "first", _DAYS[:1]),
        _collection(tmp_path / "second", _DAYS[1:2]),
    )
    original = m.iter_security_collection_days
    calls = 0
    closed = []

    def fail_last(root: Path, **kwargs: object) -> Iterator[object]:
        nonlocal calls
        calls += 1
        try:
            if calls == 4:
                raise ValueError("write-phase final batch failed")
            yield from original(root, **kwargs)
        finally:
            closed.append(root)

    monkeypatch.setattr(m, "iter_security_collection_days", fail_last)
    request = _request(tmp_path, roots, _DAYS[:2])
    with pytest.raises(ValueError, match="final batch failed"):
        m.assemble_history_archive(request)
    assert len(closed) == 4
    _no_manifest(request.root)


def test_source_change_during_publisher_tail_read_cannot_publish_success(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    m = _module()
    import rquant.factor.member_archive as archive

    source = _collection(tmp_path / "source", _DAYS[:2])
    raw = source / f"bak-basic-{_DAYS[0]:%Y%m%d}.json"
    original = archive._read_file

    def change_at_tail(*args: object, **kwargs: object) -> object:
        result = original(*args, **kwargs)
        if args[1] == f"security-{_DAYS[1]:%Y%m%d}.json":
            raw.write_bytes(b"{}")
        return result

    monkeypatch.setattr(archive, "_read_file", change_at_tail)
    request = _request(tmp_path, (source,), _DAYS[:2])
    with pytest.raises(ValueError):
        m.assemble_history_archive(request)
    _no_manifest(request.root)


def test_source_change_before_final_manifest_write_removes_owned_completion(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    m = _module()
    import rquant.factor.member_archive as archive

    source = _collection(tmp_path / "source", _DAYS[:2])
    raw = source / f"bak-basic-{_DAYS[0]:%Y%m%d}.json"
    original = archive._publish_bytes
    published = []

    def change_before_manifest(
        root: Path, root_fd: int, name: str, data: bytes, limit: int
    ) -> tuple[int, ...]:
        if name.startswith("factor-member-archive-v1-"):
            raw.write_bytes(b"{}")
        identity = original(root, root_fd, name, data, limit)
        if name.startswith("factor-member-archive-v1-"):
            published.append((name, identity))
        return identity

    monkeypatch.setattr(archive, "_publish_bytes", change_before_manifest)
    request = _request(tmp_path, (source,), _DAYS[:2])
    with pytest.raises(ValueError):
        m.assemble_history_archive(request)
    assert len(published) == 1
    assert raw.read_bytes() == b"{}"
    _no_manifest(request.root)


def test_prior_batch_models_are_released_before_opening_the_next_batch(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    m = _module()
    roots = (
        _collection(tmp_path / "first", _DAYS[:2]),
        _collection(tmp_path / "second", _DAYS[2:4]),
    )
    original_iter = m.iter_security_collection_days
    original_load = security._load_capture
    refs = []

    def load(*args: object) -> object:
        result = original_load(*args)
        refs.append(weakref.ref(result))
        return result

    def tracked(root: Path, **kwargs: object) -> Iterator[object]:
        gc.collect()
        assert all(ref() is None for ref in refs)
        iterator = original_iter(root, **kwargs)
        try:
            for result in iterator:
                refs.extend((weakref.ref(result), weakref.ref(result.batch)))
                yield result
                del result
        finally:
            iterator.close()

    monkeypatch.setattr(security, "_load_capture", load)
    monkeypatch.setattr(m, "iter_security_collection_days", tracked)
    m.assemble_history_archive(_request(tmp_path, roots, _DAYS[:4]))
    gc.collect()
    assert refs and all(ref() is None for ref in refs)


def test_optional_names_and_cli_reuse_the_existing_full_history_source(tmp_path: Path) -> None:
    _module()
    from rquant.factor.name_collect import import_name_probes
    from tests.unit.test_factor_name_collect import _probe, _securities

    roots = []
    for index, day in enumerate((date(2026, 9, 29), date(2026, 9, 30))):
        root = tmp_path / f"source-{index}"
        fd = security._new_root(root)
        daily, refs = _securities(day)
        try:
            captures = (*refs, daily)
            manifest = security.SecurityCollectionManifest(
                trading_days=(day,),
                responses=tuple(security._save_capture(root, fd, item) for item in captures),
            )
            security._publish_collection(root, fd, manifest)
        finally:
            os.close(fd)
        roots.append(root)
    names = tmp_path / "names"
    import_name_probes((_probe(tmp_path / "probe"),), root=names)
    request = _request(
        tmp_path, tuple(roots), (date(2026, 9, 29), date(2026, 9, 30)), name_root=names
    )
    command = [
        sys.executable,
        "-m",
        "rquant.factor.history_assemble",
        "archive",
        "--selection",
        "all",
        "--as-of",
        request.as_of.isoformat(),
        "--input-root",
        str(request.input_root),
        "--archive-root",
        str(request.root),
        "--name-root",
        str(names),
    ]
    for root in roots:
        command.extend(("--capture-root", str(root)))
    for day in request.trading_days:
        command.extend(("--date", day.isoformat()))
    completed = subprocess.run(command, check=False, capture_output=True, text=True)
    assert completed.returncode == 0, completed.stderr
    assert json.loads(completed.stdout)["archive_root"] == str(request.root)
    last = FactorMemberDayInput.model_validate_json(
        (request.input_root / "security-20260930.json").read_bytes()
    )
    assert next(f for f in last.securities.facts if f.stock_code == "301139.SZ").is_st is False


def test_plain_assembly_import_has_no_settings_or_client_initialization() -> None:
    _module()
    code = """
import sys
import rquant.factor.history_assemble
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
