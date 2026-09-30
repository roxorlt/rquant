"""Actual normalized daily files become bounded, reproducible member inputs."""

from __future__ import annotations

import os
from collections.abc import Iterator
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from rquant.factor.universe import (
    DailySecurityBatch,
    DailySecurityFact,
    FactorUniverseError,
    select_factor_universe,
)
from rquant.runtime_contracts import canonical_sha256
from rquant.strict_json import canonical_json_bytes

if TYPE_CHECKING:
    from rquant.factor.member_archive import FactorMemberArchiveManifest

_FIRST = date(2026, 9, 1)
_OBSERVED = datetime(2026, 9, 30, 12, tzinfo=UTC)
_CODES = ("000001.SZ", "000002.SZ", "000003.SZ")


def _private(path: Path) -> Path:
    path.mkdir(mode=0o700)
    path.chmod(0o700)
    return path


def _payload(day: date, *, unknown_st: bool = False) -> dict[str, object]:
    securities = DailySecurityBatch(
        trade_date=day,
        source_id="actual-normalized-security-label",
        source_sha256="a" * 64,
        source_mode="historical_retrospective",
        security_scope="china_a_share",
        observed_at=_OBSERVED,
        complete_stock_codes=_CODES,
        facts=tuple(
            DailySecurityFact(
                stock_code=code,
                exchange="SZ",
                board="gem" if i == 1 else "main",
                is_listed=True,
                is_st=None if unknown_st and i == 0 else False,
            )
            for i, code in enumerate(_CODES)
        ),
    )
    return {
        "schema_version": 1,
        "trade_date": day.isoformat(),
        "securities": securities.model_dump(mode="json"),
        "membership": None,
    }


def _write(root: Path, name: str, payload: object) -> None:
    (root / name).write_bytes(canonical_json_bytes(payload))
    (root / name).chmod(0o600)


def _request(days: tuple[date, ...], **changes: object) -> object:
    from rquant.factor.member_archive import FactorMemberArchiveRequest

    fields = {
        "selection": "all",
        "trading_days": days,
        "as_of": _OBSERVED + timedelta(hours=1),
        "computation_stock_codes": _CODES,
    }
    fields.update(changes)
    return FactorMemberArchiveRequest(**fields)


def test_publish_reads_files_and_derives_order_independent_component_identity(
    tmp_path: Path,
) -> None:
    from rquant.factor.member_archive import (
        load_factor_member_archive,
        publish_factor_member_archive,
    )

    inputs, root = _private(tmp_path / "input"), _private(tmp_path / "archive")
    days = (_FIRST, _FIRST + timedelta(days=1))
    for i, day in enumerate(days):
        _write(inputs, f"day{i}.json", _payload(day))
    reference = publish_factor_member_archive(
        _request(days),
        input_root=inputs,
        daily_filenames=iter(("day0.json", "day1.json")),
        root=root,
    )
    manifest = load_factor_member_archive(root, reference)
    assert manifest.request == _request(days)
    assert manifest.sources.security_source_sha256 != "a" * 64
    assert manifest.sources.index_source_sha256 is None
    assert manifest.days[0].security_payload_sha256 == canonical_sha256(
        _payload(days[0])["securities"]
    )
    assert manifest.days[0].security_count == 3
    for i, day in enumerate(days):
        payload = _payload(day)
        payload["securities"]["facts"].reverse()
        payload["securities"]["complete_stock_codes"].reverse()
        _write(inputs, f"day{i}.json", payload)
    assert (
        publish_factor_member_archive(
            _request(days), input_root=inputs, daily_filenames=("day0.json", "day1.json"), root=root
        )
        == reference
    )
    assert not list(root.glob("*.tmp"))


def test_unknown_st_is_not_replaced_with_a_known_fact(tmp_path: Path) -> None:
    from rquant.factor.member_archive import publish_factor_member_archive

    inputs, root = _private(tmp_path / "input"), _private(tmp_path / "archive")
    _write(inputs, "day.json", _payload(_FIRST, unknown_st=True))
    with pytest.raises(FactorUniverseError, match="st_status_unknown"):
        publish_factor_member_archive(
            _request((_FIRST,)), input_root=inputs, daily_filenames=("day.json",), root=root
        )
    assert not list(root.iterdir())


def test_missing_actual_file_cannot_publish_a_manifest(tmp_path: Path) -> None:
    from rquant.factor.member_archive import publish_factor_member_archive

    inputs, root = _private(tmp_path / "input"), _private(tmp_path / "archive")
    with pytest.raises(FileNotFoundError):
        publish_factor_member_archive(
            _request((_FIRST,)), input_root=inputs, daily_filenames=("absent.json",), root=root
        )
    assert not list(root.iterdir())


@pytest.mark.parametrize("fault", ["noncanonical", "oversize", "symlink"])
def test_incompatible_actual_input_files_are_refused_before_publication(
    tmp_path: Path, fault: str
) -> None:
    from rquant.factor.member_archive import (
        MAX_FACTOR_MEMBER_DAY_BYTES,
        publish_factor_member_archive,
    )

    inputs, root = _private(tmp_path / "input"), _private(tmp_path / "archive")
    _write(inputs, "day.json", _payload(_FIRST))
    if fault == "noncanonical":
        (inputs / "day.json").write_bytes((inputs / "day.json").read_bytes() + b"\n")
    elif fault == "oversize":
        with (inputs / "day.json").open("r+b") as descriptor:
            descriptor.truncate(MAX_FACTOR_MEMBER_DAY_BYTES + 1)
    else:
        (inputs / "day.json").rename(inputs / "original.json")
        (inputs / "day.json").symlink_to(inputs / "original.json")
    with pytest.raises((ValueError, OSError)):
        publish_factor_member_archive(
            _request((_FIRST,)), input_root=inputs, daily_filenames=("day.json",), root=root
        )
    assert not list(root.iterdir())


@pytest.mark.parametrize("fault", ["missing", "extra", "wrong_date"])
def test_import_requires_exact_natural_schedule(tmp_path: Path, fault: str) -> None:
    from rquant.factor.member_archive import publish_factor_member_archive

    inputs, root = _private(tmp_path / "input"), _private(tmp_path / "archive")
    _write(
        inputs,
        "day.json",
        _payload(_FIRST + timedelta(days=1) if fault == "wrong_date" else _FIRST),
    )
    names = (
        ()
        if fault == "missing"
        else (("day.json", "day.json") if fault == "extra" else ("day.json",))
    )
    with pytest.raises(ValueError, match="member input"):
        publish_factor_member_archive(
            _request((_FIRST,)), input_root=inputs, daily_filenames=names, root=root
        )
    assert not list(root.glob("factor-member-archive-*"))
    assert not list(root.glob("*.tmp"))


def test_import_rechecks_already_read_inputs_and_closes_cancelled_iterator(tmp_path: Path) -> None:
    from rquant.factor.member_archive import publish_factor_member_archive

    inputs, root = _private(tmp_path / "input"), _private(tmp_path / "archive")
    days = (_FIRST, _FIRST + timedelta(days=1))
    for i, day in enumerate(days):
        _write(inputs, f"day{i}.json", _payload(day))
    closed = []

    def names() -> Iterator[str]:
        try:
            yield "day0.json"
            _write(inputs, "replacement.json", _payload(_FIRST))
            os.replace(inputs / "replacement.json", inputs / "day0.json")
            yield "day1.json"
        finally:
            closed.append(True)

    with pytest.raises(ValueError, match="changed after read"):
        publish_factor_member_archive(
            _request(days), input_root=inputs, daily_filenames=names(), root=root
        )
    assert closed == [True]
    assert not list(root.glob("factor-member-archive-*"))
    assert not list(root.glob("*.tmp"))


def test_only_corresponding_actual_components_change_the_common_source_identity(
    tmp_path: Path,
) -> None:
    from rquant.factor.member_archive import (
        load_factor_member_archive,
        publish_factor_member_archive,
    )
    from rquant.factor.universe import DailyIndexConstituentBatch

    inputs, root = _private(tmp_path / "input"), _private(tmp_path / "archive")
    payload = _payload(_FIRST)
    payload["membership"] = DailyIndexConstituentBatch(
        selection="hs300",
        trade_date=_FIRST,
        source_id="observed-index-label",
        source_sha256="b" * 64,
        source_mode="historical_retrospective",
        source_kind="daily_complete_membership",
        observed_at=_OBSERVED,
        stock_codes=_CODES[:2],
    ).model_dump(mode="json")

    def publish() -> FactorMemberArchiveManifest:
        _write(inputs, "day.json", payload)
        return load_factor_member_archive(
            root,
            publish_factor_member_archive(
                _request((_FIRST,), selection="hs300"),
                input_root=inputs,
                daily_filenames=("day.json",),
                root=root,
            ),
        )

    first = publish()
    payload["membership"]["stock_codes"] = [_CODES[2]]
    second = publish()
    assert first.sources.security_source_sha256 == second.sources.security_source_sha256
    assert first.sources.index_source_sha256 != second.sources.index_source_sha256
    payload["securities"]["source_id"] = "different-observed-security-label"
    payload["securities"]["observed_at"] = (
        (_OBSERVED + timedelta(minutes=1)).isoformat().replace("+00:00", "Z")
    )
    third = publish()
    assert second.sources.security_source_sha256 != third.sources.security_source_sha256
    assert second.sources.index_source_sha256 == third.sources.index_source_sha256


@pytest.mark.parametrize(
    "selection,reason", [("hs300", "index_source_missing"), ("gem", "missing_board_fact")]
)
def test_required_missing_facts_are_not_manufactured(
    tmp_path: Path, selection: str, reason: str
) -> None:
    from rquant.factor.member_archive import publish_factor_member_archive

    inputs, root = _private(tmp_path / "input"), _private(tmp_path / "archive")
    payload = _payload(_FIRST)
    payload["securities"]["facts"][0]["board"] = None
    _write(inputs, "day.json", payload)
    with pytest.raises(FactorUniverseError, match=reason):
        publish_factor_member_archive(
            _request((_FIRST,), selection=selection),
            input_root=inputs,
            daily_filenames=("day.json",),
            root=root,
        )
    assert not list(root.iterdir())


def test_daily_and_manifest_limits_are_finite_and_do_not_truncate(tmp_path: Path) -> None:
    from pydantic import ValidationError

    from rquant.factor.member_archive import (
        MAX_FACTOR_MEMBER_MANIFEST_BYTES,
        load_factor_member_archive,
        publish_factor_member_archive,
    )
    from rquant.factor.member_stream import open_factor_member_stream

    inputs, root = _private(tmp_path / "input"), _private(tmp_path / "archive")
    codes = tuple(f"{i:06d}.SZ" for i in range(1, 7001))
    days = tuple(_FIRST + timedelta(days=i) for i in range(1024))
    request = _request(days, computation_stock_codes=codes)
    assert len(request.trading_days) == 1024 and len(request.computation_stock_codes) == 7000
    with pytest.raises(ValidationError):
        _request(days + (days[-1] + timedelta(days=1),))
    with pytest.raises(ValidationError):
        _request((_FIRST,), computation_stock_codes=codes + ("007001.SZ",))
    payload = _payload(_FIRST)
    payload["securities"]["complete_stock_codes"] = list(codes)
    payload["securities"]["facts"] = [
        {"stock_code": code, "exchange": "SZ", "board": None, "is_listed": True, "is_st": False}
        for code in codes
    ]
    _write(inputs, "day.json", payload)
    reference = publish_factor_member_archive(
        _request((_FIRST,), computation_stock_codes=codes),
        input_root=inputs,
        daily_filenames=("day.json",),
        root=root,
    )
    assert load_factor_member_archive(root, reference).days[0].security_count == 7000
    with open_factor_member_stream(root, reference) as stream:
        row = next(stream)
        assert len(row.securities.facts) == len(select_factor_universe(row).stock_codes) == 7000
    payload["securities"]["complete_stock_codes"].append("007001.SZ")
    payload["securities"]["facts"].append(
        {
            "stock_code": "007001.SZ",
            "exchange": "SZ",
            "board": None,
            "is_listed": True,
            "is_st": False,
        }
    )
    _write(inputs, "day.json", payload)
    with pytest.raises(ValidationError, match="7000"):
        publish_factor_member_archive(
            _request((_FIRST,), computation_stock_codes=codes),
            input_root=inputs,
            daily_filenames=("day.json",),
            root=root,
        )
    with (root / reference.filename).open("r+b") as descriptor:
        descriptor.truncate(MAX_FACTOR_MEMBER_MANIFEST_BYTES + 1)
    with pytest.raises(ValueError, match="byte limit"):
        load_factor_member_archive(root, reference)


def test_cancelled_publication_closes_own_file_and_removes_only_its_temporary(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from rquant.factor import member_archive

    inputs, root = _private(tmp_path / "input"), _private(tmp_path / "archive")
    _write(inputs, "day.json", _payload(_FIRST))
    _write(root, "unrelated.json", {"keep": True})
    descriptors = []

    def cancelled(descriptor: int, data: bytes) -> None:
        descriptors.append(descriptor)
        os.write(descriptor, data[:10])
        raise KeyboardInterrupt("synthetic file publication cancellation")

    monkeypatch.setattr(member_archive, "_write_all", cancelled)
    with pytest.raises(KeyboardInterrupt):
        member_archive.publish_factor_member_archive(
            _request((_FIRST,)), input_root=inputs, daily_filenames=("day.json",), root=root
        )
    assert [path.name for path in root.iterdir()] == ["unrelated.json"]
    assert descriptors
    for descriptor in descriptors:
        with pytest.raises(OSError):
            os.fstat(descriptor)
    print(
        "MEMBER_PUBLISH_RESOURCES: cancelled=true own_temp_removed=true "
        "unrelated_kept=true file_fd_closed=true"
    )


@pytest.mark.parametrize("binding", ["scope", "cutoff"])
def test_actual_day_must_fit_computation_scope_and_real_observation_cutoff(
    tmp_path: Path, binding: str
) -> None:
    from rquant.factor.member_archive import publish_factor_member_archive

    inputs, root = _private(tmp_path / "input"), _private(tmp_path / "archive")
    _write(inputs, "day.json", _payload(_FIRST))
    request = _request(
        (_FIRST,),
        **(
            {"computation_stock_codes": _CODES[:1]}
            if binding == "scope"
            else {"as_of": _OBSERVED - timedelta(minutes=1)}
        ),
    )
    with pytest.raises(ValueError):
        publish_factor_member_archive(
            request, input_root=inputs, daily_filenames=("day.json",), root=root
        )
    assert not list(root.iterdir())
