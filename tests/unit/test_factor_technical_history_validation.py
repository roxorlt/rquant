"""Exact verified inputs reuse logic checks without trusting mutable file metadata."""

from __future__ import annotations

import os
import shutil
from collections.abc import Iterator
from datetime import timedelta
from pathlib import Path

import pytest

import rquant.factor.technical_history_source as technical
import rquant.research_snapshot as snapshot
from rquant.factor.daily_feature_source import (
    FactorDailyFeatureQuery,
    FactorDailyFeatureSource,
    open_factor_daily_feature_source,
)
from tests.unit.test_factor_technical_history_source import _AS_OF, _FIRST, _prepared, _raw


@pytest.fixture(autouse=True)
def _empty_validation_cache() -> Iterator[None]:
    cache = getattr(technical, "_TECHNICAL_INPUT_VALIDATIONS", None)
    if cache is not None:
        cache.clear()
    yield
    if cache is not None:
        cache.clear()


def _source(tmp_path: Path) -> FactorDailyFeatureSource:
    path = _raw(tmp_path, days=12, count=3)
    prepared = _prepared(tmp_path, path, end=10, count=3)
    return technical.prepare_factor_technical_history_source(
        technical.FactorTechnicalHistoryPrepareRequest(prepared_source=prepared),
        lake_root=tmp_path / "lake",
        now=lambda: _AS_OF + timedelta(minutes=5),
    )


def test_repeated_technical_reader_reuses_one_complete_logical_verification(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = _source(tmp_path)
    scans = []
    logical = snapshot._logical_content_hash

    def observed(
        path: Path, *, columns: tuple[tuple[str, str], ...], primary_key: tuple[str, ...]
    ) -> str:
        if path.parent.parent.name == "technical_history_input":
            scans.append(path)
        return logical(path, columns=columns, primary_key=primary_key)

    monkeypatch.setattr(snapshot, "_logical_content_hash", observed)
    query = FactorDailyFeatureQuery(
        source_sha256=source.sha256,
        trade_date=_FIRST + timedelta(days=10),
        stock_codes=source.scope.stock_codes,
        fields=("ma5",),
    )
    batches = []
    for _ in range(2):
        with open_factor_daily_feature_source(source, lake_root=tmp_path / "lake") as lease:
            batches.append(lease.query(query))
        assert lease._closed and not lease._private_root.exists()
    assert batches[0] == batches[1]
    assert [fact.status for fact in batches[0].facts] == ["valid", "null", "valid"]
    assert len(scans) == 1, "original, copied input and natural tails repeat the complete scan"


def test_cached_input_still_rejects_wrong_metadata_and_earlier_asof(tmp_path: Path) -> None:
    source = _source(tmp_path)
    (artifact,) = source.technical_history.inputs
    verify = technical._verify_technical_history_input
    verify(artifact, lake_root=tmp_path / "lake", as_of_time=source.scope.as_of_time)
    for field, value in (
        ("row_count", artifact.row_count + 1),
        ("schema_hash", "0" * 64),
        ("content_hash", "0" * 64),
    ):
        with pytest.raises(ValueError, match="mismatch"):
            verify(
                artifact.model_copy(update={field: value}),
                lake_root=tmp_path / "lake",
                as_of_time=source.scope.as_of_time,
            )
    with pytest.raises(ValueError, match="future"):
        verify(artifact, lake_root=tmp_path / "lake", as_of_time=_AS_OF.replace(month=7, day=1))


@pytest.mark.parametrize("location", ["original", "copy"])
def test_cached_input_mutation_at_natural_tail_refuses_and_closes(
    tmp_path: Path, location: str
) -> None:
    source = _source(tmp_path)
    (artifact,) = source.technical_history.inputs
    lake = tmp_path / "lake"
    with open_factor_daily_feature_source(source, lake_root=lake):
        pass
    with (
        pytest.raises(ValueError, match="hash|changed"),
        open_factor_daily_feature_source(source, lake_root=lake) as lease,
    ):
        root = lake if location == "original" else lease._private_root
        path = root / artifact.relative_path
        before = path.stat()
        raw = path.read_bytes()
        path.write_bytes(raw[:-1] + bytes((raw[-1] ^ 1,)))
        os.utime(path, ns=(before.st_atime_ns, before.st_mtime_ns))
        assert path.stat().st_size == before.st_size
    assert lease._closed and not lease._private_root.exists()
    assert not list(lake.glob(".daily-feature-reader-*"))
    if location == "original":
        with (
            pytest.raises(ValueError, match="hash"),
            open_factor_daily_feature_source(source, lake_root=lake),
        ):
            pytest.fail("a warm cache cannot trust unchanged size or restored mtime")


def test_cached_same_byte_input_replacement_refuses_active_reader(tmp_path: Path) -> None:
    source = _source(tmp_path)
    (artifact,) = source.technical_history.inputs
    lake = tmp_path / "lake"
    with (
        pytest.raises(ValueError, match="changed"),
        open_factor_daily_feature_source(source, lake_root=lake) as lease,
    ):
        path = lake / artifact.relative_path
        original = path.with_suffix(".original")
        path.rename(original)
        shutil.copyfile(original, path)
        assert path.read_bytes() == original.read_bytes()
    assert lease._closed and not lease._private_root.exists()


def test_input_validation_cache_is_bounded_and_retains_no_resources(tmp_path: Path) -> None:
    source = _source(tmp_path)
    (artifact,) = source.technical_history.inputs
    before = len(os.listdir("/dev/fd"))
    cache = technical._TECHNICAL_INPUT_VALIDATIONS
    assert technical._TECHNICAL_INPUT_VALIDATION_LIMIT == 32
    for index in range(34):
        technical._verify_technical_history_input(
            artifact.model_copy(update={"artifact_key": f"technical-input-{index}"}),
            lake_root=tmp_path / "lake",
            as_of_time=source.scope.as_of_time,
        )
    assert len(cache) == 32
    assert all(isinstance(key[0], str) and len(key[0]) == 64 for key in cache)
    assert all(value is None for value in cache.values())
    assert len(os.listdir("/dev/fd")) == before
