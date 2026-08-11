from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path

import pytest

from rquant.runtime_contracts import canonical_sha256
from rquant.runtime_market_calendar_generation import (
    install_market_calendar_generation,
    market_calendar_generation_path,
)
from rquant.runtime_market_session import MarketCalendarAuthority
from rquant.strict_json import canonical_json_bytes

COMMIT = "a" * 40


def _authority(*, open_dates: tuple[date, ...]) -> MarketCalendarAuthority:
    return MarketCalendarAuthority.create(
        schema_version=1,
        exchange="SSE",
        producer_commit=COMMIT,
        coverage_start=date(2026, 1, 1),
        coverage_end=date(2026, 12, 31),
        open_dates=open_dates,
        generated_at=datetime(2025, 12, 31, 8, tzinfo=UTC),
    )


def _write_source(path: Path, authority: MarketCalendarAuthority) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(canonical_json_bytes(authority.model_dump(mode="json")))
    path.chmod(0o600)


def test_calendar_generation_is_content_addressed_private_and_idempotent(
    tmp_path: Path,
) -> None:
    authority = _authority(open_dates=(date(2026, 1, 5), date(2026, 1, 6)))
    source = tmp_path / "external" / "calendar.json"
    runtime_root = tmp_path / "runtime"
    _write_source(source, authority)

    first = install_market_calendar_generation(
        source,
        runtime_root=runtime_root,
        expected_commit=COMMIT,
        expected_content_sha256=authority.content_sha256,
    )
    second = install_market_calendar_generation(
        source,
        runtime_root=runtime_root,
        expected_commit=COMMIT,
        expected_content_sha256=authority.content_sha256,
    )

    assert (
        first
        == second
        == market_calendar_generation_path(
            runtime_root,
            authority.content_sha256,
        )
    )
    assert first.read_bytes() == canonical_json_bytes(authority.model_dump(mode="json"))
    assert first.stat().st_mode & 0o777 == 0o600
    assert first.stat().st_nlink == 1


def test_calendar_generations_coexist_and_never_overwrite_history(tmp_path: Path) -> None:
    first = _authority(open_dates=(date(2026, 1, 5),))
    second = _authority(open_dates=(date(2026, 1, 5), date(2026, 1, 6)))
    runtime_root = tmp_path / "runtime"
    source = tmp_path / "calendar.json"
    _write_source(source, first)
    first_path = install_market_calendar_generation(
        source,
        runtime_root=runtime_root,
        expected_commit=COMMIT,
        expected_content_sha256=first.content_sha256,
    )
    _write_source(source, second)
    second_path = install_market_calendar_generation(
        source,
        runtime_root=runtime_root,
        expected_commit=COMMIT,
        expected_content_sha256=second.content_sha256,
    )

    assert first_path != second_path
    assert first_path.read_bytes() == canonical_json_bytes(first.model_dump(mode="json"))
    assert second_path.read_bytes() == canonical_json_bytes(second.model_dump(mode="json"))


def test_calendar_generation_rejects_wrong_identity_and_existing_tamper(
    tmp_path: Path,
) -> None:
    authority = _authority(open_dates=(date(2026, 1, 5),))
    source = tmp_path / "calendar.json"
    runtime_root = tmp_path / "runtime"
    _write_source(source, authority)

    with pytest.raises(ValueError, match="content"):
        install_market_calendar_generation(
            source,
            runtime_root=runtime_root,
            expected_commit=COMMIT,
            expected_content_sha256="f" * 64,
        )

    target = market_calendar_generation_path(runtime_root, authority.content_sha256)
    target.parent.mkdir(parents=True)
    target.write_text("tampered", encoding="utf-8")
    target.chmod(0o600)
    with pytest.raises(ValueError, match="immutable|content"):
        install_market_calendar_generation(
            source,
            runtime_root=runtime_root,
            expected_commit=COMMIT,
            expected_content_sha256=authority.content_sha256,
        )


def test_calendar_generation_rejects_symlinked_runtime_ancestor(tmp_path: Path) -> None:
    authority = _authority(open_dates=(date(2026, 1, 5),))
    source = tmp_path / "calendar.json"
    outside = tmp_path / "outside"
    outside.mkdir()
    runtime_root = tmp_path / "runtime"
    runtime_root.symlink_to(outside, target_is_directory=True)
    _write_source(source, authority)

    with pytest.raises(ValueError, match="symlink|unsafe"):
        install_market_calendar_generation(
            source,
            runtime_root=runtime_root,
            expected_commit=COMMIT,
            expected_content_sha256=authority.content_sha256,
        )
    assert not tuple(outside.rglob("*.json"))


def test_calendar_generation_recovers_only_its_deterministic_interrupted_stage(
    tmp_path: Path,
) -> None:
    authority = _authority(open_dates=(date(2026, 1, 5),))
    source = tmp_path / "calendar.json"
    runtime_root = tmp_path / "runtime"
    _write_source(source, authority)
    target = install_market_calendar_generation(
        source,
        runtime_root=runtime_root,
        expected_commit=COMMIT,
        expected_content_sha256=authority.content_sha256,
    )
    stage = target.parent / f".{authority.content_sha256}.stage"
    stage.hardlink_to(target)
    assert target.stat().st_nlink == 2

    recovered = install_market_calendar_generation(
        source,
        runtime_root=runtime_root,
        expected_commit=COMMIT,
        expected_content_sha256=authority.content_sha256,
    )

    assert recovered == target
    assert target.stat().st_nlink == 1
    assert not stage.exists()


def test_calendar_generation_rewrites_its_partial_stage_before_publication(
    tmp_path: Path,
) -> None:
    baseline = _authority(open_dates=(date(2026, 1, 5),))
    authority = _authority(open_dates=(date(2026, 1, 5), date(2026, 1, 6)))
    source = tmp_path / "calendar.json"
    runtime_root = tmp_path / "runtime"
    _write_source(source, baseline)
    baseline_path = install_market_calendar_generation(
        source,
        runtime_root=runtime_root,
        expected_commit=COMMIT,
        expected_content_sha256=baseline.content_sha256,
    )
    _write_source(source, authority)
    stage = baseline_path.parent / f".{authority.content_sha256}.stage"
    stage.write_bytes(b"partial")
    stage.chmod(0o600)

    target = install_market_calendar_generation(
        source,
        runtime_root=runtime_root,
        expected_commit=COMMIT,
        expected_content_sha256=authority.content_sha256,
    )

    assert target.read_bytes() == canonical_json_bytes(authority.model_dump(mode="json"))
    assert not stage.exists()


def test_calendar_generation_path_rejects_noncanonical_identity(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="sha256"):
        market_calendar_generation_path(tmp_path / "runtime", canonical_sha256("x")[:-1])
