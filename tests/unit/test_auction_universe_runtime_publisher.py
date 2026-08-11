from __future__ import annotations

import json
from datetime import UTC, date, datetime
from pathlib import Path

import duckdb

from rquant.auction_universe_authority import load_auction_universe_authority
from rquant.runtime_market_session import MarketCalendarAuthority
from rquant.runtime_service_builtin import auction_universe_publisher_builder
from rquant.runtime_service_control import RuntimeServicePlane
from rquant.runtime_service_entrypoint import RuntimeServiceKind, RuntimeServiceManifest

COMMIT = "a" * 40


def _calendar(path: Path) -> MarketCalendarAuthority:
    authority = MarketCalendarAuthority.create(
        schema_version=1,
        exchange="SSE",
        producer_commit=COMMIT,
        coverage_start=date(2026, 7, 30),
        coverage_end=date(2026, 8, 4),
        open_dates=(date(2026, 7, 30), date(2026, 7, 31), date(2026, 8, 3)),
        generated_at=datetime(2026, 7, 29, tzinfo=UTC),
    )
    path.write_text(
        json.dumps(
            authority.model_dump(mode="json"),
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        )
    )
    path.chmod(0o600)
    return authority


def _database(path: Path) -> Path:
    with duckdb.connect(str(path)) as connection:
        connection.execute(
            """
            CREATE TABLE daily_bar(ts_code VARCHAR NOT NULL, trade_date DATE NOT NULL);
            INSERT INTO daily_bar VALUES
                ('000001.SZ', DATE '2026-07-31'),
                ('600000.SH', DATE '2026-07-31'),
                ('688001.SH', DATE '2026-07-31');
            """
        )
    path.chmod(0o600)
    return path


def _manifest(tmp_path: Path) -> RuntimeServiceManifest:
    calendar_path = tmp_path / "calendar.json"
    calendar = _calendar(calendar_path)
    return RuntimeServiceManifest(
        service_id="publisher.auction-universe",
        service_kind=RuntimeServiceKind.AUCTION_UNIVERSE_PUBLISHER,
        plane=RuntimeServicePlane.LIVE,
        interval_seconds=60,
        stale_after_seconds=900,
        producer_commit=COMMIT,
        settings={
            "database_path": str(_database(tmp_path / "operational-ro.duckdb")),
            "calendar_path": str(calendar_path),
            "calendar_expected_commit": calendar.producer_commit,
            "calendar_content_sha256": calendar.content_sha256,
            "authority_root": str(tmp_path / "auction-universe"),
        },
    )


def test_runtime_publisher_seals_once_and_reports_all_source_generations(
    tmp_path: Path,
) -> None:
    observed_at = datetime(2026, 7, 31, 10, 30, tzinfo=UTC)
    manifest = _manifest(tmp_path)
    step = auction_universe_publisher_builder(clock=lambda: observed_at)(manifest)

    first = step()
    second = step()

    authority = load_auction_universe_authority(
        tmp_path / "auction-universe" / "current.json",
        expected_commit=COMMIT,
        required_trade_date=date(2026, 8, 3),
        as_of=observed_at,
    )
    assert first.processed_count == 3
    assert second.processed_count == 0
    assert first.source_generations == {
        "market_calendar": manifest.producer_commit
        and MarketCalendarAuthority.model_validate_json(
            (tmp_path / "calendar.json").read_bytes()
        ).content_sha256,
        "daily_bar": authority.source_snapshot_id,
        "auction_universe": authority.content_sha256,
    }


def test_runtime_publisher_is_quiet_inside_protection_window(tmp_path: Path) -> None:
    manifest = _manifest(tmp_path)
    step = auction_universe_publisher_builder(
        clock=lambda: datetime(2026, 7, 31, 2, 0, tzinfo=UTC),
    )(manifest)

    result = step()

    assert result.processed_count == 0
    assert result.source_generations["market_calendar"]
    assert not (tmp_path / "auction-universe" / "current.json").exists()


def test_runtime_publisher_rejects_wrong_kind_or_plane(tmp_path: Path) -> None:
    manifest = _manifest(tmp_path)
    builder = auction_universe_publisher_builder(
        clock=lambda: datetime(2026, 7, 31, 10, 30, tzinfo=UTC),
    )

    try:
        builder(manifest.model_copy(update={"service_kind": RuntimeServiceKind.NOTIFIER}))
    except ValueError as exc:
        assert "auction_universe_publisher" in str(exc)
    else:
        raise AssertionError("wrong service kind was accepted")

    try:
        builder(manifest.model_copy(update={"plane": RuntimeServicePlane.RESEARCH}))
    except ValueError as exc:
        assert "live plane" in str(exc)
    else:
        raise AssertionError("wrong service plane was accepted")
