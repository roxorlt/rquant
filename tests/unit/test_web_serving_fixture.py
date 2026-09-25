"""The synthetic web fixture publishes real, verifiable Serving generations."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from rquant.serving_contracts import FreshnessStatus
from rquant.serving_publisher import ServingReader
from scripts import build_web_fixture as fixture_cli
from tests.support.web_serving_fixture import (
    FIXTURE_BUILT_AT,
    FIXTURE_PRODUCER_COMMIT,
    SCENARIOS,
    build_web_fixture,
)

_PANORAMA_TABLES = (
    "market_snapshot",
    "market_overview",
    "dc_board",
    "dc_board_member",
    "kpl_concept_member",
    "market_liquidity",
    "intraday_kline",
    "daily_bar",
    "surge_event",
    "pulse_history",
    "pulse_alert",
    "surge_runtime_config",
)


@pytest.mark.parametrize("scenario", SCENARIOS)
def test_every_scenario_publishes_a_generation_the_reader_verifies(
    tmp_path: Path, scenario: str
) -> None:
    root = tmp_path / scenario
    manifest = build_web_fixture(root, scenario)

    with ServingReader(root).acquire_generation() as lease:
        assert lease.manifest.generation_id == manifest.generation_id
        for table in ("runtime_services", "signals", "deliveries", "paper_accounts"):
            (count,) = lease.connection.execute(f"SELECT count(*) FROM {table}").fetchone()
            assert count > 0, table
    assert manifest.built_at == FIXTURE_BUILT_AT
    assert manifest.producer_commit == FIXTURE_PRODUCER_COMMIT


def test_panorama_publishes_every_panorama_projection(tmp_path: Path) -> None:
    root = tmp_path / "panorama"
    manifest = build_web_fixture(root, "panorama")

    assert all(manifest.row_counts[table] > 0 for table in _PANORAMA_TABLES)
    assert manifest.row_counts["intraday_kline"] == 5 * 240
    assert manifest.row_counts["daily_bar"] == 5 * 120
    with ServingReader(root).acquire_generation() as lease:
        unavailable = lease.connection.execute(
            "SELECT table_name FROM projection_status WHERE table_name IN "
            f"({','.join('?' for _ in _PANORAMA_TABLES)}) AND NOT available",
            _PANORAMA_TABLES,
        ).fetchall()
        systems = lease.connection.execute(
            "SELECT DISTINCT system FROM market_overview ORDER BY system"
        ).fetchall()
    assert unavailable == []
    assert [row[0] for row in systems] == ["东财概念", "东财行业", "开盘啦题材"]


def test_degraded_marks_watermarks_and_leaves_projections_unpublished(tmp_path: Path) -> None:
    root = tmp_path / "degraded"
    manifest = build_web_fixture(root, "degraded")

    statuses = {item.dataset_id: item.status for item in manifest.watermarks}
    assert statuses["runtime_health"] is FreshnessStatus.DEGRADED
    assert statuses["lab_jobs"] is FreshnessStatus.UNAVAILABLE
    with ServingReader(root).acquire_generation() as lease:
        rows = dict(
            lease.connection.execute(
                "SELECT table_name, available FROM projection_status "
                "WHERE table_name IN ('dashboard_summary', 'minute_coverage', 'trade_calendar')"
            ).fetchall()
        )
    assert rows == {"dashboard_summary": False, "minute_coverage": False, "trade_calendar": True}


def test_the_next_sequence_is_a_new_later_generation_on_the_same_root(tmp_path: Path) -> None:
    root = tmp_path / "serving"
    first = build_web_fixture(root, "baseline")
    second = build_web_fixture(root, "baseline", sequence=1)

    assert second.generation_id != first.generation_id
    assert second.built_at > first.built_at
    pointer = ServingReader(root).current_pointer()
    assert pointer.generation_id == second.generation_id
    assert pointer.previous_generation_id == first.generation_id


def test_the_command_line_wrapper_refuses_to_overwrite_foreign_directories(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    foreign = tmp_path / "foreign"
    foreign.mkdir()
    (foreign / "keep.txt").write_text("x", encoding="utf-8")
    with pytest.raises(SystemExit):
        fixture_cli.main(["--out", str(foreign), "--scenario", "baseline"])
    assert (foreign / "keep.txt").exists()

    root = tmp_path / "serving"
    assert fixture_cli.main(["--out", str(root), "--scenario", "baseline"]) == 0
    first = json.loads(capsys.readouterr().out)
    assert fixture_cli.main(["--out", str(root), "--scenario", "baseline", "--publish-next"]) == 0
    second = json.loads(capsys.readouterr().out)
    assert (first["sequence"], second["sequence"]) == (0, 1)
    assert fixture_cli.main(["--out", str(root), "--scenario", "baseline", "--replace"]) == 0
    assert json.loads(capsys.readouterr().out)["sequence"] == 0


def test_the_trade_calendar_is_the_2026_sse_schedule_not_a_weekday_rule(tmp_path: Path) -> None:
    root = tmp_path / "baseline"
    build_web_fixture(root, "baseline")
    with ServingReader(root).acquire_generation() as lease:
        rows = lease.connection.execute(
            "SELECT trade_date FROM trade_calendar WHERE exchange = 'SSE' AND is_open "
            "ORDER BY trade_date"
        ).fetchall()
    open_days = {row[0] for row in rows}

    assert len(open_days) == 242
    assert min(open_days).isoformat() == "2026-01-05"
    assert max(open_days).isoformat() == "2026-12-31"
    # 中秋 (Friday 09-25) and the National Day week are closed; the Monday after is open.
    closed = ("2026-09-25", "2026-10-01", "2026-10-02", "2026-10-05", "2026-10-06", "2026-10-07")
    assert not {day for day in open_days if day.isoformat() in closed}
    assert {day.isoformat() for day in open_days} >= {"2026-09-24", "2026-09-28", "2026-10-08"}
