from __future__ import annotations

import ast
import json
import os
import subprocess
import sys
import textwrap
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import duckdb
import pandas as pd
import pytest

from rquant.dashboard.runtime_console_data import (
    ServingFrameState,
    query_serving_frame,
)
from rquant.dashboard.serving_only_page_data import ServingOnlyRenderContext
from rquant.serving_contracts import FreshnessStatus, ServingDatasetWatermark
from rquant.serving_publisher import ServingPublisher, ServingTableSpec

_PROJECT_ROOT = Path(__file__).resolve().parents[2]
_PAGE_PATHS = (
    _PROJECT_ROOT / "src/rquant/dashboard/app.py",
    _PROJECT_ROOT / "src/rquant/dashboard/nl_screen.py",
    _PROJECT_ROOT / "src/rquant/dashboard/nl_canvas.py",
    _PROJECT_ROOT / "src/rquant/dashboard/lab/app.py",
    _PROJECT_ROOT / "src/rquant/dashboard/market_panorama.py",
    _PROJECT_ROOT / "src/rquant/dashboard/strategy_lab_data.py",
    _PROJECT_ROOT / "src/rquant/dashboard/runtime_console_data.py",
    _PROJECT_ROOT / "src/rquant/panorama_data.py",
    _PROJECT_ROOT / "src/rquant/lab_worker.py",
)

_PAGE_READER_DEPENDENCIES = (
    _PROJECT_ROOT / "src/rquant/dashboard/lab/job_center.py",
    _PROJECT_ROOT / "src/rquant/dashboard/serving_page_data.py",
    _PROJECT_ROOT / "src/rquant/dashboard/strategy_lab_data.py",
    _PROJECT_ROOT / "src/rquant/panorama_data.py",
)


def test_production_pages_have_no_operational_duckdb_fallback_import() -> None:
    forbidden_names = {"open_readonly_connection", "open_readonly_store", "DuckDBStore"}

    for path in _PAGE_PATHS:
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        imported_names = {
            alias.name
            for node in ast.walk(tree)
            if isinstance(node, ast.ImportFrom)
            for alias in node.names
        }
        imported_modules = {
            node.module for node in ast.walk(tree) if isinstance(node, ast.ImportFrom)
        }
        direct_imports = {
            alias.name
            for node in ast.walk(tree)
            if isinstance(node, ast.Import)
            for alias in node.names
        }
        assert "rquant.storage.duckdb" not in imported_modules, path
        assert not forbidden_names.intersection(imported_names), path
        if path.name == "strategy_lab_data.py":
            direct_connects = {
                node.func.attr
                for node in ast.walk(tree)
                if isinstance(node, ast.Call)
                and isinstance(node.func, ast.Attribute)
                and isinstance(node.func.value, ast.Name)
                and node.func.value.id == "duckdb"
            }
            assert "connect" not in direct_connects, path
            assert "ImmutableDuckDBMetadataCatalog" in imported_names, path
        else:
            assert "duckdb" not in direct_imports, path
        assert "rquant.duckdb" not in path.read_text(encoding="utf-8"), path


def test_streamlit_pages_submit_control_commands_without_local_persistence() -> None:
    page_paths = (
        _PROJECT_ROOT / "src/rquant/dashboard/nl_screen.py",
        _PROJECT_ROOT / "src/rquant/dashboard/nl_canvas.py",
        _PROJECT_ROOT / "src/rquant/dashboard/lab/app.py",
    )
    forbidden_calls = {"mkdir", "chmod", "write_text", "write_bytes", "unlink"}
    forbidden_imports = {
        "add_pool_to_canvas",
        "delete_canvas",
        "delete_user_pool",
        "fork_builtin_to_user",
        "save_canvas",
        "save_user_pool",
        "set_canvas_pool_refs",
    }

    for path in page_paths:
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        called_attributes = {
            node.func.attr
            for node in ast.walk(tree)
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
        }
        imported_names = {
            alias.name
            for node in ast.walk(tree)
            if isinstance(node, ast.ImportFrom)
            for alias in node.names
        }
        assert not forbidden_calls.intersection(called_attributes), path
        assert not forbidden_imports.intersection(imported_names), path
        assert "PageControlClient" in imported_names, path


def test_page_reader_dependencies_do_not_construct_writable_authorities() -> None:
    forbidden_constructors = {
        "DuckDBStore",
        "ExperimentRegistry",
        "LabJobArtifactStore",
        "LabJobStore",
    }

    for path in _PAGE_READER_DEPENDENCIES:
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        constructors = {
            node.func.id
            for node in ast.walk(tree)
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
        }
        assert not forbidden_constructors.intersection(constructors), path


def test_pages_do_not_run_large_operational_scans_or_replays() -> None:
    app_source = (_PROJECT_ROOT / "src/rquant/dashboard/app.py").read_text(encoding="utf-8")
    nl_source = (_PROJECT_ROOT / "src/rquant/dashboard/nl_screen.py").read_text(encoding="utf-8")
    panorama_source = (_PROJECT_ROOT / "src/rquant/panorama_data.py").read_text(encoding="utf-8")

    assert "FROM minute_bar" not in app_source
    assert "run_entry_mode_comparison" not in app_source
    assert "import akshare" not in app_source
    assert "requests.get(" not in app_source
    assert "subprocess.run(" not in app_source
    assert "read_recent(" not in app_source
    assert ".read_text(" not in app_source
    assert "screen_with_plan_diagnostic" not in nl_source
    assert "FROM daily_basic" not in panorama_source
    assert "acquired.connection.execute(" not in app_source
    assert "store._conn.execute(" not in panorama_source


def test_market_panorama_ui_uses_only_one_serving_generation_without_source_poller() -> None:
    path = _PROJECT_ROOT / "src/rquant/dashboard/market_panorama.py"
    source = path.read_text(encoding="utf-8")
    data_source = (_PROJECT_ROOT / "src/rquant/panorama_data.py").read_text(encoding="utf-8")

    assert "SourcePoller" not in source
    assert "panorama_poller" not in source
    assert "fetch_intraday_trend" not in source
    assert "SourcePoller" not in data_source
    assert "panorama_poller" not in data_source
    assert "rquant.config" not in data_source
    assert "live_dir=" not in source
    assert "open_panorama_serving_generation" in source
    assert source.count("open_panorama_serving_generation()") == 1


def test_panorama_service_clears_fixture_flag_after_environment_file() -> None:
    service = (_PROJECT_ROOT / "deploy/systemd/rquant-panorama.service").read_text(encoding="utf-8")

    environment_file = service.index("EnvironmentFile=/home/lighthouse/rquant/.env")
    fixture_clear = service.index("Environment=RQUANT_PANORAMA_FAKE=")

    assert fixture_clear > environment_file


def test_panorama_import_and_real_serving_open_are_operational_dependency_free(
    tmp_path: Path,
) -> None:
    serving_root = tmp_path / "serving"
    built_at = datetime(2026, 8, 2, 1, 0, tzinfo=UTC)
    ServingPublisher(
        serving_root,
        producer_commit="a" * 40,
        table_specs={"proof": ServingTableSpec(sort_keys=("value",))},
    ).publish(
        {"proof": pd.DataFrame({"value": ["serving-only"]})},
        watermarks=(
            ServingDatasetWatermark(
                dataset_id="proof",
                generation_id="proof-1",
                event_time=built_at,
                published_at=built_at,
                sequence=1,
                status=FreshnessStatus.FRESH,
            ),
        ),
        source_generations={"proof": "proof-1"},
        built_at=built_at,
    )
    script = textwrap.dedent(
        """
        import sys
        from rquant.panorama_data import open_panorama_serving_generation
        import rquant.dashboard.market_panorama

        with open_panorama_serving_generation() as store:
            frame = store.query_frame(
                "SELECT value FROM proof",
                max_rows=1,
                max_result_bytes=1024,
            )
            assert frame["value"].tolist() == ["serving-only"]

        forbidden = (
            "rquant.config",
            "rquant.storage.duckdb",
            "rquant.panorama_poller",
        )
        loaded = sorted(name for name in forbidden if name in sys.modules)
        assert loaded == [], loaded
        """
    )
    environment = {
        "PATH": os.environ.get("PATH", ""),
        "PYTHONPATH": str(_PROJECT_ROOT / "src"),
        "RQUANT_DISABLE_DOTENV": "1",
        "RQUANT_SERVING_ROOT": str(serving_root),
    }

    completed = subprocess.run(
        [sys.executable, "-c", script],
        cwd=_PROJECT_ROOT,
        env=environment,
        capture_output=True,
        text=True,
        timeout=20,
        check=False,
    )

    assert completed.returncode == 0, json.dumps(
        {"stdout": completed.stdout, "stderr": completed.stderr},
        ensure_ascii=False,
    )


def test_market_panorama_helpers_accept_the_render_serving_reader() -> None:
    """The page must pass its one lease through every data-dependent helper."""

    path = _PROJECT_ROOT / "src/rquant/dashboard/market_panorama.py"
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    helper_names = {
        "render_pulse",
        "render_stock_chart",
        "render_historical_surge_detail",
        "render_surge_log",
    }
    helpers = {
        node.name: node
        for node in tree.body
        if isinstance(node, ast.FunctionDef) and node.name in helper_names
    }

    assert helpers.keys() == helper_names
    for name, helper in helpers.items():
        parameter_names = {argument.arg for argument in helper.args.args}
        assert "store" in parameter_names, name


def test_all_streamlit_pages_render_explicit_serving_health_banners() -> None:
    page_paths = (
        _PROJECT_ROOT / "src/rquant/dashboard/app.py",
        _PROJECT_ROOT / "src/rquant/dashboard/nl_screen.py",
        _PROJECT_ROOT / "src/rquant/dashboard/nl_canvas.py",
        _PROJECT_ROOT / "src/rquant/dashboard/lab/app.py",
        _PROJECT_ROOT / "src/rquant/dashboard/market_panorama.py",
    )

    for path in page_paths:
        source = path.read_text(encoding="utf-8")
        assert "render_serving_state_banner" in source, path


def test_dashboard_serving_wrapper_passes_one_parameter_sequence() -> None:
    app_path = _PROJECT_ROOT / "src/rquant/dashboard/app.py"
    tree = ast.parse(app_path.read_text(encoding="utf-8"), filename=str(app_path))
    open_calls = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "open"
        and isinstance(node.func.value, ast.Name)
        and node.func.value.id == "ServingPageRenderContext"
    ]

    assert len(open_calls) == 1
    assert "_dashboard_serving_context.query(" in app_path.read_text(encoding="utf-8")
    assert "LIMIT 300\n        LIMIT 300" not in app_path.read_text(encoding="utf-8")


def test_page_serving_leases_close_from_finally_on_rerun_or_render_error() -> None:
    expected_context_names = {
        _PROJECT_ROOT / "src/rquant/dashboard/app.py": "_dashboard_serving_context",
        _PROJECT_ROOT / "src/rquant/dashboard/nl_screen.py": "_page_serving",
        _PROJECT_ROOT / "src/rquant/dashboard/nl_canvas.py": "_page_serving",
    }

    for path, context_name in expected_context_names.items():
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        finally_closes = [
            node
            for candidate in ast.walk(tree)
            if isinstance(candidate, ast.Try)
            for node in ast.walk(ast.Module(body=candidate.finalbody, type_ignores=[]))
            if isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr == "close"
            and isinstance(node.func.value, ast.Name)
            and node.func.value.id == context_name
        ]
        assert finally_closes, path


def test_missing_serving_generation_returns_typed_unavailable_without_creating_root(
    tmp_path: Path,
) -> None:
    root = tmp_path / "missing-serving"

    result = query_serving_frame(
        root,
        "SELECT 1 AS value",
        now=datetime(2026, 8, 2, 1, 0, tzinfo=UTC),
    )

    assert result.state is ServingFrameState.UNAVAILABLE
    assert result.generation_id is None
    assert result.columns == ()
    assert result.rows == ()
    assert "serving root" in result.detail
    assert not root.exists()


def test_serving_query_succeeds_while_operational_main_is_write_locked_and_replica_missing(
    tmp_path: Path,
) -> None:
    operational = tmp_path / "rquant.duckdb"
    writer = duckdb.connect(str(operational))
    writer.execute("CREATE TABLE production_only(value INTEGER)")
    writer.execute("BEGIN TRANSACTION")
    writer.execute("INSERT INTO production_only VALUES (1)")
    serving_root = tmp_path / "serving"
    built_at = datetime(2026, 8, 2, 1, 0, tzinfo=UTC)
    publisher = ServingPublisher(
        serving_root,
        producer_commit="a" * 40,
        table_specs={"page_data": ServingTableSpec(sort_keys=("id",))},
    )
    publisher.publish(
        {"page_data": pd.DataFrame({"id": [1], "value": ["serving"]})},
        watermarks=(
            ServingDatasetWatermark(
                dataset_id="page_data",
                generation_id="source-1",
                event_time=built_at - timedelta(seconds=1),
                published_at=built_at,
                sequence=1,
                status=FreshnessStatus.FRESH,
            ),
        ),
        source_generations={"page_data": "source-1"},
        built_at=built_at,
    )

    try:
        result = query_serving_frame(
            serving_root,
            "SELECT id, value FROM page_data",
            now=built_at,
        )
    finally:
        writer.rollback()
        writer.close()

    assert result.state is ServingFrameState.READY
    assert result.rows == ((1, "serving"),)
    assert not (tmp_path / "rquant_ro.duckdb").exists()


def test_query_requires_published_projection_and_rejects_unbounded_sql(
    tmp_path: Path,
) -> None:
    root = tmp_path / "serving"
    built_at = datetime(2026, 8, 2, 1, 0, tzinfo=UTC)
    publisher = ServingPublisher(
        root,
        producer_commit="a" * 40,
        table_specs={
            "projection_status": ServingTableSpec(sort_keys=("table_name",)),
            "dashboard_summary": ServingTableSpec(sort_keys=("snapshot_key",)),
        },
    )
    publisher.publish(
        {
            "projection_status": pd.DataFrame(
                {
                    "table_name": ["dashboard_summary"],
                    "available": [False],
                    "reason": ["projection_not_published"],
                    "owner_dataset_id": ["runtime_health"],
                    "available_at": [None],
                }
            ),
            "dashboard_summary": pd.DataFrame({"snapshot_key": pd.Series(dtype="string")}),
        },
        watermarks=(
            ServingDatasetWatermark(
                dataset_id="page-data",
                generation_id="source-1",
                event_time=built_at,
                published_at=built_at,
                sequence=1,
                status=FreshnessStatus.FRESH,
            ),
        ),
        source_generations={"page-data": "source-1"},
        built_at=built_at,
    )

    missing = query_serving_frame(
        root,
        "SELECT snapshot_key FROM dashboard_summary",
        now=built_at,
        required_projections=("dashboard_summary",),
    )
    unsafe = query_serving_frame(root, "PRAGMA database_size", now=built_at)

    assert missing.state is ServingFrameState.UNAVAILABLE
    assert "dashboard_summary" in missing.detail
    assert "not published" in missing.detail
    assert unsafe.state is ServingFrameState.UNAVAILABLE
    assert "read-only SELECT" in unsafe.detail


def test_serving_query_interrupts_computation_past_its_time_budget(tmp_path: Path) -> None:
    root = tmp_path / "serving"
    built_at = datetime(2026, 8, 2, 1, 0, tzinfo=UTC)
    publisher = ServingPublisher(
        root,
        producer_commit="a" * 40,
        table_specs={"page_data": ServingTableSpec(sort_keys=("id",))},
    )
    publisher.publish(
        {"page_data": pd.DataFrame({"id": [1]})},
        watermarks=(
            ServingDatasetWatermark(
                dataset_id="page-data",
                generation_id="source-1",
                event_time=built_at,
                published_at=built_at,
                sequence=1,
                status=FreshnessStatus.FRESH,
            ),
        ),
        source_generations={"page-data": "source-1"},
        built_at=built_at,
    )

    result = query_serving_frame(
        root,
        "SELECT SUM(SIN(i)) FROM range(10000000000) AS values(i)",
        now=built_at,
        max_query_seconds=0.01,
    )

    assert result.state is ServingFrameState.UNAVAILABLE
    assert "interrupt" in result.detail.lower()


@pytest.mark.parametrize(
    ("status", "expected_state"),
    (
        (FreshnessStatus.STALE, "stale"),
        (FreshnessStatus.DEGRADED, "degraded"),
        (FreshnessStatus.UNAVAILABLE, "degraded"),
    ),
)
def test_serving_query_propagates_manifest_watermark_state(
    tmp_path: Path,
    status: FreshnessStatus,
    expected_state: str,
) -> None:
    root = tmp_path / status.value
    built_at = datetime(2026, 8, 2, 1, 0, tzinfo=UTC)
    publisher = ServingPublisher(
        root,
        producer_commit="a" * 40,
        table_specs={"page_data": ServingTableSpec(sort_keys=("id",))},
    )
    publisher.publish(
        {"page_data": pd.DataFrame({"id": [1], "value": [status.value]})},
        watermarks=(
            ServingDatasetWatermark(
                dataset_id="page-data",
                generation_id="source-1",
                event_time=built_at,
                published_at=built_at,
                sequence=1,
                status=status,
                reason=None if status is FreshnessStatus.FRESH else f"{status.value}-evidence",
            ),
        ),
        source_generations={"page-data": "source-1"},
        built_at=built_at,
    )

    result = query_serving_frame(
        root,
        "SELECT id, value FROM page_data",
        now=built_at,
    )

    assert result.state.value == expected_state
    assert status.value in result.detail
    assert result.rows == ((1, status.value),)


def test_serving_query_scopes_watermark_state_to_required_projection_owner(
    tmp_path: Path,
) -> None:
    root = tmp_path / "scoped-serving"
    built_at = datetime(2026, 8, 2, 1, 0, tzinfo=UTC)
    publisher = ServingPublisher(
        root,
        producer_commit="a" * 40,
        table_specs={
            "dashboard_summary": ServingTableSpec(sort_keys=("snapshot_key",)),
            "projection_status": ServingTableSpec(sort_keys=("table_name",)),
            "screen_bounds": ServingTableSpec(sort_keys=("preset_name",)),
        },
    )
    publisher.publish(
        {
            "dashboard_summary": pd.DataFrame({"snapshot_key": ["current"], "value": ["healthy"]}),
            "projection_status": pd.DataFrame(
                {
                    "table_name": ["dashboard_summary", "screen_bounds"],
                    "available": [True, True],
                    "reason": [None, None],
                    "owner_dataset_id": ["runtime_health", "signals"],
                    "available_at": [built_at, built_at],
                }
            ),
            "screen_bounds": pd.DataFrame({"preset_name": ["pool1"], "candidate_count": [1]}),
        },
        watermarks=(
            ServingDatasetWatermark(
                dataset_id="runtime_health",
                generation_id="runtime-1",
                event_time=built_at,
                published_at=built_at,
                sequence=1,
                status=FreshnessStatus.FRESH,
            ),
            ServingDatasetWatermark(
                dataset_id="signals",
                generation_id="signals-1",
                event_time=built_at,
                published_at=built_at,
                sequence=1,
                status=FreshnessStatus.DEGRADED,
                reason="signal-source-delayed",
            ),
        ),
        source_generations={
            "runtime_health": "runtime-1",
            "signals": "signals-1",
        },
        built_at=built_at,
    )

    dashboard = query_serving_frame(
        root,
        "SELECT snapshot_key, value FROM dashboard_summary",
        now=built_at,
        required_projections=("dashboard_summary",),
    )
    screen = query_serving_frame(
        root,
        "SELECT preset_name, candidate_count FROM screen_bounds",
        now=built_at,
        required_projections=("screen_bounds",),
    )

    assert dashboard.state is ServingFrameState.READY
    assert screen.state is ServingFrameState.DEGRADED
    assert "signals:degraded:signal-source-delayed" in screen.detail


def test_serving_query_fails_closed_when_projection_owner_watermark_is_missing(
    tmp_path: Path,
) -> None:
    root = tmp_path / "missing-owner-watermark"
    built_at = datetime(2026, 8, 2, 1, 0, tzinfo=UTC)
    ServingPublisher(
        root,
        producer_commit="a" * 40,
        table_specs={
            "projection_status": ServingTableSpec(sort_keys=("table_name",)),
            "screen_bounds": ServingTableSpec(sort_keys=("preset_name",)),
        },
    ).publish(
        {
            "projection_status": pd.DataFrame(
                {
                    "table_name": ["screen_bounds"],
                    "available": [True],
                    "reason": [None],
                    "owner_dataset_id": ["signals"],
                    "available_at": [built_at],
                }
            ),
            "screen_bounds": pd.DataFrame({"preset_name": ["pool1"], "candidate_count": [1]}),
        },
        watermarks=(
            ServingDatasetWatermark(
                dataset_id="runtime_health",
                generation_id="runtime-1",
                event_time=built_at,
                published_at=built_at,
                sequence=1,
                status=FreshnessStatus.FRESH,
            ),
        ),
        source_generations={"runtime_health": "runtime-1"},
        built_at=built_at,
    )

    result = query_serving_frame(
        root,
        "SELECT preset_name, candidate_count FROM screen_bounds",
        now=built_at,
        required_projections=("screen_bounds",),
    )

    assert result.state is ServingFrameState.UNAVAILABLE
    assert "lacks projection owner watermarks: signals" in result.detail


def test_page_render_context_keeps_one_generation_after_pointer_rotation(tmp_path: Path) -> None:
    from rquant.dashboard.serving_page_data import ServingPageRenderContext

    root = tmp_path / "serving"
    built_at = datetime(2026, 8, 2, 1, 0, tzinfo=UTC)
    publisher = ServingPublisher(
        root,
        producer_commit="a" * 40,
        table_specs={"page_data": ServingTableSpec(sort_keys=("id",))},
    )
    first = publisher.publish(
        {"page_data": pd.DataFrame({"id": [1], "value": ["old"]})},
        watermarks=(
            ServingDatasetWatermark(
                dataset_id="page-data",
                generation_id="source-1",
                event_time=built_at,
                published_at=built_at,
                sequence=1,
                status=FreshnessStatus.FRESH,
            ),
        ),
        source_generations={"page-data": "source-1"},
        built_at=built_at,
    )

    with ServingPageRenderContext.open(root, now=built_at) as context:
        before = context.query("SELECT value FROM page_data WHERE id = 1")
        publisher.publish(
            {"page_data": pd.DataFrame({"id": [1], "value": ["new"]})},
            watermarks=(
                ServingDatasetWatermark(
                    dataset_id="page-data",
                    generation_id="source-2",
                    event_time=built_at + timedelta(seconds=1),
                    published_at=built_at + timedelta(seconds=1),
                    sequence=2,
                    status=FreshnessStatus.FRESH,
                ),
            ),
            source_generations={"page-data": "source-2"},
            built_at=built_at + timedelta(seconds=1),
        )
        after = context.query("SELECT value FROM page_data WHERE id = 1")

    assert context.generation_id == first.generation_id
    assert before.generation_id == after.generation_id == first.generation_id
    assert before.rows == after.rows == (("old",),)


def test_serving_frame_contract_covers_all_states_across_pointer_rotation(
    tmp_path: Path,
) -> None:
    root = tmp_path / "serving"
    first_built_at = datetime(2026, 8, 3, 1, 0, tzinfo=UTC)
    second_built_at = first_built_at + timedelta(minutes=1)
    publisher = ServingPublisher(
        root,
        producer_commit="a" * 40,
        table_specs={
            "projection_status": ServingTableSpec(sort_keys=("table_name",)),
            "page_data": ServingTableSpec(sort_keys=("id",)),
            "pulse_history": ServingTableSpec(sort_keys=("trade_date", "as_of")),
            "unavailable_data": ServingTableSpec(sort_keys=("id",)),
        },
    )

    def tables(*, value: str, available_at: datetime) -> dict[str, pd.DataFrame]:
        return {
            "projection_status": pd.DataFrame(
                {
                    "table_name": ["page_data", "pulse_history", "unavailable_data"],
                    "available": [True, True, False],
                    "reason": [None, None, "projection_not_published"],
                    "owner_dataset_id": ["signals", "signals", "signals"],
                    "available_at": [first_built_at, available_at, None],
                }
            ),
            "page_data": pd.DataFrame({"id": [1], "value": [value]}),
            "pulse_history": pd.DataFrame(
                {
                    "trade_date": [date(2026, 8, 3)],
                    "as_of": [available_at],
                    "t": ["09:31"],
                }
            ),
            "unavailable_data": pd.DataFrame(
                {"id": pd.Series(dtype="int64"), "value": pd.Series(dtype="string")}
            ),
        }

    first = publisher.publish(
        tables(value="first", available_at=first_built_at - timedelta(minutes=30)),
        watermarks=(
            ServingDatasetWatermark(
                dataset_id="signals",
                generation_id="signals-1",
                event_time=first_built_at,
                published_at=first_built_at,
                sequence=1,
                status=FreshnessStatus.FRESH,
            ),
        ),
        source_generations={"signals": "signals-1"},
        built_at=first_built_at,
    )

    with ServingOnlyRenderContext.open(
        root,
        now=first_built_at,
        stale_after=timedelta(minutes=10),
    ) as first_context:
        ready_rows = first_context.query(
            "SELECT id, value FROM page_data",
            required_projections=("page_data",),
        )
        ready_empty = first_context.query(
            "SELECT id, value FROM page_data WHERE id = 999",
            required_projections=("page_data",),
        )
        stale = first_context.query(
            "SELECT trade_date, as_of, t FROM pulse_history",
            required_projections=("pulse_history",),
        )
        unavailable = first_context.query(
            "SELECT id, value FROM unavailable_data",
            required_projections=("unavailable_data",),
        )

        second = publisher.publish(
            tables(value="second", available_at=second_built_at),
            watermarks=(
                ServingDatasetWatermark(
                    dataset_id="signals",
                    generation_id="signals-2",
                    event_time=second_built_at,
                    published_at=second_built_at,
                    sequence=2,
                    status=FreshnessStatus.DEGRADED,
                    reason="source-degraded",
                ),
            ),
            source_generations={"signals": "signals-2"},
            built_at=second_built_at,
        )
        pinned_after_rotation = first_context.query(
            "SELECT id, value FROM page_data",
            required_projections=("page_data",),
        )

    with ServingOnlyRenderContext.open(root, now=second_built_at) as second_context:
        degraded = second_context.query(
            "SELECT id, value FROM page_data",
            required_projections=("page_data",),
        )

    assert first.generation_id != second.generation_id
    assert ready_rows.state is ServingFrameState.READY
    assert ready_rows.rows == ((1, "first"),)
    assert ready_empty.state is ServingFrameState.READY
    assert ready_empty.rows == ()
    assert stale.state is ServingFrameState.STALE
    assert stale.rows == ((date(2026, 8, 3), first_built_at - timedelta(minutes=30), "09:31"),)
    assert unavailable.state is ServingFrameState.UNAVAILABLE
    assert unavailable.rows == ()
    assert degraded.state is ServingFrameState.DEGRADED
    assert degraded.rows == ((1, "second"),)
    assert pinned_after_rotation.rows == ((1, "first"),)

    first_results = (ready_rows, ready_empty, stale, unavailable, pinned_after_rotation)
    assert {result.source for result in first_results} == {"serving"}
    assert {result.generation_id for result in first_results} == {first.generation_id}
    assert {result.generated_at for result in first_results} == {first_built_at}
    assert degraded.source == "serving"
    assert degraded.generation_id == second.generation_id
    assert degraded.generated_at == second_built_at

    expected_states = {
        "ready_rows": (ready_rows, ServingFrameState.READY),
        "ready_empty": (ready_empty, ServingFrameState.READY),
        "stale": (stale, ServingFrameState.STALE),
        "unavailable": (unavailable, ServingFrameState.UNAVAILABLE),
        "degraded": (degraded, ServingFrameState.DEGRADED),
    }
    for name, (result, expected_state) in expected_states.items():
        evidence = result.dataframe().attrs["serving"]
        expected_generation = second if name == "degraded" else first
        assert evidence == {
            "source": "serving",
            "state": expected_state.value,
            "detail": result.detail,
            "generation_id": expected_generation.generation_id,
            "generated_at": expected_generation.built_at,
        }


def test_page_convenience_result_preserves_stale_state_instead_of_returning_raw_value(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from types import SimpleNamespace

    from rquant.dashboard.runtime_console_data import ServingFrameResult
    from rquant.dashboard.serving_page_data import ServingPageRenderContext

    class _Lease:
        manifest = SimpleNamespace(generation_id="generation-1")
        closed = False

        def close(self) -> None:
            self.closed = True

    context = ServingPageRenderContext(
        lease=_Lease(),  # type: ignore[arg-type]
        observed_at=datetime(2026, 8, 3, 1, tzinfo=UTC),
        stale_after=timedelta(minutes=10),
    )
    monkeypatch.setattr(
        context,
        "query",
        lambda *_args, **_kwargs: ServingFrameResult(
            state=ServingFrameState.STALE,
            detail="signals watermark is stale",
            generation_id="generation-1",
            generated_at=datetime(2026, 8, 3, 0, tzinfo=UTC),
            columns=("min_date", "max_date", "candidate_count"),
            rows=((date(2026, 7, 1), date(2026, 7, 31), 12),),
        ),
    )

    result = context.screen_bounds("n-shape-pool1")

    assert result.state is ServingFrameState.STALE
    assert result.detail == "signals watermark is stale"
    assert result.generation_id == "generation-1"
    assert result.value == (date(2026, 7, 1), date(2026, 7, 31), 12)


_SERVING_ROOT_CONSUMERS = {
    _PROJECT_ROOT / "src/rquant/dashboard/app.py": 2,
    _PROJECT_ROOT / "src/rquant/dashboard/nl_screen.py": 1,
    _PROJECT_ROOT / "src/rquant/dashboard/nl_canvas.py": 1,
    _PROJECT_ROOT / "src/rquant/dashboard/lab/app.py": 1,
    _PROJECT_ROOT / "src/rquant/panorama_data.py": 1,
}


def test_every_serving_root_consumer_resolves_through_the_shared_default() -> None:
    """All six reads share one default; none may re-inline the wrong "data/serving"."""

    total = 0
    for path, expected_calls in _SERVING_ROOT_CONSUMERS.items():
        source = path.read_text(encoding="utf-8")
        assert '"data/serving"' not in source, path
        assert "'data/serving'" not in source, path
        assert 'os.environ.get("RQUANT_SERVING_ROOT"' not in source, path
        assert "os.environ.get('RQUANT_SERVING_ROOT'" not in source, path
        assert "from rquant.serving_paths import serving_root_from_env" in source, path
        calls = source.count("serving_root_from_env()")
        assert calls == expected_calls, (path, calls)
        total += calls

    assert total == 6


def test_page_units_pin_the_serving_root_to_the_publisher_directory() -> None:
    """Four units must override .env with the directory the publisher writes."""

    from rquant.runtime_deployment_profile import LINUX_PRODUCTION_RUNTIME_ROOT

    assignment = f"Environment=RQUANT_SERVING_ROOT={LINUX_PRODUCTION_RUNTIME_ROOT / 'serving'}"
    for name in (
        "rquant-dashboard",
        "rquant-nl-screen",
        "rquant-canvas",
        "rquant-panorama",
    ):
        service = (_PROJECT_ROOT / f"deploy/systemd/{name}.service").read_text(encoding="utf-8")
        assert service.count(assignment + "\n") == 1, name
        # systemd applies environment assignments in order; the override only wins
        # when it follows EnvironmentFile=.
        assert service.index(assignment) > service.index(
            "EnvironmentFile=/home/lighthouse/rquant/.env"
        ), name
