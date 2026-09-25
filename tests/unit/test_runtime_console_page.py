"""Renders ``runtime_console.py`` against a minimal published serving root.

Pattern copied from ``test_serving_page_isolation.py``: build a serving root with
``ServingPublisher``, then run ``streamlit.testing.v1.AppTest`` in a subprocess (the
Streamlit test harness is not safe to run twice in one interpreter) and assert no
exception was raised.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import textwrap
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pandas as pd

from rquant.serving_contracts import FreshnessStatus, ServingDatasetWatermark
from rquant.serving_publisher import ServingPublisher, ServingTableSpec

_PROJECT_ROOT = Path(__file__).resolve().parents[2]
_PAGE_PATH = _PROJECT_ROOT / "src/rquant/dashboard/runtime_console.py"
_COMMIT = "c" * 40


def _publish_console_serving_root(root: Path, *, built_at: datetime) -> None:
    tables = {
        "runtime_services": pd.DataFrame(
            [
                {
                    "service_id": "feature-live",
                    "plane": "live",
                    "status": "running",
                    "stale": False,
                    "observed_at": built_at,
                    "heartbeat_at": built_at,
                    "input_sequence": 42,
                    "output_sequence": 41,
                    "backlog_count": 1,
                    "consecutive_failures": 0,
                    "last_error": None,
                },
                {
                    "service_id": "serving-publisher",
                    "plane": "serving",
                    "status": "running",
                    "stale": False,
                    "observed_at": built_at,
                    "heartbeat_at": built_at,
                    "input_sequence": 10,
                    "output_sequence": 10,
                    "backlog_count": 0,
                    "consecutive_failures": 0,
                    "last_error": None,
                },
            ]
        ),
        "signals": pd.DataFrame(
            [
                {
                    "global_sequence": 1,
                    "signal_id": "sig-1",
                    "strategy_id": "n-shape",
                    "strategy_version": "v1",
                    "candidate_id": "603937.SH",
                    "action": "BUY",
                    "available_at": built_at - timedelta(minutes=1),
                    "expires_at": built_at + timedelta(minutes=4),
                    "reason_codes_json": '["volume_progress"]',
                },
                {
                    "global_sequence": 2,
                    "signal_id": "sig-2",
                    "strategy_id": "auction-gap",
                    "strategy_version": "v2",
                    "candidate_id": "002238.SZ",
                    "action": "SELL",
                    "available_at": built_at,
                    "expires_at": None,
                    "reason_codes_json": '["stop_strong"]',
                },
            ]
        ),
        "deliveries": pd.DataFrame(
            [
                {
                    "outbox_id": "outbox-1",
                    "signal_id": "sig-1",
                    "recipient_id": "admin",
                    "channel": "pushdeer",
                    "status": "succeeded",
                    "attempt_count": 1,
                    "updated_at": built_at,
                    "last_error": None,
                },
                {
                    "outbox_id": "outbox-2",
                    "signal_id": "sig-2",
                    "recipient_id": "admin",
                    "channel": "pushdeer",
                    "status": "failed",
                    "attempt_count": 3,
                    "updated_at": built_at,
                    "last_error": "timeout",
                },
            ]
        ),
        "paper_accounts": pd.DataFrame(
            [
                {
                    "account_id": "shadow-main",
                    "as_of_time": built_at,
                    "cash": 100000,
                    "available_cash": 98000,
                    "frozen_cash": 2000,
                    "nav": 102500,
                    "unrealized_pnl": 2500,
                    "realized_pnl": 0,
                }
            ]
        ),
        "paper_holdings": pd.DataFrame(
            [
                {
                    "account_id": "shadow-main",
                    "ts_code": "603937.SH",
                    "quantity": 100,
                    "available_quantity": 100,
                    "frozen_quantity": 0,
                    "average_cost": 10.0,
                    "market_price": 10.5,
                    "market_value": 1050,
                    "unrealized_pnl": 50,
                    "as_of_time": built_at,
                },
                {
                    "account_id": "shadow-main",
                    "ts_code": "002238.SZ",
                    "quantity": 100,
                    "available_quantity": 100,
                    "frozen_quantity": 0,
                    "average_cost": 20.0,
                    "market_price": 19.0,
                    "market_value": 1900,
                    "unrealized_pnl": -100,
                    "as_of_time": built_at,
                },
            ]
        ),
        "lab_jobs": pd.DataFrame(
            columns=[
                "job_id",
                "strategy_name",
                "job_type",
                "resource_class",
                "status",
                "progress_fraction",
                "phase",
                "terminal_shards",
                "total_shards",
                "eta_status",
                "eta_finish_low",
                "eta_finish_center",
                "eta_finish_high",
                "updated_at",
            ]
        ),
        "promotions": pd.DataFrame(
            columns=[
                "decision_id",
                "stage",
                "approved",
                "experiment_ids_json",
                "gate_failures_json",
                "decided_at",
            ]
        ),
    }
    sort_keys = {
        "runtime_services": ("service_id",),
        "signals": ("global_sequence",),
        "deliveries": ("outbox_id",),
        "paper_accounts": ("account_id",),
        "paper_holdings": ("account_id", "ts_code"),
        "lab_jobs": ("job_id",),
        "promotions": ("decision_id",),
    }
    publisher = ServingPublisher(
        root,
        producer_commit=_COMMIT,
        table_specs={
            table: ServingTableSpec(sort_keys=sort_keys[table]) for table in tables
        },
    )
    publisher.publish(
        tables,
        watermarks=(
            ServingDatasetWatermark(
                dataset_id="runtime-console",
                generation_id="source-1",
                event_time=built_at,
                published_at=built_at,
                sequence=1,
                status=FreshnessStatus.FRESH,
            ),
        ),
        source_generations={"runtime-console": "source-1"},
        built_at=built_at,
    )


def _run_apptest_harness(script_path: Path, *, root: Path, tmp_path: Path) -> dict:
    harness = textwrap.dedent(
        f"""
        import json
        from streamlit.testing.v1 import AppTest

        app = AppTest.from_file({str(script_path)!r}).run(timeout=30)
        result = {{
            "exceptions": [str(item.value) for item in app.exception],
            "errors": [str(item.value) for item in app.error],
            "metrics": {{metric.label: metric.value for metric in app.metric}},
            "captions": [str(item.value) for item in app.caption],
            "subheaders": [str(item.value) for item in app.subheader],
            "dataframes": [
                {{"rows": len(item.value.index), "columns": list(item.value.columns)}}
                for item in app.dataframe
            ],
        }}
        print("RESULT_JSON=" + json.dumps(result, ensure_ascii=False))
        """
    )
    environment = dict(os.environ)
    environment.update(
        {
            "TUSHARE_TOKEN_MAIN": "0" * 40,
            "DATA_DIR": str(tmp_path / "data"),
            "DUCKDB_PATH": str(tmp_path / "data" / "rquant.duckdb"),
            "PARQUET_DIR": str(tmp_path / "data" / "parquet"),
            "LOG_DIR": str(tmp_path / "data" / "logs"),
            "RQUANT_DISABLE_DOTENV": "1",
            "PYTHONPATH": str(_PROJECT_ROOT / "src"),
            "RQUANT_SERVING_ROOT": str(root),
        }
    )
    completed = subprocess.run(
        [sys.executable, "-c", harness],
        cwd=_PROJECT_ROOT,
        env=environment,
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )
    assert completed.returncode == 0, completed.stdout + completed.stderr
    [result_line] = [
        line for line in completed.stdout.splitlines() if line.startswith("RESULT_JSON=")
    ]
    return json.loads(result_line.removeprefix("RESULT_JSON="))


def test_runtime_console_renders_signals_deliveries_paper_and_placeholders(
    tmp_path: Path,
) -> None:
    root = tmp_path / "serving"
    built_at = datetime.now(UTC) - timedelta(seconds=5)
    _publish_console_serving_root(root, built_at=built_at)

    result = _run_apptest_harness(_PAGE_PATH, root=root, tmp_path=tmp_path)

    assert result["exceptions"] == [], result["exceptions"]
    assert result["errors"] == [], result["errors"]

    metrics = result["metrics"]
    assert metrics["近期信号"] == "2"
    assert metrics["推送失败"] == "1"

    dataframe_row_counts = sorted(item["rows"] for item in result["dataframes"])
    # services split into live/serving/research columns (2 populated + 1 empty),
    # signals (2), deliveries (2), paper accounts (1), paper holdings (2).
    assert 2 in dataframe_row_counts
    assert 1 in dataframe_row_counts

    captions = result["captions"]
    assert any("Lab 任务" in caption for caption in captions)
    assert any("策略晋级决策" in caption for caption in captions)

    subheaders = result["subheaders"]
    for expected in ("服务健康", "信号", "推送", "模拟账户", "Lab Jobs", "策略晋级"):
        assert expected in subheaders, subheaders


def test_runtime_console_reports_unavailable_serving_root_without_crashing(
    tmp_path: Path,
) -> None:
    missing_root = tmp_path / "does-not-exist"

    result = _run_apptest_harness(_PAGE_PATH, root=missing_root, tmp_path=tmp_path)

    assert result["exceptions"] == [], result["exceptions"]
