"""``preview_app.py`` must import and render both of its navigation pages
(健康看板 / app.py and 运行控制台 / runtime_console.py) in the same session
without raising. Pattern copied from ``test_serving_page_isolation.py``: run the
Streamlit AppTest harness in a subprocess, then assert no exceptions on either page.
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
_PREVIEW_PATH = _PROJECT_ROOT / "src/rquant/dashboard/preview_app.py"
_COMMIT = "d" * 40


def _publish_minimal_serving_root(root: Path, *, built_at: datetime) -> None:
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
                    "input_sequence": 5,
                    "output_sequence": 5,
                    "backlog_count": 0,
                    "consecutive_failures": 0,
                    "last_error": None,
                }
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
                    "available_at": built_at,
                    "expires_at": None,
                    "reason_codes_json": "[]",
                }
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
                }
            ]
        ),
        "paper_accounts": pd.DataFrame(
            [
                {
                    "account_id": "shadow-main",
                    "as_of_time": built_at,
                    "cash": 100000,
                    "available_cash": 100000,
                    "frozen_cash": 0,
                    "nav": 100000,
                    "unrealized_pnl": 0,
                    "realized_pnl": 0,
                }
            ]
        ),
        "paper_holdings": pd.DataFrame(
            columns=[
                "account_id",
                "ts_code",
                "quantity",
                "available_quantity",
                "frozen_quantity",
                "average_cost",
                "market_price",
                "market_value",
                "unrealized_pnl",
                "as_of_time",
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
    publisher = ServingPublisher(
        root,
        producer_commit=_COMMIT,
        table_specs={
            table: ServingTableSpec(sort_keys=(frame.columns[0],))
            for table, frame in tables.items()
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


def test_preview_app_renders_both_pages_without_exceptions(tmp_path: Path) -> None:
    root = tmp_path / "serving"
    built_at = datetime.now(UTC) - timedelta(seconds=5)
    _publish_minimal_serving_root(root, built_at=built_at)

    harness = textwrap.dedent(
        f"""
        import json
        from streamlit.testing.v1 import AppTest

        app = AppTest.from_file({str(_PREVIEW_PATH)!r}).run(timeout=30)
        first_page_exceptions = [str(item.value) for item in app.exception]
        first_page_titles = [str(item.value) for item in app.title]

        app.switch_page("runtime_console.py")
        app.run(timeout=30)
        second_page_exceptions = [str(item.value) for item in app.exception]
        second_page_subheaders = [str(item.value) for item in app.subheader]

        result = {{
            "first_page_exceptions": first_page_exceptions,
            "first_page_titles": first_page_titles,
            "second_page_exceptions": second_page_exceptions,
            "second_page_subheaders": second_page_subheaders,
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
    result = json.loads(result_line.removeprefix("RESULT_JSON="))

    assert result["first_page_exceptions"] == [], result["first_page_exceptions"]
    assert result["second_page_exceptions"] == [], result["second_page_exceptions"]
    assert "服务健康" in result["second_page_subheaders"]
